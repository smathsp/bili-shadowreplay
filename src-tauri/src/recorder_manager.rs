use crate::config::Config;
use crate::danmu2ass;
use crate::database::record::RecordRow;
use crate::database::recorder::RecorderRow;
use crate::database::video::VideoRow;
use crate::database::{Database, DatabaseError};
use crate::ffmpeg::{clip_timeline_anchors, encode_video_danmu, transcode, Range};
use crate::migration::migration_methods::try_rebuild_archives;
use crate::progress::progress_reporter::{EventEmitter, ProgressReporter, ProgressReporterTrait};
use crate::subtitle_generator::item_to_srt;
use crate::task::{Task, TaskManager, TaskPriority};
use crate::webhook::events::{self, Payload};
use crate::webhook::poster::WebhookPoster;
use chrono::DateTime;
use danmu_stream::LiveEvent;
use m3u8_rs::{MediaPlaylist, MediaPlaylistType};
use recorder::account::Account;
use recorder::danmu::{DanmuEntry, DanmuStorage};
use recorder::errors::RecorderError;
use recorder::events::RecorderEvent;
use recorder::platforms::bilibili::BiliRecorder;
use recorder::platforms::douyin::{
    acknowledge_live_end, DouyinRecorder, DOUYIN_SESSION_PARENT_FILE,
};
use recorder::platforms::huya::HuyaRecorder;
use recorder::platforms::kuaishou::KuaishouRecorder;
use recorder::platforms::tiktok::TikTokRecorder;
use recorder::platforms::PlatformType;
use recorder::traits::RecorderTrait;
use recorder::RoomInfo;
use recorder::UserInfo;
use recorder::{CachePath, RecorderInfo};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "gui")]
use tauri_plugin_notification::NotificationExt;
use thiserror::Error;
use tokio::fs::{remove_file, write, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, Mutex, RwLock};

#[cfg(not(feature = "headless"))]
use tauri::AppHandle;

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct RecorderList {
    pub count: usize,
    pub recorders: Vec<RecorderInfo>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClipRangeParams {
    pub title: String,
    pub note: String,
    pub cover: String,
    pub platform: String,
    pub room_id: String,
    pub live_id: String,
    pub ranges: Vec<Range>,
    /// Encode danmu after clip
    pub danmu: bool,
    pub local_offset: i64,
    /// Fix encoding after clip
    pub fix_encoding: bool,
    /// Transition effect between clips (for multiple ranges)
    pub transition: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GenerateWholeClipParams {
    pub encode_danmu: bool,
    pub platform: String,
    pub room_id: String,
    pub parent_id: String,
    pub selected_live_ids: Option<Vec<String>>,
    pub output_name: Option<String>,
}

pub struct RelatedPlaylist {
    pub live_id: String,
    pub title: String,
    pub path: PathBuf,
}

const DOUYIN_WHOLE_RETRY_INITIAL_SECS: u64 = 5;
const DOUYIN_WHOLE_RETRY_MAX_SECS: u64 = 300;
const DOUYIN_WHOLE_COMPLETIONS_DIR: &str = ".whole-completions";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct DouyinWholeCompletion {
    version: u8,
    incarnation_token: String,
    output_file: String,
}

fn encode_douyin_lifecycle_key(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn douyin_whole_output_name(room_id: &str, incarnation_token: &str) -> String {
    let room = sanitize_filename::sanitize(room_id)
        .chars()
        .take(64)
        .collect::<String>();
    let incarnation = sanitize_filename::sanitize(incarnation_token)
        .chars()
        .take(96)
        .collect::<String>();
    format!("[full][douyin][{room}][{incarnation}].mp4")
}

fn douyin_whole_completion_path(
    cache_dir: &Path,
    room_id: &str,
    incarnation_token: &str,
) -> PathBuf {
    cache_dir
        .join(PlatformType::Douyin.as_str())
        .join(room_id)
        .join(DOUYIN_WHOLE_COMPLETIONS_DIR)
        .join(encode_douyin_lifecycle_key(incarnation_token))
}

async fn sync_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        tokio::fs::File::open(path).await?.sync_all().await
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

async fn persist_douyin_whole_completion(
    cache_dir: &Path,
    room_id: &str,
    incarnation_token: &str,
    output_file: &str,
) -> Result<(), String> {
    let path = douyin_whole_completion_path(cache_dir, room_id, incarnation_token);
    let parent = path
        .parent()
        .ok_or_else(|| "whole-session completion path has no parent".to_string())?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| error.to_string())?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        encode_douyin_lifecycle_key(incarnation_token),
        uuid::Uuid::new_v4().simple()
    ));
    let marker = DouyinWholeCompletion {
        version: 1,
        incarnation_token: incarnation_token.to_string(),
        output_file: output_file.to_string(),
    };
    let bytes = serde_json::to_vec(&marker).map_err(|error| error.to_string())?;
    let mut file = tokio::fs::File::create(&temporary)
        .await
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes)
        .await
        .map_err(|error| error.to_string())?;
    file.flush().await.map_err(|error| error.to_string())?;
    file.sync_data().await.map_err(|error| error.to_string())?;
    drop(file);
    if let Err(error) = tokio::fs::rename(&temporary, &path).await {
        #[cfg(not(unix))]
        if matches!(
            error.kind(),
            std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
        ) {
            let _ = tokio::fs::remove_file(&path).await;
            tokio::fs::rename(&temporary, &path)
                .await
                .map_err(|error| error.to_string())?;
        } else {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error.to_string());
        }
        #[cfg(unix)]
        {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error.to_string());
        }
    }
    sync_directory(parent)
        .await
        .map_err(|error| error.to_string())
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DouyinWholeSessionKey {
    room_id: String,
    session_id: String,
}

fn claim_douyin_whole_session(
    claimed: &mut HashSet<DouyinWholeSessionKey>,
    room_id: &str,
    session_id: &str,
) -> bool {
    claimed.insert(DouyinWholeSessionKey {
        room_id: room_id.to_string(),
        session_id: session_id.to_string(),
    })
}

fn douyin_whole_retry_delay(failure_count: u32) -> Duration {
    let exponent = failure_count.saturating_sub(1).min(6);
    let seconds = DOUYIN_WHOLE_RETRY_INITIAL_SECS
        .saturating_mul(1_u64 << exponent)
        .min(DOUYIN_WHOLE_RETRY_MAX_SECS);
    Duration::from_secs(seconds)
}

async fn retry_until_douyin_live_end_acknowledged<F, Fut, D>(
    mut acknowledge: F,
    mut retry_delay: D,
) -> u32
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
    D: FnMut(u32) -> Duration,
{
    let mut failure_count = 0_u32;
    loop {
        if acknowledge().await {
            return failure_count;
        }
        failure_count = failure_count.saturating_add(1);
        tokio::time::sleep(retry_delay(failure_count)).await;
    }
}

#[derive(Debug, Eq, PartialEq)]
enum DouyinArchiveDiskState {
    Empty,
    ValuableDataOnly,
    VideoMedia { total_size: u64 },
}

async fn douyin_playlist_has_local_media_segment(
    archive_path: &Path,
    playlist_path: &Path,
) -> std::io::Result<bool> {
    let bytes = match tokio::fs::read(playlist_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let Ok((_, playlist)) = m3u8_rs::parse_media_playlist(&bytes) else {
        return Ok(false);
    };
    for segment in playlist.segments {
        let uri = segment.uri.split(['?', '#']).next().unwrap_or_default();
        let relative = Path::new(uri);
        if uri.is_empty()
            || relative.components().any(|component| {
                matches!(
                    component,
                    Component::Prefix(_) | Component::RootDir | Component::ParentDir
                )
            })
        {
            continue;
        }
        if tokio::fs::metadata(archive_path.join(relative))
            .await
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Return the total directory size when an archive contains durable media
/// evidence. Unknown non-empty files are treated as media: retaining an empty
/// directory is harmless, while deleting a segment with an unusual extension
/// is irreversible.
async fn inspect_douyin_archive(archive_path: &Path) -> std::io::Result<DouyinArchiveDiskState> {
    let mut entries = match tokio::fs::read_dir(archive_path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DouyinArchiveDiskState::Empty)
        }
        Err(error) => return Err(error),
    };
    let mut total_size = 0_u64;
    let mut has_valuable_data = false;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() {
            continue;
        }
        let metadata = entry.metadata().await?;
        let size = metadata.len();
        total_size = total_size.saturating_add(size);
        if size == 0 {
            continue;
        }
        let file_name = entry.file_name();
        has_valuable_data |= match file_name.to_str() {
            Some(name) => {
                let lower = name.to_ascii_lowercase();
                lower != DOUYIN_SESSION_PARENT_FILE && !lower.ends_with(".tmp")
            }
            None => true,
        };
    }
    // A playlist header, entries.log, or stray segment is not proof of a
    // playable attempt. Require the playlist to reference at least one local,
    // non-empty media segment before assigning a video size.
    let has_media =
        douyin_playlist_has_local_media_segment(archive_path, &archive_path.join("playlist.m3u8"))
            .await?;
    if has_media {
        Ok(DouyinArchiveDiskState::VideoMedia {
            total_size: total_size.max(1),
        })
    } else if has_valuable_data {
        Ok(DouyinArchiveDiskState::ValuableDataOnly)
    } else {
        Ok(DouyinArchiveDiskState::Empty)
    }
}

async fn douyin_archive_media_size(archive_path: &Path) -> std::io::Result<Option<u64>> {
    Ok(match inspect_douyin_archive(archive_path).await? {
        DouyinArchiveDiskState::VideoMedia { total_size } => Some(total_size),
        DouyinArchiveDiskState::Empty | DouyinArchiveDiskState::ValuableDataOnly => None,
    })
}

#[derive(Debug, Eq, PartialEq)]
enum DouyinZeroSizedArchiveRepair {
    Empty,
    ValuableDataOnly,
    VideoRepaired,
}

async fn repair_douyin_zero_sized_archive(
    db: &Database,
    cache_dir: &Path,
    room_id: &str,
    live_id: &str,
) -> Result<DouyinZeroSizedArchiveRepair, RecorderManagerError> {
    let archive_path = cache_dir
        .join(PlatformType::Douyin.as_str())
        .join(room_id)
        .join(live_id);
    match inspect_douyin_archive(&archive_path).await? {
        DouyinArchiveDiskState::Empty => Ok(DouyinZeroSizedArchiveRepair::Empty),
        DouyinArchiveDiskState::ValuableDataOnly => {
            Ok(DouyinZeroSizedArchiveRepair::ValuableDataOnly)
        }
        DouyinArchiveDiskState::VideoMedia { total_size } => {
            db.update_record_delta(live_id, 0.0, total_size).await?;
            Ok(DouyinZeroSizedArchiveRepair::VideoRepaired)
        }
    }
}

async fn douyin_session_has_cached_media(
    db: &Database,
    cache_dir: &Path,
    room_id: &str,
    session_id: &str,
) -> Result<bool, RecorderManagerError> {
    let room_path = cache_dir.join(PlatformType::Douyin.as_str()).join(room_id);
    let related_archives = db
        .get_archives_by_parent_id(room_id, session_id)
        .await?
        .into_iter()
        .filter(|archive| archive.platform == PlatformType::Douyin.as_str())
        .map(|archive| archive.live_id)
        .collect::<HashSet<_>>();

    for live_id in &related_archives {
        if douyin_archive_media_size(&room_path.join(live_id))
            .await?
            .is_some()
        {
            return Ok(true);
        }
    }

    let mut entries = match tokio::fs::read_dir(&room_path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(live_id) = entry.file_name().to_str().map(str::to_string) else {
            // An uninspectable archive name is uncertainty. Keep the recovery
            // marker instead of declaring this session empty.
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Douyin archive directory has a non-Unicode name",
            )
            .into());
        };
        if live_id.starts_with('.') || related_archives.contains(&live_id) {
            continue;
        }
        let parent_path = entry.path().join(DOUYIN_SESSION_PARENT_FILE);
        let belongs_to_session = match tokio::fs::read_to_string(&parent_path).await {
            Ok(parent_id) => parent_id.trim() == session_id,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if belongs_to_session && douyin_archive_media_size(&entry.path()).await?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn require_danmu_ass_files(
    live_ids: &[String],
    results: Vec<Result<PathBuf, RecorderManagerError>>,
) -> Result<Vec<Option<PathBuf>>, RecorderManagerError> {
    if live_ids.len() != results.len() {
        return Err(RecorderManagerError::ArchiveDanmuAssGenerationFailed {
            error: format!(
                "Archive/danmu result count mismatch: {} archives, {} results",
                live_ids.len(),
                results.len()
            ),
        });
    }

    live_ids
        .iter()
        .zip(results)
        .map(|(live_id, result)| {
            result.map(Some).map_err(|error| {
                RecorderManagerError::ArchiveDanmuAssGenerationFailed {
                    error: format!("archive '{live_id}': {error}"),
                }
            })
        })
        .collect()
}

pub enum RecorderType {
    BiliBili(BiliRecorder),
    Douyin(DouyinRecorder),
    Huya(HuyaRecorder),
    Kuaishou(KuaishouRecorder),
    TikTok(TikTokRecorder),
}

impl RecorderType {
    async fn run(&self) {
        match self {
            RecorderType::BiliBili(recorder) => recorder.run().await,
            RecorderType::Douyin(recorder) => recorder.run().await,
            RecorderType::Huya(recorder) => recorder.run().await,
            RecorderType::Kuaishou(recorder) => recorder.run().await,
            RecorderType::TikTok(recorder) => recorder.run().await,
        }
    }

    async fn stop(&self) {
        match self {
            RecorderType::BiliBili(recorder) => recorder.stop().await,
            RecorderType::Douyin(recorder) => recorder.stop().await,
            RecorderType::Huya(recorder) => recorder.stop().await,
            RecorderType::Kuaishou(recorder) => recorder.stop().await,
            RecorderType::TikTok(recorder) => recorder.stop().await,
        }
    }

    async fn info(&self) -> RecorderInfo {
        match self {
            RecorderType::BiliBili(recorder) => recorder.info().await,
            RecorderType::Douyin(recorder) => recorder.info().await,
            RecorderType::Huya(recorder) => recorder.info().await,
            RecorderType::Kuaishou(recorder) => recorder.info().await,
            RecorderType::TikTok(recorder) => recorder.info().await,
        }
    }

    async fn enable(&self) {
        match self {
            RecorderType::BiliBili(recorder) => recorder.enable().await,
            RecorderType::Douyin(recorder) => recorder.enable().await,
            RecorderType::Huya(recorder) => recorder.enable().await,
            RecorderType::Kuaishou(recorder) => recorder.enable().await,
            RecorderType::TikTok(recorder) => recorder.enable().await,
        }
    }

    async fn disable(&self) {
        match self {
            RecorderType::BiliBili(recorder) => recorder.disable().await,
            RecorderType::Douyin(recorder) => recorder.disable().await,
            RecorderType::Huya(recorder) => recorder.disable().await,
            RecorderType::Kuaishou(recorder) => recorder.disable().await,
            RecorderType::TikTok(recorder) => recorder.disable().await,
        }
    }
}

#[derive(Clone)]
pub struct RecorderManager {
    #[cfg(not(feature = "headless"))]
    app_handle: AppHandle,
    emitter: EventEmitter,
    db: Arc<Database>,
    config: Arc<RwLock<Config>>,
    task_manager: Arc<TaskManager>,
    resource_dir: PathBuf,
    recorders: Arc<RwLock<HashMap<String, RecorderType>>>,
    to_remove: Arc<RwLock<HashSet<String>>>,
    event_tx: broadcast::Sender<RecorderEvent>,
    is_migrating: Arc<AtomicBool>,
    webhook_poster: WebhookPoster,
    /// Claimed sessions are retained for the process lifetime. This prevents a
    /// delayed duplicate recovery event from creating a second export even
    /// after the first long-running task has completed.
    douyin_whole_sessions: Arc<Mutex<HashSet<DouyinWholeSessionKey>>>,
    /// `try_rebuild_archives` updates zero-sized rows by adding their measured
    /// size, so concurrent reconciliations must not both apply the same delta.
    douyin_archive_reconcile: Arc<Mutex<()>>,
}

#[derive(Error, Debug)]
pub enum RecorderManagerError {
    #[error("Recorder already exists: {room_id}")]
    AlreadyExisted { room_id: String },
    #[error("Recorder not found: {room_id}")]
    NotFound { room_id: String },
    #[error("Invalid platform type: {platform}")]
    InvalidPlatformType { platform: String },
    #[error("Recorder error: {0}")]
    RecorderError(#[from] RecorderError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("HLS error: {err}")]
    HLSError { err: String },
    #[error("Database error: {0}")]
    DatabaseError(#[from] DatabaseError),
    #[error("Recording: {live_id}")]
    Recording { live_id: String },
    #[error("Clip error: {err}")]
    ClipError { err: String },
    #[error("M3u8 parse failed: {content}")]
    M3u8ParseFailed { content: String },
    #[error("Empty playlist")]
    EmptyPlaylist,
    #[error("Subtitle not found: {live_id}")]
    SubtitleNotFound { live_id: String },
    #[error("Subtitle generation failed: {error}")]
    SubtitleGenerationFailed { error: String },
    #[error("Invalid live id, not timestamp str")]
    InvalidLiveID,
    #[error("Archive danmu ass generation failed: {error}")]
    ArchiveDanmuAssGenerationFailed { error: String },
}

impl From<RecorderManagerError> for String {
    fn from(err: RecorderManagerError) -> Self {
        err.to_string()
    }
}

/// Resolve the session even when the last recording attempt produced no archive.
async fn live_end_parent_id(
    db: &Database,
    platform: PlatformType,
    room_id: &str,
    recorder: &RecorderInfo,
) -> Result<Option<String>, DatabaseError> {
    let parent_id = if !recorder.platform_live_id.is_empty() {
        recorder.platform_live_id.clone()
    } else if !recorder.live_id.is_empty() {
        // Keep compatibility with recorders that only retain their current archive ID.
        db.get_record(room_id, &recorder.live_id).await?.parent_id
    } else {
        return Ok(None);
    };

    let records = db.get_archives_by_parent_id(room_id, &parent_id).await?;
    Ok(records
        .iter()
        .any(|record| record.platform == platform.as_str() && record.size > 0)
        .then_some(parent_id))
}

impl RecorderManager {
    pub fn new(
        #[cfg(not(feature = "headless"))] app_handle: AppHandle,
        emitter: EventEmitter,
        db: Arc<Database>,
        config: Arc<RwLock<Config>>,
        task_manager: Arc<TaskManager>,
        resource_dir: PathBuf,
        webhook_poster: WebhookPoster,
    ) -> RecorderManager {
        let (event_tx, _) = broadcast::channel(100);
        let manager = RecorderManager {
            #[cfg(not(feature = "headless"))]
            app_handle,
            emitter,
            db,
            config,
            task_manager,
            resource_dir,
            recorders: Arc::new(RwLock::new(HashMap::new())),
            to_remove: Arc::new(RwLock::new(HashSet::new())),
            event_tx,
            is_migrating: Arc::new(AtomicBool::new(false)),
            webhook_poster,
            douyin_whole_sessions: Arc::new(Mutex::new(HashSet::new())),
            douyin_archive_reconcile: Arc::new(Mutex::new(())),
        };

        // Start event listener
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            manager_clone.handle_events().await;
        });

        let manager_clone = manager.clone();
        tokio::spawn(async move {
            manager_clone.monitor_recorders().await;
        });

        manager
    }

    pub fn get_event_sender(&self) -> broadcast::Sender<RecorderEvent> {
        self.event_tx.clone()
    }

    async fn handle_events(&self) {
        let mut rx = self.event_tx.subscribe();
        loop {
            let event = match rx.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    log::warn!("Recorder event listener lagged; skipped {skipped} events");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            match event {
                RecorderEvent::LiveStart { recorder } => {
                    let event = events::new_webhook_event(
                        events::LIVE_STARTED,
                        Payload::Room(recorder.clone()),
                    );
                    let _ = self.webhook_poster.post_event(&event).await;
                    if self.config.read().await.live_start_notify {
                        #[cfg(feature = "gui")]
                        self.app_handle
                            .notification()
                            .builder()
                            .title("BiliShadowReplay - 直播开始")
                            .body(format!(
                                "{} 开启了直播：{}",
                                recorder.user_info.user_name, recorder.room_info.room_title
                            ))
                            .show()
                            .unwrap();
                    }
                    self.emitter.emit(&RecorderEvent::LiveStart { recorder });
                }
                RecorderEvent::LiveEnd {
                    platform,
                    room_id,
                    recorder,
                } => {
                    if platform == PlatformType::Douyin {
                        let session_id = recorder.platform_live_id.trim();
                        if session_id.is_empty() {
                            log::error!(
                                "Ignoring Douyin LiveEnd without an incarnation token for room {room_id}"
                            );
                            continue;
                        }
                        let claimed = {
                            let mut sessions = self.douyin_whole_sessions.lock().await;
                            claim_douyin_whole_session(&mut sessions, &room_id, session_id)
                        };
                        if !claimed {
                            // The recorder intentionally redelivers until the
                            // durable marker is ACKed. Suppress *all* external
                            // side effects for those duplicate deliveries.
                            log::debug!(
                                "Ignoring duplicate Douyin LiveEnd delivery: room={room_id}, incarnation={session_id}"
                            );
                            continue;
                        }
                    }
                    let event = events::new_webhook_event(
                        events::LIVE_ENDED,
                        Payload::Room(recorder.clone()),
                    );
                    let _ = self.webhook_poster.post_event(&event).await;
                    self.handle_live_end(platform, &room_id, &recorder).await;
                    if self.config.read().await.live_end_notify {
                        #[cfg(feature = "gui")]
                        self.app_handle
                            .notification()
                            .builder()
                            .title("BiliShadowReplay - 直播结束")
                            .body(format!(
                                "{} 结束了直播：{}",
                                recorder.user_info.user_name, recorder.room_info.room_title
                            ))
                            .show()
                            .unwrap();
                    }
                    self.emitter.emit(&RecorderEvent::LiveEnd {
                        platform,
                        room_id,
                        recorder,
                    });
                }
                RecorderEvent::RecordStart { recorder } => {
                    // add record entry into db
                    let platform = PlatformType::from_str(&recorder.room_info.platform).unwrap();
                    let room_id = recorder.room_info.room_id.clone();
                    log::info!("Record start: {recorder:?}");
                    if let Err(e) = self
                        .db
                        .add_record(
                            platform,
                            &recorder.platform_live_id,
                            &recorder.live_id,
                            &room_id,
                            &recorder.room_info.room_title,
                            None,
                        )
                        .await
                    {
                        log::error!("Failed to add record entry into db: {e}");
                    }
                    let event =
                        events::new_webhook_event(events::RECORD_STARTED, Payload::Room(recorder));
                    let _ = self.webhook_poster.post_event(&event).await;
                }
                RecorderEvent::RecordUpdate {
                    live_id,
                    duration_secs,
                    cached_size_bytes,
                } => {
                    let _ = self
                        .db
                        .update_record_delta(&live_id, duration_secs, cached_size_bytes)
                        .await;
                }
                RecorderEvent::RecordEnd { recorder } => {
                    log::info!("Record end: {recorder:?}");
                    let live_id = recorder.live_id.clone();
                    if live_id.is_empty() {
                        log::warn!(
                            "Ignoring record end without live id for room {}",
                            recorder.room_info.room_id
                        );
                        continue;
                    }
                    let event = events::new_webhook_event(
                        events::RECORD_ENDED,
                        Payload::Room(recorder.clone()),
                    );
                    let _ = self.webhook_poster.post_event(&event).await;
                    // check record in db, if length is 0, delete it
                    let room_id = recorder.room_info.room_id.clone();
                    let record = match self.db.get_record(&room_id, &live_id).await {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("Record not found in db: {recorder:?}, err={e:?}");
                            continue;
                        }
                    };
                    if record.size == 0 {
                        let platform = PlatformType::from_str(&recorder.room_info.platform)
                            .unwrap_or(PlatformType::BiliBili);
                        let cache_dir = PathBuf::from(&self.config.read().await.cache);
                        if platform == PlatformType::Douyin {
                            match repair_douyin_zero_sized_archive(
                                &self.db, &cache_dir, &room_id, &live_id,
                            )
                            .await
                            {
                                Ok(DouyinZeroSizedArchiveRepair::VideoRepaired) => {
                                    log::warn!(
                                        "Repaired zero-sized Douyin archive from durable cache media: {live_id}"
                                    );
                                    continue;
                                }
                                Ok(DouyinZeroSizedArchiveRepair::ValuableDataOnly) => {
                                    log::info!(
                                        "Preserving data-only Douyin archive without treating it as video: {live_id}"
                                    );
                                    continue;
                                }
                                Err(error) => {
                                    // An unreadable directory or failed DB repair
                                    // is uncertainty, not proof that the archive is
                                    // empty. Never trade a transient error for data
                                    // loss.
                                    log::error!(
                                        "Could not verify zero-sized Douyin archive {live_id}; preserving it: {error}"
                                    );
                                    continue;
                                }
                                Ok(DouyinZeroSizedArchiveRepair::Empty)
                                    if recorder.room_info.status =>
                                {
                                    // The session-level danmu task can still be
                                    // writing into this attempt between HLS
                                    // reconnects. Removing the directory on Linux
                                    // would unlink its open JSONL file.
                                    log::info!(
                                        "Preserving empty active Douyin attempt for lossless danmu recovery: {live_id}"
                                    );
                                    continue;
                                }
                                Ok(DouyinZeroSizedArchiveRepair::Empty) => {}
                            }
                        }

                        if let Err(error) = self.db.remove_record(&live_id).await {
                            log::error!(
                                "Failed to remove empty archive row {live_id}; preserving its cache directory: {error}"
                            );
                            continue;
                        }
                        // remove record folder
                        let cache_folder = cache_dir
                            .join(platform.as_str())
                            .join(&room_id)
                            .join(&live_id);
                        match tokio::fs::remove_dir_all(&cache_folder).await {
                            Ok(()) => log::info!("Record folder removed: {cache_folder:?}"),
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => log::warn!(
                                "Failed to remove empty record folder {cache_folder:?}: {error}"
                            ),
                        }
                    }
                }
                RecorderEvent::ProgressUpdate { id, content } => {
                    self.emitter
                        .emit(&RecorderEvent::ProgressUpdate { id, content });
                }
                RecorderEvent::ProgressFinished {
                    id,
                    success,
                    message,
                } => {
                    self.emitter.emit(&RecorderEvent::ProgressFinished {
                        id,
                        success,
                        message,
                    });
                }
                RecorderEvent::DanmuReceived { room, ts, content } => {
                    self.emitter
                        .emit(&RecorderEvent::DanmuReceived { room, ts, content });
                }
                RecorderEvent::UserNotification { title, body } => {
                    self.emitter
                        .emit(&RecorderEvent::UserNotification { title, body });
                }
            }
        }
    }

    async fn handle_live_end(
        &self,
        platform: PlatformType,
        room_id: &str,
        recorder: &RecorderInfo,
    ) {
        if platform == PlatformType::Douyin {
            let session_id = recorder.platform_live_id.trim();
            if session_id.is_empty() {
                // There is no stable key for either deduplication or recovery.
                // Do not consume any marker and do not guess from another live.
                log::error!("Ignoring Douyin LiveEnd without a session id for room {room_id}");
                return;
            }
            let manager = self.clone();
            let room_id = room_id.to_string();
            let recorder = recorder.clone();
            tokio::spawn(async move {
                manager
                    .run_douyin_whole_session_with_retry(room_id, recorder)
                    .await;
            });
            return;
        }

        let auto_generate = self.config.read().await.auto_generate.clone();
        if !auto_generate.enabled {
            return;
        }

        let encode_danmu = auto_generate.encode_danmu;

        let recorder_id = format!("{}:{}", platform.as_str(), room_id);
        log::info!("Start auto generate for {recorder_id}");
        let parent_id = match live_end_parent_id(&self.db, platform, room_id, recorder).await {
            Ok(Some(parent_id)) => parent_id,
            Ok(None) => {
                log::info!("No recorded archives for ended live: {recorder_id}");
                self.acknowledge_douyin_live_end(platform, room_id, &recorder.platform_live_id)
                    .await;
                return;
            }
            Err(error) => {
                log::error!("Failed to find ended live archives for {recorder_id}: {error}");
                return;
            }
        };

        let Ok(task) = self
            .db
            .generate_task(
                "generate_whole_clip",
                "",
                &serde_json::json!({
                    "platform": platform.as_str(),
                    "room_id": room_id,
                    "parent_id": parent_id.clone(),
                    "encode_danmu": encode_danmu,
                })
                .to_string(),
            )
            .await
        else {
            log::error!("Failed to generate task");
            return;
        };

        let Ok(reporter) = ProgressReporter::new(self.db.clone(), &self.emitter, &task.id).await
        else {
            log::error!("Failed to create reporter");
            let _ = self
                .db
                .update_task(&task.id, "failed", "Failed to create reporter", None)
                .await;
            return;
        };

        log::info!("Create task: {} {}", task.id, task.task_type);

        let self_clone = self.clone();
        let task_id = task.id.clone();
        let room_id = room_id.to_string();
        let ended_session_id = recorder.platform_live_id.clone();
        let enqueue_failure_reporter = reporter.clone();
        let enqueue_failure_task_id = task_id.clone();
        if let Err(error) = self
            .task_manager
            .add_task(Task::new(
                task_id.clone(),
                TaskPriority::Normal,
                async move {
                    if let Err(e) = self_clone
                        .generate_whole_clip(
                            Some(&reporter),
                            GenerateWholeClipParams {
                                encode_danmu,
                                platform: platform.as_str().to_string(),
                                room_id: room_id.clone(),
                                parent_id,
                                selected_live_ids: None,
                                output_name: None,
                            },
                        )
                        .await
                    {
                        log::error!("Failed to generate whole clip: {e}");
                        reporter
                            .finish(false, &format!("Failed to generate whole clip: {e}"))
                            .await;
                        let _ = self_clone
                            .db
                            .update_task(
                                &task_id,
                                "failed",
                                &format!("Failed to generate whole clip: {e}"),
                                None,
                            )
                            .await;
                        return Err(format!("Failed to generate whole clip: {e}"));
                    }

                    reporter
                        .finish(true, "Whole clip generated successfully")
                        .await;
                    let _ = self_clone
                        .db
                        .update_task(
                            &task_id,
                            "success",
                            "Whole clip generated successfully",
                            None,
                        )
                        .await;
                    // Keep the durable recovery marker throughout generation.
                    // A container restart or failed FFmpeg task will therefore
                    // retry the whole-session export on the next startup.
                    self_clone
                        .acknowledge_douyin_live_end(platform, &room_id, &ended_session_id)
                        .await;
                    Ok(())
                },
            ))
            .await
        {
            let message = format!("Failed to enqueue whole clip task: {error}");
            log::error!("{message}");
            enqueue_failure_reporter.finish(false, &message).await;
            let _ = self
                .db
                .update_task(&enqueue_failure_task_id, "failed", &message, None)
                .await;
        }
    }

    async fn resolve_douyin_live_end_parent(
        &self,
        room_id: &str,
        recorder: &RecorderInfo,
    ) -> Result<Option<String>, String> {
        match live_end_parent_id(&self.db, PlatformType::Douyin, room_id, recorder).await {
            Ok(Some(parent_id)) => return Ok(Some(parent_id)),
            Ok(None) => {}
            Err(error) => {
                return Err(format!(
                    "failed to query ended Douyin archives for room {room_id}: {error}"
                ));
            }
        }

        if self.is_migrating.load(std::sync::atomic::Ordering::Relaxed) {
            return Err("cache migration is still in progress".to_string());
        }

        let cache_dir = PathBuf::from(&self.config.read().await.cache);
        let _reconcile_guard = self.douyin_archive_reconcile.lock().await;

        // A different ended session may have repaired the same room while this
        // one waited for the reconciliation lock.
        match live_end_parent_id(&self.db, PlatformType::Douyin, room_id, recorder).await {
            Ok(Some(parent_id)) => return Ok(Some(parent_id)),
            Ok(None) => {}
            Err(error) => {
                return Err(format!(
                    "failed to re-query ended Douyin archives for room {room_id}: {error}"
                ));
            }
        }

        try_rebuild_archives(&self.db, cache_dir.clone())
            .await
            .map_err(|error| {
                format!("failed to reconcile Douyin archive cache for room {room_id}: {error}")
            })?;

        match live_end_parent_id(&self.db, PlatformType::Douyin, room_id, recorder).await {
            Ok(Some(parent_id)) => return Ok(Some(parent_id)),
            Ok(None) => {}
            Err(error) => {
                return Err(format!(
                    "failed to query reconciled Douyin archives for room {room_id}: {error}"
                ));
            }
        }

        let session_id = recorder.platform_live_id.trim();
        if douyin_session_has_cached_media(&self.db, &cache_dir, room_id, session_id)
            .await
            .map_err(|error| {
                format!("failed to inspect Douyin cache for room {room_id}: {error}")
            })?
        {
            return Err(format!(
                "Douyin cache still contains media for session {session_id}, but no usable database archive was reconciled"
            ));
        }

        Ok(None)
    }

    async fn douyin_whole_session_is_complete(
        &self,
        room_id: &str,
        incarnation_token: &str,
        output_file: &str,
    ) -> Result<bool, String> {
        let config = self.config.read().await.clone();
        let cache_dir = PathBuf::from(config.cache);
        let output_path = PathBuf::from(config.output).join(output_file);
        let output_exists = tokio::fs::metadata(&output_path)
            .await
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0);
        if !output_exists {
            return Ok(false);
        }

        let videos = self
            .db
            .get_videos(room_id)
            .await
            .map_err(|error| format!("failed to inspect generated clips: {error}"))?;
        let registered = videos.iter().any(|video| {
            video.platform == PlatformType::Douyin.as_str() && video.file == output_file
        });
        if !registered {
            return Ok(false);
        }

        let completion_path = douyin_whole_completion_path(&cache_dir, room_id, incarnation_token);
        let marker_is_valid = match tokio::fs::read_to_string(&completion_path).await {
            Ok(contents) => {
                serde_json::from_str::<DouyinWholeCompletion>(&contents).is_ok_and(|marker| {
                    marker.incarnation_token == incarnation_token
                        && marker.output_file == output_file
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(format!(
                    "failed to read whole-session completion marker: {error}"
                ));
            }
        };
        if !marker_is_valid {
            // Covers a crash after DB registration but before the marker, and
            // safely repairs a torn/invalid marker. The deterministic output
            // plus its matching database row are the authoritative proof that
            // the expensive FFmpeg work already committed.
            persist_douyin_whole_completion(&cache_dir, room_id, incarnation_token, output_file)
                .await?;
        }
        Ok(true)
    }

    async fn enqueue_douyin_whole_clip_attempt(
        &self,
        room_id: &str,
        parent_id: String,
        encode_danmu: bool,
        output_name: String,
    ) -> Result<(), String> {
        let task = self
            .db
            .generate_task(
                "generate_whole_clip",
                "",
                &serde_json::json!({
                    "platform": PlatformType::Douyin.as_str(),
                    "room_id": room_id,
                    "parent_id": parent_id.clone(),
                    "encode_danmu": encode_danmu,
                    "output_name": output_name.clone(),
                })
                .to_string(),
            )
            .await
            .map_err(|error| format!("failed to create whole-session task: {error}"))?;

        let reporter = match ProgressReporter::new(self.db.clone(), &self.emitter, &task.id).await {
            Ok(reporter) => reporter,
            Err(error) => {
                let message = format!("Failed to create reporter: {error}");
                let _ = self
                    .db
                    .update_task(&task.id, "failed", &message, None)
                    .await;
                return Err(message);
            }
        };
        log::info!(
            "Create retryable Douyin task: {} {}",
            task.id,
            task.task_type
        );

        let manager = self.clone();
        let task_id = task.id.clone();
        let task_id_for_run = task_id.clone();
        let room_id = room_id.to_string();
        let reporter_for_run = reporter.clone();
        let output_name_for_run = output_name.clone();
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let task_future = async move {
            let result = match manager
                .generate_whole_clip(
                    Some(&reporter_for_run),
                    GenerateWholeClipParams {
                        encode_danmu,
                        platform: PlatformType::Douyin.as_str().to_string(),
                        room_id,
                        parent_id,
                        selected_live_ids: None,
                        output_name: Some(output_name_for_run),
                    },
                )
                .await
            {
                Ok(()) => {
                    reporter_for_run
                        .finish(true, "Whole clip generated successfully")
                        .await;
                    let _ = manager
                        .db
                        .update_task(
                            &task_id_for_run,
                            "success",
                            "Whole clip generated successfully",
                            None,
                        )
                        .await;
                    Ok(())
                }
                Err(error) => {
                    let message = format!("Failed to generate whole clip: {error}");
                    log::error!("{message}");
                    reporter_for_run.finish(false, &message).await;
                    let _ = manager
                        .db
                        .update_task(&task_id_for_run, "failed", &message, None)
                        .await;
                    Err(message)
                }
            };
            let _ = completion_tx.send(result.clone());
            result
        };

        if let Err(error) = self
            .task_manager
            .add_task(Task::new(
                task_id.clone(),
                TaskPriority::Normal,
                task_future,
            ))
            .await
        {
            let message = format!("Failed to enqueue whole clip task: {error}");
            log::error!("{message}");
            reporter.finish(false, &message).await;
            let _ = self
                .db
                .update_task(&task_id, "failed", &message, None)
                .await;
            return Err(message);
        }

        completion_rx
            .await
            .map_err(|_| "whole-session task ended without reporting a result".to_string())?
    }

    async fn run_douyin_whole_session_with_retry(&self, room_id: String, recorder: RecorderInfo) {
        let session_id = recorder.platform_live_id.clone();
        let output_name = douyin_whole_output_name(&room_id, &session_id);
        let mut failure_count = 0_u32;
        loop {
            if self.is_migrating.load(std::sync::atomic::Ordering::Relaxed) {
                failure_count = failure_count.saturating_add(1);
                let delay = douyin_whole_retry_delay(failure_count);
                log::info!(
                    "Deferring Douyin whole-session task during cache migration: room={room_id}, session={session_id}, retry_in={delay:?}"
                );
                tokio::time::sleep(delay).await;
                continue;
            }

            match self
                .douyin_whole_session_is_complete(&room_id, &session_id, &output_name)
                .await
            {
                Ok(true) => {
                    log::info!(
                        "Recovered completed Douyin whole-session output: room={room_id}, incarnation={session_id}"
                    );
                    self.acknowledge_douyin_live_end_with_retry(&room_id, &session_id)
                        .await;
                    return;
                }
                Ok(false) => {}
                Err(error) => {
                    failure_count = failure_count.saturating_add(1);
                    let delay = douyin_whole_retry_delay(failure_count);
                    log::error!(
                        "Failed to inspect Douyin whole-session completion: room={room_id}, incarnation={session_id}, error={error}; retrying in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
            }

            let auto_generate = self.config.read().await.auto_generate.clone();
            if !auto_generate.enabled {
                self.acknowledge_douyin_live_end_with_retry(&room_id, &session_id)
                    .await;
                return;
            }

            let attempt = match self
                .resolve_douyin_live_end_parent(&room_id, &recorder)
                .await
            {
                Ok(Some(parent_id)) => {
                    self.enqueue_douyin_whole_clip_attempt(
                        &room_id,
                        parent_id,
                        auto_generate.encode_danmu,
                        output_name.clone(),
                    )
                    .await
                }
                Ok(None) => {
                    log::info!(
                        "No durable media found for ended Douyin session: room={room_id}, session={session_id}"
                    );
                    self.acknowledge_douyin_live_end_with_retry(&room_id, &session_id)
                        .await;
                    return;
                }
                Err(error) => Err(error),
            };

            match attempt {
                Ok(()) => {
                    let cache_dir = PathBuf::from(&self.config.read().await.cache);
                    if let Err(error) = persist_douyin_whole_completion(
                        &cache_dir,
                        &room_id,
                        &session_id,
                        &output_name,
                    )
                    .await
                    {
                        failure_count = failure_count.saturating_add(1);
                        let delay = douyin_whole_retry_delay(failure_count);
                        log::error!(
                            "Failed to persist Douyin whole-session completion: room={room_id}, incarnation={session_id}, error={error}; retrying in {delay:?}"
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    // The session remains claimed for the lifetime of this
                    // process, so a delayed duplicate recovery broadcast cannot
                    // enqueue another long-running export after completion.
                    self.acknowledge_douyin_live_end_with_retry(&room_id, &session_id)
                        .await;
                    return;
                }
                Err(error) => {
                    failure_count = failure_count.saturating_add(1);
                    let delay = douyin_whole_retry_delay(failure_count);
                    log::error!(
                        "Douyin whole-session attempt failed: room={room_id}, session={session_id}, error={error}; retrying in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn acknowledge_douyin_live_end(
        &self,
        platform: PlatformType,
        room_id: &str,
        session_id: &str,
    ) -> bool {
        if platform != PlatformType::Douyin {
            return true;
        }
        let cache_dir = PathBuf::from(&self.config.read().await.cache);
        let acknowledged = acknowledge_live_end(&cache_dir, room_id, session_id).await;
        if !acknowledged {
            log::warn!(
                "Douyin whole-session completion did not clear the matching recovery marker"
            );
        }
        acknowledged
    }

    async fn acknowledge_douyin_live_end_with_retry(&self, room_id: &str, session_id: &str) {
        retry_until_douyin_live_end_acknowledged(
            || self.acknowledge_douyin_live_end(PlatformType::Douyin, room_id, session_id),
            |failure_count| {
                let delay = douyin_whole_retry_delay(failure_count);
                log::warn!(
                    "Retrying Douyin whole-session marker acknowledgement: room={room_id}, session={session_id}, retry_in={delay:?}"
                );
                delay
            },
        )
        .await;
    }

    pub fn set_migrating(&self, migrating: bool) {
        self.is_migrating
            .store(migrating, std::sync::atomic::Ordering::Relaxed);
    }

    async fn monitor_recorders(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            if self.is_migrating.load(std::sync::atomic::Ordering::Relaxed) {
                interval.tick().await;
                continue;
            }
            // get a list of recorders in db, if not created yet, create them
            let recorders = self.db.get_recorders().await;
            if recorders.is_err() {
                log::error!(
                    "Failed to get recorders from db: {}",
                    recorders.err().unwrap()
                );
                return;
            }
            let recorders = recorders.unwrap();
            let mut recorder_map = HashMap::new();
            for recorder in recorders {
                let platform = PlatformType::from_str(&recorder.platform).unwrap();
                let room_id = recorder.room_id;
                let auto_start = recorder.auto_start;
                let extra = recorder.extra;
                recorder_map.insert((platform, room_id), (auto_start, extra));
            }
            let mut recorders_to_add = Vec::new();
            for (platform, room_id) in recorder_map.keys() {
                let recorder_id = format!("{}:{}", platform.as_str(), room_id);
                if !self.recorders.read().await.contains_key(&recorder_id)
                    && !self.to_remove.read().await.contains(&recorder_id)
                {
                    recorders_to_add.push((*platform, room_id.clone()));
                }
            }
            for (platform, room_id) in recorders_to_add {
                if self.is_migrating.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let (auto_start, extra) = recorder_map.get(&(platform, room_id.clone())).unwrap();
                let account = self
                    .db
                    .get_account_by_platform(platform.clone().as_str())
                    .await;
                if platform != PlatformType::Huya
                    && platform != PlatformType::Kuaishou
                    && platform != PlatformType::TikTok
                    && account.is_err()
                {
                    log::warn!("Failed to find an account for {platform:?} {room_id}");
                    continue;
                }
                let account = if let Ok(account) = account {
                    account.to_account()
                } else {
                    Account::default()
                };

                if let Err(e) = self
                    .add_recorder(&account, platform, &room_id, extra, *auto_start)
                    .await
                {
                    log::error!(
                        "Failed to add recorder: {} {} {}",
                        platform.as_str(),
                        room_id,
                        e
                    );
                }
            }
            interval.tick().await;
        }
    }

    pub async fn add_recorder(
        &self,
        account: &Account,
        platform: PlatformType,
        room_id: &str,
        extra: &str,
        enabled: bool,
    ) -> Result<(), RecorderManagerError> {
        let recorder_id = format!("{}:{}", platform.as_str(), room_id);
        if self.recorders.read().await.contains_key(&recorder_id) {
            return Err(RecorderManagerError::AlreadyExisted {
                room_id: room_id.to_string(),
            });
        }

        let cache_dir = self.config.read().await.cache.clone();
        let cache_dir = PathBuf::from(&cache_dir);

        let event_tx = self.get_event_sender();
        let update_interval = self.config.read().await.update_interval.clone();
        let recorder: RecorderType = match platform {
            PlatformType::BiliBili => RecorderType::BiliBili(
                BiliRecorder::new(
                    room_id,
                    account,
                    cache_dir,
                    event_tx,
                    update_interval,
                    enabled,
                    self.db.clone(),
                )
                .await?,
            ),
            PlatformType::Douyin => {
                // High-volume realtime danmu must not share the lifecycle
                // channel used for RecordStart/RecordEnd/LiveEnd. A burst over
                // the broadcast capacity could otherwise drop the LiveEnd that
                // schedules automatic whole-session generation.
                let realtime_emitter = self.emitter.clone();
                let realtime_event_sink = Arc::new(move |event: RecorderEvent| {
                    realtime_emitter.emit(&event);
                });
                RecorderType::Douyin(
                    DouyinRecorder::new(
                        room_id,
                        extra,
                        account,
                        cache_dir,
                        event_tx,
                        realtime_event_sink,
                        update_interval,
                        enabled,
                    )
                    .await?,
                )
            }
            PlatformType::Huya => RecorderType::Huya(
                HuyaRecorder::new(
                    room_id,
                    account,
                    cache_dir,
                    event_tx,
                    update_interval,
                    enabled,
                )
                .await?,
            ),
            PlatformType::Kuaishou => RecorderType::Kuaishou(
                KuaishouRecorder::new(
                    room_id,
                    account,
                    cache_dir,
                    event_tx,
                    update_interval,
                    enabled,
                )
                .await?,
            ),
            PlatformType::TikTok => RecorderType::TikTok(
                TikTokRecorder::new(
                    room_id,
                    account,
                    cache_dir,
                    event_tx,
                    update_interval,
                    enabled,
                )
                .await?,
            ),
            _ => {
                return Err(RecorderManagerError::InvalidPlatformType {
                    platform: platform.as_str().to_string(),
                })
            }
        };
        self.recorders
            .write()
            .await
            .insert(recorder_id.clone(), recorder);
        if let Some(recorder_ref) = self.recorders.read().await.get(&recorder_id) {
            recorder_ref.run().await;
        }
        Ok(())
    }

    pub async fn stop_all(&self) {
        let recorders = self.recorders.read().await;
        futures::future::join_all(recorders.values().map(|recorder| recorder.stop())).await;
        drop(recorders);

        // remove all recorders
        self.recorders.write().await.clear();
    }

    /// Remove a recorder from the manager
    ///
    /// This will stop the recorder and remove it from the manager
    /// and remove the related cache folder
    pub async fn remove_recorder(
        &self,
        platform: PlatformType,
        room_id: &str,
    ) -> Result<RecorderRow, RecorderManagerError> {
        // check recorder exists
        let recorder_id = format!("{}:{}", platform.as_str(), room_id);
        if !self.recorders.read().await.contains_key(&recorder_id) {
            return Err(RecorderManagerError::NotFound {
                room_id: room_id.to_string(),
            });
        }

        // remove from db
        let recorder = self.db.remove_recorder(room_id).await?;

        // add to to_remove
        log::debug!("Add to to_remove: {recorder_id}");
        self.to_remove.write().await.insert(recorder_id.clone());

        // stop recorder
        log::debug!("Stop recorder: {recorder_id}");
        if let Some(recorder_ref) = self.recorders.read().await.get(&recorder_id) {
            recorder_ref.stop().await;
        }

        // remove recorder
        log::debug!("Remove recorder from manager: {recorder_id}");
        self.recorders.write().await.remove(&recorder_id);

        // remove from to_remove
        log::debug!("Remove from to_remove: {recorder_id}");
        self.to_remove.write().await.remove(&recorder_id);

        // remove related cache folder
        let cache_folder = format!(
            "{}/{}/{}",
            self.config.read().await.cache,
            platform.as_str(),
            room_id
        );
        log::debug!("Remove cache folder: {cache_folder}");
        let _ = tokio::fs::remove_dir_all(cache_folder).await;
        log::info!("Recorder {room_id} cache folder removed");

        Ok(recorder)
    }

    async fn load_playlist_bytes(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<Vec<u8>, RecorderManagerError> {
        let cache_path = self.config.read().await.cache.clone();
        let cache_path = Path::new(&cache_path);
        let playlist_path = cache_path
            .join(platform.as_str())
            .join(room_id)
            .join(live_id)
            .join("playlist.m3u8");
        if !playlist_path.exists() {
            return Err(RecorderManagerError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Playlist file not found",
            )));
        }
        let mut bytes: Vec<u8> = Vec::new();
        tokio::fs::File::open(playlist_path)
            .await
            .unwrap()
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        Ok(bytes)
    }

    /// Check if the playlist is outdated
    ///
    /// This will check if the current recorder live id is the same as the live id
    /// and if the current recorder is recording
    /// and if the current recorder is recording, return false
    /// otherwise, return true
    async fn is_outdated_playlist(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> bool {
        // check current recorder live id is the same as the live id
        let recorder = self.get_recorder_info(platform, room_id).await;
        let Some(recorder) = recorder else {
            return true;
        };

        if recorder.live_id != live_id {
            return true;
        }

        false
    }

    async fn load_playlist(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<MediaPlaylist, RecorderManagerError> {
        let bytes = self.load_playlist_bytes(platform, room_id, live_id).await?;
        if let Result::Ok((_, mut pl)) = m3u8_rs::parse_media_playlist(&bytes) {
            if self.is_outdated_playlist(platform, room_id, live_id).await {
                pl.end_list = true;
                pl.playlist_type = Some(MediaPlaylistType::Vod);
            }
            return Ok(pl);
        }
        Err(RecorderManagerError::M3u8ParseFailed {
            content: String::from_utf8_lossy(&bytes).into_owned(),
        })
    }

    async fn playlist_range(
        &self,
        playlist: &MediaPlaylist,
        range: Option<Range>,
    ) -> Result<MediaPlaylist, RecorderManagerError> {
        let mut playlist = playlist.clone();
        if let Some(range) = range {
            let mut duration = 0.0f64;
            let mut segments = Vec::new();
            for s in playlist.segments {
                if range.is_in(duration) || range.is_in(duration + s.duration as f64) {
                    segments.push(s.clone());
                }
                duration += s.duration as f64;
            }
            playlist.segments = segments;
            playlist.end_list = true;
            playlist.playlist_type = Some(MediaPlaylistType::Vod);
        }

        Ok(playlist)
    }

    async fn first_segment_timestamp(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<i64, RecorderManagerError> {
        let playlist = self.load_playlist(platform, room_id, live_id).await?;
        if playlist.segments.is_empty() {
            return Err(RecorderManagerError::EmptyPlaylist);
        }

        let first_segment = playlist.segments.first().unwrap();
        if let Some(program_date_time) = first_segment.program_date_time {
            return Ok(program_date_time.timestamp_millis());
        }

        // else, find in unknown tags
        let program_date_time = first_segment
            .unknown_tags
            .iter()
            .find(|t| t.tag == "X-PROGRAM-DATE-TIME");

        let Some(program_date_time) = program_date_time else {
            return live_id
                .parse::<i64>()
                .map_err(|_| RecorderManagerError::InvalidLiveID);
        };

        let Some(value) = &program_date_time.rest else {
            return live_id
                .parse::<i64>()
                .map_err(|_| RecorderManagerError::InvalidLiveID);
        };

        // example: "2025-10-18T17:18:17.004+0800"
        // convert to timestamp
        let timestamp = DateTime::parse_from_rfc3339(value)
            .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f%z"))
            .map_err(|error| RecorderManagerError::HLSError {
                err: format!("Invalid X-PROGRAM-DATE-TIME {value}: {error}"),
            })?
            .timestamp_millis();
        Ok(timestamp)
    }

    pub async fn load_danmus(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<Vec<DanmuEntry>, RecorderManagerError> {
        let cache_path = self.config.read().await.cache.clone();
        let cache_path = Path::new(&cache_path);
        let mut danmus_path = cache_path
            .join(platform.as_str())
            .join(room_id)
            .join(live_id)
            .join("events.jsonl");
        if !danmus_path.exists() {
            // Keep existing archives readable after the storage format
            // migration. New recordings only create events.jsonl.
            let legacy_path = danmus_path.with_file_name("danmu.txt");
            if !legacy_path.exists() {
                return Ok(Vec::new());
            }
            danmus_path = legacy_path;
        }
        Ok(DanmuStorage::read_entries(&danmus_path, 0).await)
    }

    /// Load danmu timestamps relative to the first recorded media segment.
    /// This is intended for analytics and tools which operate on video time,
    /// while `load_danmus` keeps absolute timestamps for the live player.
    pub async fn load_relative_danmus(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<Vec<DanmuEntry>, RecorderManagerError> {
        let mut danmus = self.load_danmus(platform, room_id, live_id).await?;
        if danmus.is_empty() {
            return Ok(danmus);
        }
        let start = self
            .first_segment_timestamp(platform, room_id, live_id)
            .await
            .unwrap_or_else(|_| {
                danmus
                    .iter()
                    .map(|entry| entry.ts)
                    .min()
                    .unwrap_or_default()
            });
        danmus.retain(|entry| entry.ts >= start);
        for entry in &mut danmus {
            entry.ts -= start;
        }
        Ok(danmus)
    }

    /// Load the complete live events for exports which must retain user and
    /// provider-specific metadata.
    pub async fn load_danmu_events(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<Vec<LiveEvent>, RecorderManagerError> {
        let cache_path = self.config.read().await.cache.clone();
        let cache_path = Path::new(&cache_path);
        let mut events_path = cache_path
            .join(platform.as_str())
            .join(room_id)
            .join(live_id)
            .join("events.jsonl");
        if !events_path.exists() {
            let legacy_path = events_path.with_file_name("danmu.txt");
            if !legacy_path.exists() {
                return Ok(Vec::new());
            }
            events_path = legacy_path;
        }
        Ok(DanmuStorage::read_events(&events_path).await)
    }

    /// Get related playlists by parent id
    ///
    /// This will return a list of tuples, the first element is the title of the archive,
    /// the second element is the path of the playlist
    async fn get_related_playlists(
        &self,
        platform: &PlatformType,
        room_id: &str,
        parent_id: &str,
    ) -> Result<Vec<RelatedPlaylist>, RecorderManagerError> {
        let cache_path = self.config.read().await.cache.clone();
        let cache_path = Path::new(&cache_path);
        let mut archives = self
            .db
            .get_archives_by_parent_id(room_id, parent_id)
            .await?;
        archives.retain(|archive| archive.platform == platform.as_str());
        // Rebuilt rows receive a fresh created_at in directory iteration order,
        // which is not chronological. Douyin attempt IDs are epoch millis, so
        // prefer their numeric order and use the DB timestamp as a fallback.
        if *platform == PlatformType::Douyin
            && archives
                .iter()
                .all(|archive| archive.live_id.parse::<i128>().is_ok())
        {
            archives.sort_by_key(|archive| archive.live_id.parse::<i128>().unwrap_or_default());
        } else {
            archives.sort_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.live_id.cmp(&right.live_id))
            });
        }

        let mut playlists = Vec::with_capacity(archives.len());
        for archive in archives {
            let work_dir = CachePath::new(
                cache_path.to_path_buf(),
                *platform,
                room_id,
                archive.live_id.as_str(),
            );
            let playlist_path = work_dir.with_filename("playlist.m3u8").full_path();
            if *platform == PlatformType::Douyin
                && !douyin_playlist_has_local_media_segment(&work_dir.full_path(), &playlist_path)
                    .await?
            {
                log::warn!(
                    "Skipping non-playable Douyin recording attempt {} from whole-session generation",
                    archive.live_id
                );
                continue;
            }
            if archive.size <= 0 {
                let has_playlist = tokio::fs::metadata(&playlist_path)
                    .await
                    .map(|metadata| metadata.is_file() && metadata.len() > 0)
                    .unwrap_or(false);
                if !has_playlist {
                    log::warn!(
                        "Skipping empty recording attempt {} from whole-session generation",
                        archive.live_id
                    );
                    continue;
                }
            }
            playlists.push(RelatedPlaylist {
                live_id: archive.live_id,
                title: archive.title,
                path: playlist_path,
            });
        }

        Ok(playlists)
    }

    pub async fn clip_range(
        &self,
        reporter: Option<&ProgressReporter>,
        clip_file: PathBuf,
        params: &ClipRangeParams,
    ) -> Result<PathBuf, RecorderManagerError> {
        let cache_path = self.config.read().await.cache.clone();
        let cache_path = Path::new(&cache_path);
        let playlist_path = cache_path
            .join(params.platform.clone())
            .join(params.room_id.clone())
            .join(params.live_id.clone())
            .join("playlist.m3u8");

        if !playlist_path.exists() {
            log::error!("Playlist file not found: {}", playlist_path.display());
            return Err(RecorderManagerError::ClipError {
                err: "Playlist file not found".to_string(),
            });
        }

        if params.ranges.is_empty() {
            crate::ffmpeg::playlist::clip_from_playlist(reporter, &playlist_path, &clip_file, None)
                .await
                .map_err(|e| RecorderManagerError::ClipError { err: e.to_string() })?;
        } else {
            crate::ffmpeg::playlist::clip_multiple_from_playlist(
                reporter,
                &playlist_path,
                &clip_file,
                &params.ranges,
                params.transition.as_deref(),
            )
            .await
            .map_err(|e| RecorderManagerError::ClipError { err: e.to_string() })?;
        }

        if params.fix_encoding && !params.danmu {
            // transcode clip_file
            let tmp_clip_file = clip_file.with_extension("tmp.mp4");
            if let Err(e) = transcode(reporter, &clip_file, &tmp_clip_file, false).await {
                log::error!("Failed to transcode clip file: {e}");
                return Err(RecorderManagerError::ClipError { err: e.to_string() });
            }

            // remove clip_file
            let _ = tokio::fs::remove_file(&clip_file).await;

            // rename tmp_clip_file to clip_file
            let _ = tokio::fs::rename(tmp_clip_file, &clip_file).await;
        }

        if !params.danmu {
            log::info!("Skip danmu encoding");
            return Ok(clip_file);
        }

        let Ok(platform) = PlatformType::from_str(&params.platform) else {
            return Err(RecorderManagerError::InvalidPlatformType {
                platform: params.platform.clone(),
            });
        };
        let stream_start_timestamp_milis = self
            .first_segment_timestamp(platform, &params.room_id, &params.live_id)
            .await?;

        let danmus = self
            .load_danmus(platform, &params.room_id, &params.live_id)
            .await;
        if danmus.is_err() {
            log::error!(
                "Failed to get danmus, skip danmu encoding: {}",
                danmus.err().unwrap()
            );
            return Ok(clip_file);
        }

        let mut danmus = danmus.unwrap();
        log::debug!("First danmu entry: {:?}", danmus.first());
        log::debug!("Last danmu entry: {:?}", danmus.last());
        log::debug!("Stream start timestamp: {}", stream_start_timestamp_milis);
        log::debug!("Local offset: {}", params.local_offset);
        log::debug!("Range: {:?}", params.ranges);

        // update danmu entry ts to relative offset
        for d in &mut danmus {
            d.ts -= stream_start_timestamp_milis + params.local_offset * 1000;
        }

        let range_anchors = clip_timeline_anchors(&params.ranges, params.transition.as_deref());

        log::debug!("Range anchors: {:?}", range_anchors);

        let mut filtered_danmus = Vec::<DanmuEntry>::new();
        for (i, range) in params.ranges.iter().enumerate() {
            filtered_danmus.extend(self.filter_danmus_in_range(
                danmus.clone(),
                range,
                range_anchors[i],
            ));
        }

        let ass_content = danmu2ass::danmu_to_ass(
            filtered_danmus,
            self.config.read().await.danmu_ass_options.clone(),
        );
        // dump ass_content into a temp file
        let ass_file_path = clip_file.with_extension("ass");
        if let Err(e) = write(&ass_file_path, ass_content).await {
            log::error!(
                "Failed to write temp ass file: {} {}",
                ass_file_path.display(),
                e
            );
            return Ok(clip_file);
        }

        let result = encode_video_danmu(reporter, &clip_file, &ass_file_path).await;
        // clean ass file
        let _ = remove_file(ass_file_path).await;
        let _ = remove_file(clip_file).await;

        result.map_err(|e| RecorderManagerError::ClipError { err: e })
    }

    fn filter_danmus_in_range(
        &self,
        mut danmus: Vec<DanmuEntry>,
        range: &Range,
        anchor: i64,
    ) -> Vec<DanmuEntry> {
        for d in &mut danmus {
            d.ts -= (range.start * 1000.0) as i64;
        }
        if range.duration() > 0.0 {
            danmus.retain(|x| x.ts >= 0 && x.ts <= (range.duration() * 1000.0).round() as i64);
        }

        for d in &mut danmus {
            d.ts += anchor;
        }

        danmus
    }

    async fn generate_archive_danmu_ass(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<PathBuf, RecorderManagerError> {
        log::info!(
            "Generate archive danmu ass file for {} {} {}",
            platform.as_str(),
            room_id,
            live_id
        );
        let first_segment_timestamp_milis = self
            .first_segment_timestamp(platform, room_id, live_id)
            .await?;
        let mut danmus = self.load_danmus(platform, room_id, live_id).await?;
        danmus.retain(|x| x.ts >= first_segment_timestamp_milis);
        for d in &mut danmus {
            d.ts -= first_segment_timestamp_milis;
        }
        let ass_content =
            danmu2ass::danmu_to_ass(danmus, self.config.read().await.danmu_ass_options.clone());
        let work_dir = CachePath::new(
            self.config.read().await.cache.clone().into(),
            platform,
            room_id,
            live_id,
        );
        let ass_file_path = work_dir.with_filename("danmu.ass");
        if let Err(e) = write(&ass_file_path.full_path(), ass_content).await {
            log::error!(
                "Failed to write archive danmu ass file: {} {}",
                ass_file_path.full_path().display(),
                e
            );
            return Err(RecorderManagerError::ArchiveDanmuAssGenerationFailed {
                error: e.to_string(),
            });
        }
        Ok(ass_file_path.full_path())
    }

    pub async fn get_recorder_list(&self) -> RecorderList {
        let mut summary = RecorderList {
            count: 0,
            recorders: Vec::new(),
        };

        // initialized recorder set
        let mut recorder_set = HashSet::new();
        for recorder_ref in self.recorders.read().await.iter() {
            let recorder_info = recorder_ref.1.info().await;
            summary.recorders.push(recorder_info.clone());
            recorder_set.insert(recorder_info.room_info.room_id);
        }

        // get recorders from db
        let recorders = self.db.get_recorders().await;
        if recorders.is_err() {
            log::error!(
                "Failed to get recorders from db: {}",
                recorders.err().unwrap()
            );
            return summary;
        }
        let recorders = recorders.unwrap();
        summary.count = recorders.len();
        for recorder in recorders {
            // check if recorder is in recorder_set
            if !recorder_set.contains(&recorder.room_id.to_string()) {
                summary.recorders.push(RecorderInfo {
                    platform_live_id: "".to_string(),
                    live_id: "".to_string(),
                    recording: false,
                    enabled: false,
                    room_info: RoomInfo {
                        platform: recorder.platform.as_str().to_string(),
                        status: false,
                        room_id: recorder.room_id.to_string(),
                        room_title: recorder.room_id.to_string(),
                        room_cover: "".to_string(),
                    },
                    user_info: UserInfo {
                        user_id: "".to_string(),
                        user_name: "".to_string(),
                        user_avatar: "".to_string(),
                    },
                });
            }
        }

        summary
            .recorders
            .sort_by(|a, b| a.room_info.room_id.cmp(&b.room_info.room_id));
        summary
    }

    pub async fn get_recorder_info(
        &self,
        platform: PlatformType,
        room_id: &str,
    ) -> Option<RecorderInfo> {
        let recorder_id = format!("{}:{}", platform.as_str(), room_id);
        if let Some(recorder_ref) = self.recorders.read().await.get(&recorder_id) {
            let room_info = recorder_ref.info().await;
            Some(room_info)
        } else {
            None
        }
    }

    pub async fn get_archive_disk_usage(&self) -> Result<i64, RecorderManagerError> {
        Ok(self.db.get_record_disk_usage().await?)
    }

    pub async fn get_archives(
        &self,
        room_id: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<RecordRow>, RecorderManagerError> {
        Ok(self.db.get_records(room_id, offset, limit).await?)
    }

    pub async fn get_archive(
        &self,
        room_id: &str,
        live_id: &str,
    ) -> Result<RecordRow, RecorderManagerError> {
        Ok(self.db.get_record(room_id, live_id).await?)
    }

    pub async fn get_archive_subtitle(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<String, RecorderManagerError> {
        // read subtitle file under work_dir
        let work_dir = CachePath::new(
            self.config.read().await.cache.clone().into(),
            platform,
            room_id,
            live_id,
        );
        let subtitle_file_path = work_dir.with_filename("subtitle.srt");
        let subtitle_file = File::open(subtitle_file_path.full_path()).await;
        if subtitle_file.is_err() {
            return Err(RecorderManagerError::SubtitleNotFound {
                live_id: live_id.to_string(),
            });
        }
        let subtitle_file = subtitle_file.unwrap();
        let mut subtitle_file = BufReader::new(subtitle_file);
        let mut subtitle_content = String::new();
        subtitle_file.read_to_string(&mut subtitle_content).await?;
        Ok(subtitle_content)
    }

    pub async fn generate_archive_subtitle(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<String, RecorderManagerError> {
        // generate subtitle file under work_dir
        let work_dir = CachePath::new(
            self.config.read().await.cache.clone().into(),
            platform,
            room_id,
            live_id,
        );
        let subtitle_file_path = work_dir.with_filename("subtitle.srt");
        let mut subtitle_file = File::create(subtitle_file_path.full_path()).await?;
        // first generate a tmp clip file
        // generate a tmp m3u8 index file
        let m3u8_index_file_path = work_dir.with_filename("tmp.m3u8");
        let mut playlist = self.load_playlist(platform, room_id, live_id).await?;
        playlist.end_list = true;
        playlist.playlist_type = Some(MediaPlaylistType::Vod);

        let mut v: Vec<u8> = Vec::new();
        playlist.write_to(&mut v).unwrap();
        let m3u8_content: &str = std::str::from_utf8(&v).unwrap();
        tokio::fs::write(&m3u8_index_file_path.full_path(), m3u8_content).await?;
        log::info!(
            "[{}]M3U8 index file generated: {}",
            room_id,
            m3u8_index_file_path.full_path().display()
        );

        // Generate a tmp mp4 clip file first
        let clip_file_path = work_dir.with_filename("tmp.mp4");
        if let Err(e) = crate::ffmpeg::playlist::clip_from_playlist(
            None::<&crate::progress::progress_reporter::ProgressReporter>,
            Path::new(&m3u8_index_file_path.full_path()),
            Path::new(&clip_file_path.full_path()),
            None,
        )
        .await
        {
            return Err(RecorderManagerError::SubtitleGenerationFailed {
                error: e.to_string(),
            });
        }
        log::info!("[{}]Temp clip file generated: {}", room_id, clip_file_path);

        // Read config to determine generator type
        let config = self.config.read().await;
        let generator_type = config.subtitle_generator_type.as_str();

        // For third-party services (powerlive), extract opus audio from mp4
        let media_file_path = if generator_type == "powerlive" {
            let opus_file_path = work_dir.with_filename("tmp.opus");
            log::info!("[{}]Extracting opus audio for third-party service", room_id);

            // Extract opus audio using FFmpeg
            let ffmpeg_path = crate::ffmpeg::ffmpeg_path();
            let mut cmd = tokio::process::Command::new(ffmpeg_path);
            cmd.args([
                "-i",
                clip_file_path.full_path().to_str().unwrap(),
                "-vn", // no video
                "-acodec",
                "libopus",
                "-b:a",
                "128k",
                "-y",
                opus_file_path.full_path().to_str().unwrap(),
            ]);

            let output =
                cmd.output()
                    .await
                    .map_err(|e| RecorderManagerError::SubtitleGenerationFailed {
                        error: format!("Failed to run FFmpeg: {}", e),
                    })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(RecorderManagerError::SubtitleGenerationFailed {
                    error: format!("Failed to extract opus audio: {}", stderr),
                });
            }

            log::info!("[{}]Opus audio extracted: {}", room_id, opus_file_path);
            opus_file_path
        } else {
            // For whisper/whisper_online, use mp4 directly
            clip_file_path
        };

        // generate subtitle file
        let resource_dir = self.resource_dir.clone();
        let result = crate::ffmpeg::generate_video_subtitle(
            None,
            Path::new(&media_file_path.full_path()),
            &config.subtitle_generator_type,
            &resource_dir,
            &config.whisper_model,
            &config.whisper_prompt,
            &config.openai_api_key,
            &config.openai_api_endpoint,
            &config.whisper_language,
        )
        .await;
        // write subtitle file
        if let Err(e) = result {
            return Err(RecorderManagerError::SubtitleGenerationFailed {
                error: e.to_string(),
            });
        }
        log::info!("[{room_id}]Subtitle generated");
        let result = result.unwrap();
        let subtitle_content = result
            .subtitle_content
            .iter()
            .map(item_to_srt)
            .collect::<String>();
        subtitle_file.write_all(subtitle_content.as_bytes()).await?;
        log::info!("[{room_id}]Subtitle file written");
        // remove tmp files
        tokio::fs::remove_file(&m3u8_index_file_path.full_path()).await?;

        // Remove both mp4 and opus files if they exist
        let clip_file_path = work_dir.with_filename("tmp.mp4");
        let _ = tokio::fs::remove_file(&clip_file_path.full_path()).await;

        let opus_file_path = work_dir.with_filename("tmp.opus");
        let _ = tokio::fs::remove_file(&opus_file_path.full_path()).await;

        log::info!("[{room_id}]Tmp files removed");
        Ok(subtitle_content)
    }

    pub async fn delete_archive(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_id: &str,
    ) -> Result<RecordRow, RecorderManagerError> {
        log::info!("Deleting archive {room_id}:{live_id}");
        let to_delete = self.db.remove_record(live_id).await?;
        let cache_folder = Path::new(self.config.read().await.cache.as_str())
            .join(platform.as_str())
            .join(room_id)
            .join(live_id);
        let _ = tokio::fs::remove_dir_all(cache_folder).await;
        Ok(to_delete)
    }

    pub async fn delete_archives(
        &self,
        platform: PlatformType,
        room_id: &str,
        live_ids: &[&str],
    ) -> Result<Vec<RecordRow>, RecorderManagerError> {
        log::info!("Deleting archives in batch: {live_ids:?}");
        let mut to_deletes = Vec::new();
        for live_id in live_ids {
            let to_delete = self.delete_archive(platform, room_id, live_id).await?;
            to_deletes.push(to_delete);
        }
        Ok(to_deletes)
    }

    pub async fn handle_hls_request(&self, uri: &str) -> Result<Vec<u8>, RecorderManagerError> {
        let cache_path = self.config.read().await.cache.clone();
        let path = uri.split('?').next().unwrap_or(uri);
        let params = uri.split('?').nth(1).unwrap_or("");
        let path_segs: Vec<&str> = path.split('/').collect();

        if path_segs.len() < 4 {
            log::warn!("Invalid request path: {path}");
            return Err(RecorderManagerError::HLSError {
                err: "Invalid hls path".into(),
            });
        }
        // parse recorder type
        let platform = path_segs[0];
        // parse room id
        let room_id = path_segs[1];
        // parse live id
        let live_id = path_segs[2];

        let params = Some(params);

        // parse params, example: start=10&end=20
        // start and end are optional
        // split params by &, and then split each param by =
        let params = if let Some(params) = params {
            let params = params
                .split('&')
                .map(|param| param.split('=').collect::<Vec<&str>>())
                .collect::<Vec<Vec<&str>>>();
            Some(params)
        } else {
            None
        };

        let start = if let Some(params) = &params {
            params
                .iter()
                .find(|param| param[0] == "start")
                .map_or(0, |param| param[1].parse::<i64>().unwrap())
        } else {
            0
        };
        let end = if let Some(params) = &params {
            params
                .iter()
                .find(|param| param[0] == "end")
                .map_or(0, |param| param[1].parse::<i64>().unwrap())
        } else {
            0
        };

        let platform = PlatformType::from_str(platform).map_err(|_| {
            RecorderManagerError::InvalidPlatformType {
                platform: platform.to_string(),
            }
        })?;

        let range = if start != 0 || end != 0 {
            Some(Range {
                start: start as f64,
                end: end as f64,
            })
        } else {
            None
        };

        // Check if this is a playlist request
        // The remaining path after platform/room_id/live_id could be:
        // - "playlist.m3u8" (4 segments total)
        // - "some_dir/playlist.m3u8" (5+ segments)
        // - "segment.ts" (4 segments total)
        // - "some_dir/segment.ts" (5+ segments)
        let remaining_path = path_segs[3..].join("/");

        if remaining_path == "playlist.m3u8" || remaining_path.ends_with("/playlist.m3u8") {
            let playlist = self.load_playlist(platform, room_id, live_id).await?;
            let playlist = self.playlist_range(&playlist, range).await?;
            let mut bytes: Vec<u8> = Vec::new();
            playlist.write_to(&mut bytes).unwrap();
            Ok(bytes)
        } else {
            // try to find requested ts file in recorder's cache
            // cache files are stored in {cache_dir}/{room_id}/{timestamp}/{ts_file}
            // remove path params
            let path = path.split('?').next().unwrap_or(path);
            let ts_file = format!("{}/{}", cache_path, path.replace("%7C", "|"));
            let ts_file_content = tokio::fs::read(&ts_file).await;
            if ts_file_content.is_err() {
                log::warn!("Segment file not found: {ts_file}");
                return Err(RecorderManagerError::HLSError {
                    err: "Segment file not found".into(),
                });
            }

            Ok(ts_file_content.unwrap())
        }
    }

    pub async fn set_enable(&self, platform: PlatformType, room_id: &str, enabled: bool) {
        // update RecordRow auto_start field
        if let Err(e) = self.db.update_recorder(platform, room_id, enabled).await {
            log::error!("Failed to update recorder auto_start: {e}");
        }

        let recorder_id = format!("{}:{}", platform.as_str(), room_id);
        if let Some(recorder_ref) = self.recorders.read().await.get(&recorder_id) {
            if enabled {
                recorder_ref.enable().await;
            } else {
                recorder_ref.disable().await;
            }
        }
    }

    pub async fn generate_whole_clip(
        &self,
        reporter: Option<&ProgressReporter>,
        params: GenerateWholeClipParams,
    ) -> Result<(), RecorderManagerError> {
        let GenerateWholeClipParams {
            encode_danmu,
            platform,
            room_id,
            parent_id,
            selected_live_ids,
            output_name,
        } = params;

        let platform = PlatformType::from_str(&platform).map_err(|_| {
            RecorderManagerError::InvalidPlatformType {
                platform: platform.to_string(),
            }
        })?;

        let mut playlists = self
            .get_related_playlists(&platform, &room_id, &parent_id)
            .await?;
        if playlists.is_empty() {
            log::error!("No related playlists found: {parent_id}");
            return Err(RecorderManagerError::EmptyPlaylist);
        }

        if let Some(selected_live_ids) = selected_live_ids {
            let mut by_id = std::collections::HashMap::new();
            for playlist in playlists {
                by_id.insert(playlist.live_id.clone(), playlist);
            }
            let mut ordered = Vec::new();
            for live_id in selected_live_ids {
                if let Some(playlist) = by_id.remove(&live_id) {
                    ordered.push(playlist);
                }
            }
            playlists = ordered;
        }

        if playlists.is_empty() {
            log::error!("No selected playlists found: {parent_id}");
            return Err(RecorderManagerError::EmptyPlaylist);
        }

        let title = playlists.first().unwrap().title.clone();

        // generate archive danmu ass file for all playlists
        let danmu_ass_files = if encode_danmu {
            let live_ids = playlists
                .iter()
                .map(|playlist| playlist.live_id.clone())
                .collect::<Vec<_>>();
            let danmu_ass_results = playlists
                .iter()
                .map(async |p| {
                    self.generate_archive_danmu_ass(platform, &room_id, &p.live_id)
                        .await
                })
                .collect::<Vec<_>>();

            require_danmu_ass_files(
                &live_ids,
                futures::future::join_all(danmu_ass_results).await,
            )?
        } else {
            vec![None; playlists.len()]
        };

        let output_filename = if let Some(output_name) = output_name {
            let trimmed = output_name.trim();
            if trimmed.is_empty() {
                None
            } else {
                let mut sanitized = sanitize_filename::sanitize(trimmed);
                if !sanitized.to_lowercase().ends_with(".mp4") {
                    sanitized.push_str(".mp4");
                }
                Some(std::path::PathBuf::from(sanitized))
            }
        } else {
            None
        };

        let output_filename = if let Some(output_filename) = output_filename {
            output_filename
        } else {
            let timestamp = chrono::Local::now().format("%Y%m%d%H%M%S").to_string();
            let sanitized_filename = sanitize_filename::sanitize(format!(
                "[full][{platform:?}][{room_id}][{parent_id}][{timestamp}]{title}.mp4"
            ));
            std::path::PathBuf::from(sanitized_filename)
        };

        let cover_filename = output_filename.with_extension("jpg");
        let output_dir = PathBuf::from(&self.config.read().await.output);
        tokio::fs::create_dir_all(&output_dir).await?;
        let output_path = output_dir.join(&output_filename);
        let output_stem = output_filename
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("whole-clip");
        let partial_path = output_dir.join(format!(".{output_stem}.partial.mp4"));
        // A prior Docker stop may have killed FFmpeg mid-write. The public
        // filename is committed only by rename after FFmpeg and metadata checks.
        match tokio::fs::remove_file(&partial_path).await {
            Ok(()) => log::warn!("Removed stale whole-session partial: {partial_path:?}"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let playlists_refs: Vec<&Path> = playlists.iter().map(|p| p.path.as_path()).collect();

        log::info!("Concat playlists: {playlists_refs:?}");
        log::info!("Output path: {output_path:?} (staging: {partial_path:?})");

        if let Err(e) = crate::ffmpeg::playlist::concat_playlists_to_video(
            reporter,
            &playlists_refs,
            danmu_ass_files,
            &partial_path,
        )
        .await
        {
            log::error!("Failed to concat playlists: {e}");
            let _ = tokio::fs::remove_file(&partial_path).await;
            return Err(RecorderManagerError::HLSError {
                err: format!("Failed to concat playlists: {e}"),
            });
        }

        let metadata = match std::fs::metadata(&partial_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                let _ = tokio::fs::remove_file(&partial_path).await;
                return Err(error.into());
            }
        };
        let size = match i64::try_from(metadata.len()) {
            Ok(size) => size,
            Err(error) => {
                let _ = tokio::fs::remove_file(&partial_path).await;
                return Err(RecorderManagerError::ClipError {
                    err: format!("Generated file is too large: {error}"),
                });
            }
        };

        if size == 0 {
            let _ = tokio::fs::remove_file(&partial_path).await;
            return Err(RecorderManagerError::ClipError {
                err: "Generated whole-session file is empty".to_string(),
            });
        }

        let video_metadata = match crate::ffmpeg::extract_video_metadata(&partial_path).await {
            Ok(metadata) => metadata,
            Err(error) => {
                let _ = tokio::fs::remove_file(&partial_path).await;
                return Err(RecorderManagerError::ClipError {
                    err: format!("Generated whole-session file failed validation: {error}"),
                });
            }
        };
        if !video_metadata.duration.is_finite() || video_metadata.duration <= 0.0 {
            let _ = tokio::fs::remove_file(&partial_path).await;
            return Err(RecorderManagerError::ClipError {
                err: format!(
                    "Generated whole-session file has an invalid duration: {}",
                    video_metadata.duration
                ),
            });
        }
        let length = video_metadata.duration.ceil() as i64;

        if let Err(error) = tokio::fs::rename(&partial_path, &output_path).await {
            #[cfg(not(unix))]
            if matches!(
                error.kind(),
                std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
            ) {
                tokio::fs::remove_file(&output_path).await?;
                tokio::fs::rename(&partial_path, &output_path).await?;
            } else {
                let _ = tokio::fs::remove_file(&partial_path).await;
                return Err(error.into());
            }
            #[cfg(unix)]
            {
                let _ = tokio::fs::remove_file(&partial_path).await;
                return Err(error.into());
            }
        }
        sync_directory(&output_dir).await?;

        let cover = if crate::ffmpeg::generate_thumbnail(&output_path, 0.0)
            .await
            .is_ok()
        {
            cover_filename.to_string_lossy().to_string()
        } else {
            let _ = tokio::fs::remove_file(output_path.with_extension("jpg")).await;
            String::new()
        };
        if crate::ffmpeg::extract_audio_sample(&output_path)
            .await
            .is_err()
        {
            let _ = tokio::fs::remove_file(output_path.with_extension("opus")).await;
        }

        let video_row = VideoRow {
            id: 0,
            status: 0,
            room_id: room_id.to_string(),
            created_at: chrono::Local::now().to_rfc3339(),
            cover,
            file: output_filename.to_string_lossy().to_string(),
            note: "".into(),
            length,
            size,
            bvid: String::new(),
            title: String::new(),
            desc: String::new(),
            tags: String::new(),
            area: 0,
            platform: platform.as_str().to_string(),
        };
        let video = match self.db.add_video(&video_row).await {
            Ok(video) => video,
            Err(error) => {
                let _ = tokio::fs::remove_file(&output_path).await;
                let _ = tokio::fs::remove_file(output_path.with_extension("jpg")).await;
                let _ = tokio::fs::remove_file(output_path.with_extension("opus")).await;
                return Err(error.into());
            }
        };

        let event =
            events::new_webhook_event(events::CLIP_GENERATED, events::Payload::Clip(video.clone()));
        if let Err(e) = self.webhook_poster.post_event(&event).await {
            log::error!("Post webhook event error: {e}");
        }

        if self.config.read().await.clip_notify {
            let body = format!(
                "生成了房间 {} 的整场切片: {}",
                room_id,
                output_filename.display()
            );
            #[cfg(feature = "gui")]
            if let Err(error) = self
                .app_handle
                .notification()
                .builder()
                .title("BiliShadowReplay - 整场切片完成")
                .body(body.clone())
                .show()
            {
                log::warn!("Failed to show whole clip notification: {error}");
            }
            #[cfg(feature = "headless")]
            self.emitter.emit(&RecorderEvent::UserNotification {
                title: "BiliShadowReplay - 整场切片完成".to_string(),
                body,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod live_end_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn database() -> Database {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE records (
                live_id TEXT PRIMARY KEY, platform TEXT, parent_id TEXT,
                room_id TEXT, title TEXT, length REAL, size INTEGER,
                created_at TEXT, cover TEXT
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        let db = Database::new();
        db.set(pool).await;
        db
    }

    fn ended_recorder(parent_id: &str, live_id: &str) -> RecorderInfo {
        RecorderInfo {
            platform_live_id: parent_id.into(),
            live_id: live_id.into(),
            room_info: RoomInfo::default(),
            user_info: UserInfo::default(),
            recording: false,
            enabled: true,
        }
    }

    async fn add_archive(db: &Database, platform: PlatformType, parent: &str, live: &str) {
        db.add_record(platform, parent, live, "10220184", "test", None)
            .await
            .unwrap();
        db.update_record_delta(live, 4.0, 1024).await.unwrap();
    }

    fn temp_cache_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "bili-shadowreplay-manager-test-{}",
            uuid::Uuid::new_v4()
        ))
    }

    #[test]
    fn whole_session_claim_is_scoped_to_room_and_session() {
        let mut claimed = HashSet::new();

        assert!(claim_douyin_whole_session(
            &mut claimed,
            "room-1",
            "session-1"
        ));
        assert!(!claim_douyin_whole_session(
            &mut claimed,
            "room-1",
            "session-1"
        ));
        assert!(claim_douyin_whole_session(
            &mut claimed,
            "room-1",
            "session-2"
        ));
        assert!(claim_douyin_whole_session(
            &mut claimed,
            "room-2",
            "session-1"
        ));
    }

    #[test]
    fn whole_session_retry_delay_backs_off_and_caps() {
        let delays = (1..=9)
            .map(|failure_count| douyin_whole_retry_delay(failure_count).as_secs())
            .collect::<Vec<_>>();

        assert_eq!(delays, vec![5, 10, 20, 40, 80, 160, 300, 300, 300]);
    }

    #[tokio::test]
    async fn live_end_acknowledgement_retries_without_repeating_completed_work() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_ack = attempts.clone();

        let failed_attempts = retry_until_douyin_live_end_acknowledged(
            move || {
                let attempts = attempts_for_ack.clone();
                async move { attempts.fetch_add(1, Ordering::SeqCst) >= 2 }
            },
            |_| Duration::ZERO,
        )
        .await;

        assert_eq!(failed_attempts, 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn zero_sized_douyin_playlist_is_repaired_without_deletion() {
        let db = database().await;
        let cache_dir = temp_cache_dir();
        let archive_dir = cache_dir
            .join(PlatformType::Douyin.as_str())
            .join("10220184")
            .join("segment-1");
        db.add_record(
            PlatformType::Douyin,
            "session-1",
            "segment-1",
            "10220184",
            "test",
            None,
        )
        .await
        .unwrap();
        tokio::fs::create_dir_all(&archive_dir).await.unwrap();
        tokio::fs::write(
            archive_dir.join("playlist.m3u8"),
            b"#EXTM3U\n#EXTINF:1,\nsegment-1.ts\n",
        )
        .await
        .unwrap();
        tokio::fs::write(archive_dir.join("segment-1.ts"), b"media")
            .await
            .unwrap();

        let repair = repair_douyin_zero_sized_archive(&db, &cache_dir, "10220184", "segment-1")
            .await
            .unwrap();

        assert_eq!(repair, DouyinZeroSizedArchiveRepair::VideoRepaired);
        assert!(db.get_record("10220184", "segment-1").await.unwrap().size > 0);
        assert!(archive_dir.join("playlist.m3u8").exists());
        assert!(
            douyin_session_has_cached_media(&db, &cache_dir, "10220184", "session-1")
                .await
                .unwrap()
        );

        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn header_only_douyin_playlist_is_not_promoted_to_video() {
        let cache_dir = temp_cache_dir();
        let archive_dir = cache_dir
            .join(PlatformType::Douyin.as_str())
            .join("10220184")
            .join("segment-empty");
        tokio::fs::create_dir_all(&archive_dir).await.unwrap();
        tokio::fs::write(
            archive_dir.join("playlist.m3u8"),
            b"#EXTM3U\n#EXT-X-PLAYLIST-TYPE:EVENT\n",
        )
        .await
        .unwrap();

        assert_eq!(
            inspect_douyin_archive(&archive_dir).await.unwrap(),
            DouyinArchiveDiskState::ValuableDataOnly
        );
        assert_eq!(douyin_archive_media_size(&archive_dir).await.unwrap(), None);

        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn events_only_douyin_archive_is_preserved_without_fake_video_size() {
        let db = database().await;
        let cache_dir = temp_cache_dir();
        let archive_dir = cache_dir
            .join(PlatformType::Douyin.as_str())
            .join("10220184")
            .join("segment-1");
        db.add_record(
            PlatformType::Douyin,
            "session-1",
            "segment-1",
            "10220184",
            "test",
            None,
        )
        .await
        .unwrap();
        tokio::fs::create_dir_all(&archive_dir).await.unwrap();
        tokio::fs::write(archive_dir.join(DOUYIN_SESSION_PARENT_FILE), b"session-1\n")
            .await
            .unwrap();
        tokio::fs::write(
            archive_dir.join("events.jsonl"),
            br#"{"method":"WebcastChatMessage","content":"kept"}\n"#,
        )
        .await
        .unwrap();

        let repair = repair_douyin_zero_sized_archive(&db, &cache_dir, "10220184", "segment-1")
            .await
            .unwrap();

        assert_eq!(repair, DouyinZeroSizedArchiveRepair::ValuableDataOnly);
        assert_eq!(
            db.get_record("10220184", "segment-1").await.unwrap().size,
            0
        );
        assert!(archive_dir.join("events.jsonl").exists());
        assert_eq!(douyin_archive_media_size(&archive_dir).await.unwrap(), None);
        assert!(
            !douyin_session_has_cached_media(&db, &cache_dir, "10220184", "session-1")
                .await
                .unwrap()
        );

        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn resolves_entire_session_without_a_last_archive() {
        let db = database().await;
        add_archive(&db, PlatformType::BiliBili, "session-1", "segment-1").await;
        add_archive(&db, PlatformType::BiliBili, "session-1", "segment-2").await;
        add_archive(&db, PlatformType::BiliBili, "old-session", "old-segment").await;

        // Covers both a reset current ID and a removed zero-byte final segment.
        for live_id in ["", "removed-empty-segment"] {
            let parent = live_end_parent_id(
                &db,
                PlatformType::BiliBili,
                "10220184",
                &ended_recorder("session-1", live_id),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(parent, "session-1");
            let archives = db
                .get_archives_by_parent_id("10220184", &parent)
                .await
                .unwrap();
            assert_eq!(archives.len(), 2);
            assert_eq!(archives[0].live_id, "segment-1");
            assert_eq!(archives[1].live_id, "segment-2");
            assert!(archives
                .iter()
                .all(|archive| archive.live_id.starts_with("segment-")));
        }
    }

    #[tokio::test]
    async fn skips_unrecorded_sessions_instead_of_using_another_live() {
        let db = database().await;
        add_archive(&db, PlatformType::BiliBili, "old-session", "old-segment").await;
        add_archive(&db, PlatformType::Douyin, "session-1", "other-platform").await;
        db.add_record(
            PlatformType::BiliBili,
            "session-1",
            "empty-segment",
            "10220184",
            "test",
            None,
        )
        .await
        .unwrap();

        for parent_id in ["", "session-1", "unrecorded-session"] {
            assert_eq!(
                live_end_parent_id(
                    &db,
                    PlatformType::BiliBili,
                    "10220184",
                    &ended_recorder(parent_id, ""),
                )
                .await
                .unwrap(),
                None
            );
        }
    }

    #[tokio::test]
    async fn supports_recorders_that_only_provide_an_archive_id() {
        let db = database().await;
        add_archive(&db, PlatformType::Douyin, "session-1", "segment-1").await;
        assert_eq!(
            live_end_parent_id(
                &db,
                PlatformType::Douyin,
                "10220184",
                &ended_recorder("", "segment-1"),
            )
            .await
            .unwrap(),
            Some("session-1".into())
        );
    }

    #[test]
    fn required_danmu_ass_failure_is_not_downgraded_to_none() {
        let live_ids = vec![
            "douyin-segment-1".to_string(),
            "douyin-segment-2".to_string(),
        ];
        let results = vec![
            Ok(PathBuf::from("segment-1.ass")),
            Err(RecorderManagerError::InvalidLiveID),
        ];

        let error = require_danmu_ass_files(&live_ids, results).unwrap_err();
        match error {
            RecorderManagerError::ArchiveDanmuAssGenerationFailed { error } => {
                assert!(error.contains("douyin-segment-2"));
                assert!(error.contains("Invalid live id"));
            }
            error => panic!("unexpected error: {error}"),
        }
    }
}
