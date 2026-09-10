pub mod api;
mod response;
pub mod stream_info;
use crate::account::Account;
use crate::core::hls_recorder::{construct_stream_from_variant, HlsRecorder};
use crate::core::{Codec, Format};
use crate::errors::RecorderError;
use crate::events::RecorderEvent;
use crate::platforms::douyin::stream_info::DouyinStream;
use crate::traits::RecorderTrait;
use crate::{Recorder, RoomInfo, UserInfo};
use async_trait::async_trait;
use chrono::Utc;
use danmu_stream::danmu_stream::DanmuStream;
use danmu_stream::provider::ProviderType;
use danmu_stream::{DanmuMessageType, LiveEvent};
use rand::random;
use std::collections::{HashSet, VecDeque};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{atomic, Arc};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::danmu::DanmuStorage;
use crate::platforms::PlatformType;

pub type DouyinRecorder = Recorder<DouyinExtra>;

#[derive(Clone)]
pub struct DouyinExtra {
    sec_user_id: String,
    stream_url: Arc<RwLock<Option<String>>>,
    danmu_stream: Arc<Mutex<Option<DanmuStream>>>,
    danmu_shutdown: Arc<Mutex<()>>,
    pending_live_ends: Arc<Mutex<VecDeque<PendingLiveEnd>>>,
    realtime_event_sink: Arc<dyn Fn(RecorderEvent) + Send + Sync>,
    danmu_persist_degraded: Arc<atomic::AtomicBool>,
    active_session_persist_degraded: Arc<atomic::AtomicBool>,
    pending_live_end_persist_degraded: Arc<atomic::AtomicBool>,
    recent_event_ids: Arc<Mutex<EventDeduplicator>>,
    pending_event_ids: Arc<Mutex<Vec<PendingEventId>>>,
    danmu_storage_generation: Arc<atomic::AtomicU64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingEventId {
    storage_generation: u64,
    key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingLiveEnd {
    session_id: String,
    marker_persisted: bool,
}

const DOUYIN_DEDUP_CAPACITY: usize = 8192;
const DOUYIN_ACTIVE_SESSION_FILE: &str = ".active-session";
const DOUYIN_PENDING_LIVE_ENDS_DIR: &str = ".pending-live-ends";
pub const DOUYIN_SESSION_PARENT_FILE: &str = ".session-parent";
const DOUYIN_DANMU_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const DOUYIN_LIVE_END_RETRY_BATCH: usize = 8;
static DOUYIN_SESSION_MARKER_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(Debug, PartialEq, Eq)]
struct SessionTransition {
    end_previous: bool,
    start_current: bool,
}

fn session_transition(
    was_live: bool,
    active_session_id: &str,
    is_live: bool,
    current_session_id: &str,
) -> SessionTransition {
    let session_replaced =
        is_live && !active_session_id.is_empty() && active_session_id != current_session_id;
    SessionTransition {
        end_previous: (was_live && !is_live)
            || (!was_live && !active_session_id.is_empty() && !is_live)
            || session_replaced,
        start_current: is_live && (!was_live || session_replaced),
    }
}

#[derive(Default)]
struct EventDeduplicator {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl EventDeduplicator {
    fn accept(&mut self, key: String) -> bool {
        if self.seen.contains(&key) {
            return false;
        }
        self.seen.insert(key.clone());
        self.order.push_back(key);
        if self.order.len() > DOUYIN_DEDUP_CAPACITY {
            if let Some(expired) = self.order.pop_front() {
                self.seen.remove(&expired);
            }
        }
        true
    }

    fn clear(&mut self) {
        self.seen.clear();
        self.order.clear();
    }
}

fn get_best_stream_url(stream: &DouyinStream) -> Option<String> {
    // find the best stream url
    if stream.data.origin.main.hls.is_empty() {
        log::error!("No stream url found in stream_data: {stream:#?}");
        return None;
    }

    Some(stream.data.origin.main.hls.clone())
}

fn select_stream_url(hls_url: &str, stream_data: &str) -> Option<String> {
    serde_json::from_str::<DouyinStream>(stream_data)
        .ok()
        .and_then(|stream| get_best_stream_url(&stream))
        .or_else(|| {
            let hls_url = hls_url.trim();
            (!hls_url.is_empty()).then(|| hls_url.to_string())
        })
}

fn active_session_path(cache_dir: &Path, room_id: &str) -> PathBuf {
    cache_dir
        .join(PlatformType::Douyin.as_str())
        .join(room_id)
        .join(DOUYIN_ACTIVE_SESSION_FILE)
}

fn pending_live_ends_path(cache_dir: &Path, room_id: &str) -> PathBuf {
    cache_dir
        .join(PlatformType::Douyin.as_str())
        .join(room_id)
        .join(DOUYIN_PENDING_LIVE_ENDS_DIR)
}

fn encode_session_marker_name(session_id: &str) -> String {
    let mut encoded = String::with_capacity(session_id.len() * 2);
    for byte in session_id.as_bytes() {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn decode_session_marker_name(encoded: &str) -> Option<String> {
    if encoded.is_empty() || !encoded.len().is_multiple_of(2) {
        return None;
    }
    let encoded = encoded.as_bytes();
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for offset in (0..encoded.len()).step_by(2) {
        let pair = std::str::from_utf8(&encoded[offset..offset + 2]).ok()?;
        bytes.push(u8::from_str_radix(pair, 16).ok()?);
    }
    String::from_utf8(bytes)
        .ok()
        .filter(|value| !value.is_empty())
}

async fn load_pending_live_ends(cache_dir: &Path, room_id: &str) -> VecDeque<String> {
    let _marker_guard = DOUYIN_SESSION_MARKER_LOCK.lock().await;
    let path = pending_live_ends_path(cache_dir, room_id);
    let mut entries = match tokio::fs::read_dir(&path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return VecDeque::new(),
        Err(error) => {
            log::warn!("Failed to read pending Douyin LiveEnd markers {path:?}: {error}");
            return VecDeque::new();
        }
    };
    let mut sessions = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Some(session_id) = entry
            .file_name()
            .to_str()
            .and_then(decode_session_marker_name)
        {
            sessions.push(session_id);
        }
    }
    if sessions
        .iter()
        .all(|session| session.parse::<u128>().is_ok())
    {
        sessions.sort_by_key(|session| session.parse::<u128>().unwrap_or_default());
    } else {
        sessions.sort();
    }
    sessions.dedup();
    sessions.into()
}

async fn load_active_session(cache_dir: &Path, room_id: &str) -> String {
    let _marker_guard = DOUYIN_SESSION_MARKER_LOCK.lock().await;
    let path = active_session_path(cache_dir, room_id);
    match tokio::fs::read_to_string(&path).await {
        Ok(session_id) => {
            let session_id = session_id.trim().to_string();
            if session_id.is_empty() {
                log::warn!("Ignoring empty Douyin active-session marker: {path:?}");
            } else {
                log::info!(
                    "Recovered interrupted Douyin live session {} for room {}",
                    session_id,
                    room_id
                );
            }
            session_id
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => {
            log::warn!("Failed to read Douyin active-session marker {path:?}: {error}");
            String::new()
        }
    }
}

async fn clear_active_session_if_matches(
    cache_dir: &Path,
    room_id: &str,
    session_id: &str,
) -> std::io::Result<bool> {
    let _marker_guard = DOUYIN_SESSION_MARKER_LOCK.lock().await;
    let path = active_session_path(cache_dir, room_id);
    let current = match tokio::fs::read_to_string(&path).await {
        Ok(current) => current,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if current.trim() != session_id {
        return Ok(false);
    }
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

async fn pending_live_end_marker_exists(
    cache_dir: &Path,
    room_id: &str,
    session_id: &str,
) -> std::io::Result<bool> {
    let _marker_guard = DOUYIN_SESSION_MARKER_LOCK.lock().await;
    tokio::fs::try_exists(
        pending_live_ends_path(cache_dir, room_id).join(encode_session_marker_name(session_id)),
    )
    .await
}

/// Best-effort parent for archives created by versions which did not persist a
/// per-attempt `.session-parent`. It is safe only when exactly one active or
/// pending session exists for the room.
pub async fn recovery_session_parent_fallback(cache_dir: &Path, room_id: &str) -> Option<String> {
    let active = load_active_session(cache_dir, room_id).await;
    let pending = load_pending_live_ends(cache_dir, room_id).await;
    let mut sessions = pending.into_iter().collect::<Vec<_>>();
    if !active.is_empty() && !sessions.contains(&active) {
        sessions.push(active);
    }
    (sessions.len() == 1).then(|| sessions.remove(0))
}

async fn persist_pending_live_end(
    cache_dir: &Path,
    room_id: &str,
    session_id: &str,
) -> std::io::Result<()> {
    if session_id.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cannot persist an empty Douyin session id",
        ));
    }
    let _marker_guard = DOUYIN_SESSION_MARKER_LOCK.lock().await;
    let pending_dir = pending_live_ends_path(cache_dir, room_id);
    tokio::fs::create_dir_all(&pending_dir).await?;
    let pending_path = pending_dir.join(encode_session_marker_name(session_id));
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&pending_path)
        .await?;
    file.sync_all().await?;
    Ok(())
}

/// Remove only the ended incarnation's pending marker after whole-session
/// generation succeeds. An active marker with the same platform session ID may
/// already belong to a newer live incarnation and must never be removed here.
pub async fn acknowledge_live_end(cache_dir: &Path, room_id: &str, session_id: &str) -> bool {
    if session_id.is_empty() {
        return false;
    }
    let _marker_guard = DOUYIN_SESSION_MARKER_LOCK.lock().await;
    let pending_path =
        pending_live_ends_path(cache_dir, room_id).join(encode_session_marker_name(session_id));
    match tokio::fs::remove_file(&pending_path).await {
        Ok(()) => true,
        // ACK is idempotent. Missing means this exact pending marker was already
        // cleared; it must not cause a completed manager job to look failed.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            log::warn!("Failed to remove pending Douyin LiveEnd {pending_path:?}: {error}");
            false
        }
    }
}

impl DouyinRecorder {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        room_id: &str,
        sec_user_id: &str,
        account: &Account,
        cache_dir: PathBuf,
        channel: broadcast::Sender<RecorderEvent>,
        realtime_event_sink: Arc<dyn Fn(RecorderEvent) + Send + Sync>,
        update_interval: Arc<atomic::AtomicU64>,
        enabled: bool,
    ) -> Result<Self, crate::errors::RecorderError> {
        let mut recovered_session_id = load_active_session(&cache_dir, room_id).await;
        let pending_session_ids = load_pending_live_ends(&cache_dir, room_id).await;
        // A crash can occur after the pending marker is created but before the
        // old active marker is removed. Clear it before accepting a new live:
        // Douyin can reuse the same platform ID, and a later ACK for the old
        // pending marker must not erase that new incarnation's active marker.
        if !recovered_session_id.is_empty() && pending_session_ids.contains(&recovered_session_id) {
            match clear_active_session_if_matches(&cache_dir, room_id, &recovered_session_id).await
            {
                Ok(true) => recovered_session_id.clear(),
                Ok(false) => {
                    // The file changed between the initial read and cleanup.
                    // Preserve the newer value instead of clearing it blindly.
                    recovered_session_id = load_active_session(&cache_dir, room_id).await;
                }
                Err(error) => {
                    let message = format!(
                        "无法清理直播间 {room_id} 的旧抖音会话标记，已暂停该录制器初始化：{error}"
                    );
                    log::error!("{message}");
                    let _ = channel.send(RecorderEvent::UserNotification {
                        title: "抖音会话恢复失败".to_string(),
                        body: message,
                    });
                    return Err(RecorderError::IoError(error));
                }
            }
        }
        let pending_live_ends = pending_session_ids
            .into_iter()
            .map(|session_id| PendingLiveEnd {
                session_id,
                marker_persisted: true,
            })
            .collect();
        Ok(Self {
            platform: PlatformType::Douyin,
            room_id: room_id.to_string(),
            account: account.clone(),
            client: reqwest::Client::new(),
            event_channel: channel,
            cache_dir,
            quit: Arc::new(atomic::AtomicBool::new(false)),
            enabled: Arc::new(atomic::AtomicBool::new(enabled)),
            is_recording: Arc::new(atomic::AtomicBool::new(false)),
            room_info: Arc::new(RwLock::new(RoomInfo::default())),
            user_info: Arc::new(RwLock::new(UserInfo::default())),
            platform_live_id: Arc::new(RwLock::new(recovered_session_id)),
            live_id: Arc::new(RwLock::new(String::new())),
            danmu_storage: Arc::new(RwLock::new(None)),
            last_update: Arc::new(atomic::AtomicI64::new(Utc::now().timestamp())),
            last_sequence: Arc::new(atomic::AtomicU64::new(0)),
            danmu_task: Arc::new(Mutex::new(None)),
            record_task: Arc::new(Mutex::new(None)),
            update_interval,
            extra: DouyinExtra {
                sec_user_id: sec_user_id.to_string(),
                stream_url: Arc::new(RwLock::new(None)),
                danmu_stream: Arc::new(Mutex::new(None)),
                danmu_shutdown: Arc::new(Mutex::new(())),
                pending_live_ends: Arc::new(Mutex::new(pending_live_ends)),
                realtime_event_sink,
                danmu_persist_degraded: Arc::new(atomic::AtomicBool::new(false)),
                active_session_persist_degraded: Arc::new(atomic::AtomicBool::new(false)),
                pending_live_end_persist_degraded: Arc::new(atomic::AtomicBool::new(false)),
                recent_event_ids: Arc::new(Mutex::new(EventDeduplicator::default())),
                pending_event_ids: Arc::new(Mutex::new(Vec::new())),
                danmu_storage_generation: Arc::new(atomic::AtomicU64::new(0)),
            },
        })
    }

    async fn check_status(&self) -> bool {
        self.emit_recovered_pending_live_ends().await;
        let previous_room_info = self.room_info.read().await.clone();
        let previous_user_info = self.user_info.read().await.clone();
        let pre_live_status = previous_room_info.status;
        match api::get_room_info(
            &self.client,
            &self.account,
            &self.room_id,
            &self.extra.sec_user_id,
        )
        .await
        {
            Ok(info) => {
                let live_status = info.status == 0; // room_status == 0 表示正在直播
                if live_status && info.room_id_str.trim().is_empty() {
                    log::error!(
                        "[{}] Douyin reported a live room without a webcast room id",
                        self.room_id
                    );
                    return false;
                }

                *self.room_info.write().await = RoomInfo {
                    platform: PlatformType::Douyin.as_str().to_string(),
                    room_id: self.room_id.to_string(),
                    room_title: if info.room_title.is_empty() {
                        previous_room_info.room_title
                    } else {
                        info.room_title.clone()
                    },
                    room_cover: info.cover.clone().unwrap_or(previous_room_info.room_cover),
                    status: live_status,
                };

                *self.user_info.write().await = UserInfo {
                    user_id: if info.sec_user_id.is_empty() {
                        previous_user_info.user_id
                    } else {
                        info.sec_user_id.clone()
                    },
                    user_name: if info.user_name.is_empty() {
                        previous_user_info.user_name
                    } else {
                        info.user_name.clone()
                    },
                    user_avatar: if info.user_avatar.is_empty() {
                        previous_user_info.user_avatar
                    } else {
                        info.user_avatar.clone()
                    },
                };

                // Docker can stop while a live is still running. The marker
                // lets the next container either resume the same Douyin live,
                // or finish the previous session if it ended while offline.
                let recovered_session_id = self.platform_live_id.read().await.clone();
                let transition = session_transition(
                    pre_live_status,
                    &recovered_session_id,
                    live_status,
                    &info.room_id_str,
                );
                if transition.end_previous {
                    log::info!(
                        "Finishing recovered Douyin session {} for room {}",
                        recovered_session_id,
                        self.room_id
                    );
                    self.emit_live_end_and_reset().await;
                }

                if transition.start_current {
                    log::info!(
                        "[{}]Douyin live session started, auto_start: {}",
                        self.room_id,
                        self.enabled.load(atomic::Ordering::Relaxed)
                    );
                    self.reset_live().await;
                    let _ = self.event_channel.send(RecorderEvent::LiveStart {
                        recorder: self.info().await,
                    });
                }

                if live_status {
                    let session_changed =
                        self.platform_live_id.read().await.as_str() != info.room_id_str.as_str();
                    if session_changed {
                        (*self.platform_live_id.write().await).clone_from(&info.room_id_str);
                    }
                    // Verify the durable marker on every live poll, not only on
                    // the in-memory transition. A transient bind-mount failure
                    // is therefore retried without restarting the container.
                    match self.persist_active_session(&info.room_id_str).await {
                        Ok(()) => self
                            .extra
                            .active_session_persist_degraded
                            .store(false, atomic::Ordering::Relaxed),
                        Err(error) => {
                            log::error!(
                                "[{}] Failed to persist Douyin active session {}: {error}",
                                self.room_id,
                                info.room_id_str
                            );
                            if !self
                                .extra
                                .active_session_persist_degraded
                                .swap(true, atomic::Ordering::Relaxed)
                            {
                                let _ = self.event_channel.send(
                                    RecorderEvent::UserNotification {
                                        title: "抖音会话恢复标记保存失败".to_string(),
                                        body: format!(
                                            "直播间 {} 的恢复标记暂时无法写入，程序会在下轮自动重试：{}",
                                            self.room_id, error
                                        ),
                                    },
                                );
                            }
                        }
                    }
                }

                if !live_status {
                    return false;
                }

                let should_record = self.should_record().await;

                if !should_record {
                    return true;
                }

                let Some(new_stream_url) = select_stream_url(&info.hls_url, &info.stream_data)
                else {
                    log::error!("No usable douyin stream URL found: {info:#?}");
                    return false;
                };

                // Store the selected URL only after the live status and account
                // response are valid. The direct hls_pull_url is a supported
                // fallback when Douyin changes or omits stream_data.
                log::info!("New douyin stream URL: {new_stream_url}");
                *self.extra.stream_url.write().await = Some(new_stream_url);

                true
            }
            Err(e) => {
                log::warn!("[{}]Update room status failed: {}", self.room_id, e);
                pre_live_status
            }
        }
    }

    async fn emit_recovered_pending_live_ends(&self) {
        // Retry a bounded batch per status poll. Successful broadcast delivery
        // is not an acknowledgement (a receiver can lag after send succeeds),
        // so each entry stays in the rotating queue until its durable marker is
        // removed by `acknowledge_live_end`.
        let batch_size = self
            .extra
            .pending_live_ends
            .lock()
            .await
            .len()
            .min(DOUYIN_LIVE_END_RETRY_BATCH);
        for _ in 0..batch_size {
            let Some(mut pending) = self.extra.pending_live_ends.lock().await.pop_front() else {
                break;
            };

            if pending.marker_persisted {
                match pending_live_end_marker_exists(
                    &self.cache_dir,
                    &self.room_id,
                    &pending.session_id,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        // The manager completed generation and acknowledged this
                        // exact pending marker. It can now leave the retry queue.
                        continue;
                    }
                    Err(error) => log::warn!(
                        "Failed to inspect pending Douyin LiveEnd {}: {error}",
                        pending.session_id
                    ),
                }
            } else {
                match persist_pending_live_end(&self.cache_dir, &self.room_id, &pending.session_id)
                    .await
                {
                    Ok(()) => {
                        pending.marker_persisted = true;
                        self.extra
                            .pending_live_end_persist_degraded
                            .store(false, atomic::Ordering::Relaxed);
                        let same_id_is_live = self.room_info.read().await.status
                            && self.platform_live_id.read().await.as_str()
                                == pending.session_id.as_str();
                        if !same_id_is_live {
                            if let Err(error) = clear_active_session_if_matches(
                                &self.cache_dir,
                                &self.room_id,
                                &pending.session_id,
                            )
                            .await
                            {
                                log::warn!(
                                    "Failed to clear ended Douyin active session {} during retry: {error}",
                                    pending.session_id
                                );
                            }
                        }
                    }
                    Err(error) => {
                        log::error!(
                            "Failed to persist pending Douyin LiveEnd {}: {error}",
                            pending.session_id
                        );
                        if !self
                            .extra
                            .pending_live_end_persist_degraded
                            .swap(true, atomic::Ordering::Relaxed)
                        {
                            let _ = self.event_channel.send(RecorderEvent::UserNotification {
                                title: "抖音下播恢复标记保存失败".to_string(),
                                body: format!(
                                    "直播间 {} 的下播恢复标记暂时无法写入，程序会持续重试：{}",
                                    self.room_id, error
                                ),
                            });
                        }
                    }
                }
            }

            if !pending.marker_persisted {
                // Without a durable marker the manager cannot acknowledge this
                // delivery. Keep retrying persistence first; otherwise a fast
                // successful generation followed by a late marker write would
                // leave that marker orphaned forever.
                self.extra.pending_live_ends.lock().await.push_back(pending);
                continue;
            }

            let session_id = pending.session_id.clone();
            let mut recorder = self.info().await;
            recorder.platform_live_id.clone_from(&session_id);
            recorder.live_id.clear();
            recorder.recording = false;
            recorder.room_info.status = false;
            if self
                .event_channel
                .send(RecorderEvent::LiveEnd {
                    platform: PlatformType::Douyin,
                    room_id: self.room_id.clone(),
                    recorder,
                })
                .is_err()
            {
                log::warn!(
                    "Could not deliver recovered Douyin LiveEnd {session_id}; retrying next poll"
                );
            }
            self.extra.pending_live_ends.lock().await.push_back(pending);
        }
    }

    async fn danmu(&self) -> Result<(), crate::errors::RecorderError> {
        let cookies = self.account.cookies.clone();
        let platform_live_id = self.platform_live_id.read().await.clone();
        let danmu_room_id = douyin_danmu_room_id(&platform_live_id)?;
        let danmu_stream = DanmuStream::new(ProviderType::Douyin, &cookies, &danmu_room_id)
            .await
            .map_err(|error| {
                log::error!("Failed to create danmu stream: {error}");
                crate::errors::RecorderError::DanmuStreamError(error)
            })?;
        *self.extra.danmu_stream.lock().await = Some(danmu_stream.clone());

        // Drive websocket I/O and disk persistence in separate tasks. A slow
        // Docker bind mount can then apply backpressure without preventing the
        // provider from sending heartbeat/Pong frames while it waits for the
        // durability barrier that precedes each server ACK.
        let (close_consumer_tx, mut close_consumer_rx) = tokio::sync::oneshot::channel();
        let consumer_stream = danmu_stream.clone();
        let consumer_recorder = self.clone();
        let mut consumers = tokio::task::JoinSet::new();
        consumers.spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut close_consumer_rx => break,
                    recv_res = consumer_stream.recv() => {
                        match recv_res? {
                            Some(message) => consumer_recorder.process_danmu_message(message).await,
                            None => break,
                        }
                    }
                }
            }

            // Stop accepting new provider messages, then persist every message
            // which was already accepted before shutdown.
            consumer_stream.close_receiver().await;
            while let Some(message) = consumer_stream.recv().await? {
                consumer_recorder.process_danmu_message(message).await;
            }
            Ok::<(), crate::errors::RecorderError>(())
        });

        let mut start_fut = Box::pin(danmu_stream.start());
        let start_result = tokio::select! {
            start_result = &mut start_fut => start_result,
            consumer_result = consumers.join_next() => {
                let error = match consumer_result {
                    Some(Ok(Err(error))) => error,
                    Some(Err(error)) => RecorderError::ApiError {
                        error: format!("Douyin danmu persistence task failed: {error}"),
                    },
                    Some(Ok(Ok(()))) | None => RecorderError::ApiError {
                        error: "Douyin danmu persistence task ended unexpectedly".to_string(),
                    },
                };
                let _ = danmu_stream.stop().await;
                drop(start_fut);
                *self.extra.danmu_stream.lock().await = None;
                return Err(error);
            }
        };
        drop(start_fut);
        let _ = close_consumer_tx.send(());
        match consumers.join_next().await {
            Some(Ok(result)) => result?,
            Some(Err(error)) => {
                return Err(RecorderError::ApiError {
                    error: format!("Douyin danmu persistence task failed: {error}"),
                });
            }
            None => {}
        }
        *self.extra.danmu_stream.lock().await = None;

        start_result.map_err(|error| {
            log::error!("Danmu stream start error: {error}");
            crate::errors::RecorderError::DanmuStreamError(error)
        })
    }

    async fn process_danmu_message(&self, message: DanmuMessageType) {
        match message {
            DanmuMessageType::Event(event) => {
                let dedup_key = Self::event_dedup_key(&event);
                if let Some(key) = &dedup_key {
                    let was_persisted = self.extra.recent_event_ids.lock().await.seen.contains(key);
                    let is_pending = self
                        .extra
                        .pending_event_ids
                        .lock()
                        .await
                        .iter()
                        .any(|pending| pending.key == *key);
                    if was_persisted || is_pending {
                        log::debug!("Discard duplicate douyin event");
                        return;
                    }
                }
                if event.event_type == "danmu" {
                    let content = event
                        .data
                        .get("content")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string();
                    (self.extra.realtime_event_sink)(RecorderEvent::DanmuReceived {
                        room: self.room_id.clone(),
                        ts: event.ts,
                        content,
                    });
                }
                self.persist_event(&event, dedup_key).await;
            }
            DanmuMessageType::DanmuMessage(danmu) => {
                let ts = Utc::now().timestamp_millis();
                (self.extra.realtime_event_sink)(RecorderEvent::DanmuReceived {
                    room: self.room_id.clone(),
                    ts,
                    content: danmu.message.clone(),
                });

                let event = LiveEvent::danmu(danmu, "douyin");
                self.persist_event(&event, None).await;
            }
            DanmuMessageType::PersistBarrier(reply) => {
                // Hold the storage read lock through both flush and pending-ID
                // settlement. An HLS attempt switch must not make this barrier
                // flush a different file from the one its events were written to.
                let storage = self.danmu_storage.read().await;
                let generation = self
                    .extra
                    .danmu_storage_generation
                    .load(atomic::Ordering::Relaxed);
                let result = match storage.as_ref() {
                    Some(storage) => storage.flush().await.map_err(|error| error.to_string()),
                    None => Err("Douyin danmu storage is unavailable".to_string()),
                };
                // A failed barrier must release the IDs for replay. The original
                // bytes may already be present, so this deliberately prefers a
                // possible duplicate over silently losing a complete event.
                self.settle_pending_event_ids(generation, result.is_ok())
                    .await;
                drop(storage);
                let _ = reply.send(result);
            }
        }
    }

    async fn settle_pending_event_ids(&self, generation: u64, durable: bool) {
        let event_ids = {
            let mut pending = self.extra.pending_event_ids.lock().await;
            let mut retained = Vec::with_capacity(pending.len());
            let mut settled = Vec::new();
            for event in std::mem::take(&mut *pending) {
                if event.storage_generation == generation {
                    settled.push(event.key);
                } else {
                    retained.push(event);
                }
            }
            *pending = retained;
            settled
        };

        if durable {
            let mut recent_event_ids = self.extra.recent_event_ids.lock().await;
            for event_id in event_ids {
                recent_event_ids.accept(event_id);
            }
        }
    }

    async fn replace_danmu_storage(&self, replacement: DanmuStorage) -> std::io::Result<u64> {
        let mut storage = self.danmu_storage.write().await;
        let previous_generation = self
            .extra
            .danmu_storage_generation
            .load(atomic::Ordering::Relaxed);

        if let Some(previous) = storage.as_ref() {
            // Seal the previous attempt before switching the shared slot. All
            // event writes register their pending key while holding a read lock
            // on this slot, so a successful sync covers the complete generation.
            previous.sync().await?;
            self.settle_pending_event_ids(previous_generation, true)
                .await;
        }

        let generation = previous_generation.saturating_add(1);
        self.extra
            .danmu_storage_generation
            .store(generation, atomic::Ordering::Relaxed);
        *storage = Some(replacement);
        Ok(generation)
    }

    async fn persist_event(&self, event: &LiveEvent, dedup_key: Option<String>) -> bool {
        let mut attempt = 0_u32;
        loop {
            attempt = attempt.saturating_add(1);
            let result = {
                let storage = self.danmu_storage.read().await;
                let Some(storage) = storage.as_ref() else {
                    return false;
                };
                let generation = self
                    .extra
                    .danmu_storage_generation
                    .load(atomic::Ordering::Relaxed);
                let result = storage.add_event(event).await;
                if result.is_ok() {
                    if let Some(key) = dedup_key.as_ref() {
                        // Register before releasing the storage read lock so an
                        // attempt switch cannot seal the file and miss this ID.
                        self.extra
                            .pending_event_ids
                            .lock()
                            .await
                            .push(PendingEventId {
                                storage_generation: generation,
                                key: key.clone(),
                            });
                    }
                }
                result
            };

            match result {
                Ok(()) => {
                    self.extra
                        .danmu_persist_degraded
                        .store(false, atomic::Ordering::Relaxed);
                    return true;
                }
                Err(error) => {
                    let error = error.to_string();
                    if attempt == 1 || attempt.is_multiple_of(12) {
                        log::error!(
                            "[{}] Failed to persist Douyin live event (attempt {attempt}): {error}",
                            self.room_id
                        );
                    }
                    if !self
                        .extra
                        .danmu_persist_degraded
                        .swap(true, atomic::Ordering::Relaxed)
                    {
                        let _ = self.event_channel.send(RecorderEvent::UserNotification {
                            title: "抖音弹幕保存异常".to_string(),
                            body: format!(
                                "直播间 {} 的完整弹幕暂时无法写入磁盘，程序会持续重试；请检查 Docker 挂载目录空间和权限：{}",
                                self.room_id, error
                            ),
                        });
                    }
                    // Keep the event pending instead of acknowledging it to
                    // the recorder and silently dropping metadata. The bounded
                    // provider channel applies backpressure while its separate
                    // heartbeat path keeps the Douyin connection alive.
                    let delay_ms = 100_u64.saturating_mul(1_u64 << attempt.min(5));
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }

    fn event_dedup_key(event: &LiveEvent) -> Option<String> {
        let id = event
            .raw
            .get("id")
            .or_else(|| event.data.get("id"))
            .and_then(|value| value.as_str())
            .filter(|id| !id.is_empty() && *id != "0");
        let id = id?;
        let method = event
            .raw
            .get("method")
            .or_else(|| event.data.get("method"))
            .and_then(|value| value.as_str())
            .unwrap_or(&event.event_type);
        Some(format!("{method}:{id}"))
    }

    async fn reset_recording(&self) {
        self.last_update
            .store(Utc::now().timestamp(), atomic::Ordering::Relaxed);
        self.last_sequence.store(0, atomic::Ordering::Relaxed);
        *self.extra.stream_url.write().await = None;
        *self.live_id.write().await = String::new();
    }

    async fn stop_danmu(&self) {
        let _shutdown_guard = self.extra.danmu_shutdown.lock().await;
        self.stop_danmu_locked().await;
    }

    async fn stop_danmu_locked(&self) {
        let deadline = tokio::time::Instant::now() + DOUYIN_DANMU_SHUTDOWN_TIMEOUT;
        let active_stream = self.extra.danmu_stream.lock().await.clone();
        let can_wait_for_drain = if let Some(stream) = &active_stream {
            match tokio::time::timeout_at(deadline, stream.stop()).await {
                Ok(Ok(())) => true,
                Ok(Err(error)) => {
                    log::warn!("Failed to stop Douyin danmu stream: {error}");
                    // stop() sets the stop flag and detaches the writer before
                    // attempting the close handshake. Its task can normally
                    // still finish and drain the recorder queue after an error.
                    true
                }
                Err(_) => {
                    log::warn!("Timed out while closing the Douyin websocket; aborting its task");
                    false
                }
            }
        } else {
            false
        };

        if let Some(mut task) = self.danmu_task.lock().await.take() {
            if !can_wait_for_drain {
                task.abort();
                let _ = task.await;
            } else if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
                log::warn!(
                    "Douyin danmu drain timed out after {} seconds; aborting the stalled task",
                    DOUYIN_DANMU_SHUTDOWN_TIMEOUT.as_secs()
                );
                task.abort();
                let _ = task.await;
            }
        }

        *self.extra.danmu_stream.lock().await = None;
        if let Some(storage) = self.danmu_storage.read().await.as_ref() {
            let finalize = async {
                storage.repair_tail().await?;
                storage.sync().await
            };
            match tokio::time::timeout_at(deadline, finalize).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    log::error!("Failed to flush the final Douyin danmu records: {error}")
                }
                Err(_) => log::error!(
                    "Timed out flushing final Douyin danmu records within {} seconds",
                    DOUYIN_DANMU_SHUTDOWN_TIMEOUT.as_secs()
                ),
            }
        }
        *self.danmu_storage.write().await = None;
    }

    async fn reset_live(&self) {
        self.stop_danmu().await;
        self.reset_recording().await;
        *self.platform_live_id.write().await = String::new();
        self.extra.recent_event_ids.lock().await.clear();
        self.extra.pending_event_ids.lock().await.clear();
    }

    async fn persist_active_session(&self, session_id: &str) -> std::io::Result<()> {
        if session_id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cannot persist an empty Douyin active session id",
            ));
        }
        let _marker_guard = DOUYIN_SESSION_MARKER_LOCK.lock().await;
        let path = active_session_path(&self.cache_dir, &self.room_id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::read_to_string(&path).await {
            Ok(current) if current.trim() == session_id => return Ok(()),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let temporary_path = path.with_file_name(".active-session.tmp");
        let write_result = async {
            let mut file = tokio::fs::File::create(&temporary_path).await?;
            file.write_all(session_id.as_bytes()).await?;
            file.flush().await?;
            file.sync_data().await?;
            drop(file);
            match tokio::fs::rename(&temporary_path, &path).await {
                Ok(()) => Ok(()),
                // Windows does not replace an existing destination. Docker's
                // Linux rename is atomic; this fallback only affects desktop.
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
                    ) =>
                {
                    match tokio::fs::remove_file(&path).await {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error),
                    }
                    tokio::fs::rename(&temporary_path, &path).await
                }
                Err(error) => Err(error),
            }
        }
        .await;
        if write_result.is_err() {
            let _ = tokio::fs::remove_file(&temporary_path).await;
        }
        write_result
    }

    async fn emit_live_end_and_reset(&self) {
        let mut recorder = self.info().await;
        recorder.room_info.status = false;
        let ended_session_id = recorder.platform_live_id.clone();
        let marker_persisted = match persist_pending_live_end(
            &self.cache_dir,
            &self.room_id,
            &ended_session_id,
        )
        .await
        {
            Ok(()) => {
                self.extra
                    .pending_live_end_persist_degraded
                    .store(false, atomic::Ordering::Relaxed);
                // This path runs only after a confirmed end transition, so
                // the matching active marker is the ended incarnation.
                if let Err(error) = clear_active_session_if_matches(
                    &self.cache_dir,
                    &self.room_id,
                    &ended_session_id,
                )
                .await
                {
                    log::warn!(
                        "Failed to clear ended Douyin active session {ended_session_id}: {error}"
                    );
                }
                true
            }
            Err(error) => {
                log::error!(
                        "Failed to persist pending Douyin LiveEnd {ended_session_id}: {error}; keeping it in the retry queue"
                    );
                if !self
                    .extra
                    .pending_live_end_persist_degraded
                    .swap(true, atomic::Ordering::Relaxed)
                {
                    let _ = self.event_channel.send(RecorderEvent::UserNotification {
                        title: "抖音下播恢复标记保存失败".to_string(),
                        body: format!(
                            "直播间 {} 的下播恢复标记暂时无法写入，程序会持续重试：{}",
                            self.room_id, error
                        ),
                    });
                }
                false
            }
        };
        {
            // Queue before the potentially slow danmu drain. Broadcast send
            // success is intentionally irrelevant: only marker ACK removes it.
            let mut pending = self.extra.pending_live_ends.lock().await;
            if let Some(existing) = pending
                .iter_mut()
                .find(|pending| pending.session_id == ended_session_id)
            {
                existing.marker_persisted |= marker_persisted;
            } else {
                pending.push_back(PendingLiveEnd {
                    session_id: ended_session_id.clone(),
                    marker_persisted,
                });
            }
        }
        // Stop the provider and persist every queued event before notifying the
        // manager. Whole-session generation can now safely read closed files.
        self.reset_live().await;
        if marker_persisted {
            let sent = self.event_channel.send(RecorderEvent::LiveEnd {
                platform: PlatformType::Douyin,
                room_id: self.room_id.clone(),
                recorder,
            });
            if sent.is_err() {
                log::warn!(
                    "Could not deliver Douyin LiveEnd {ended_session_id}; retrying next poll"
                );
            }
        } else {
            log::warn!(
                "Deferring Douyin LiveEnd {ended_session_id} until its pending marker is durable"
            );
        }
    }

    async fn update_entries(&self, live_id: &str) -> Result<(), RecorderError> {
        // Get current room info and stream URL
        let room_info = self.room_info.read().await.clone();
        let Some(stream_url) = self.extra.stream_url.read().await.clone() else {
            return Err(RecorderError::NoStreamAvailable);
        };

        let work_dir = self.work_dir(live_id).await;
        let _ = tokio::fs::create_dir_all(&work_dir.full_path()).await;

        // Persist the session parent inside every recording attempt before any
        // media is written. Startup recovery can then rebuild the exact parent
        // relationship even if the process exits before RecordStart reaches DB.
        let session_parent = self.platform_live_id.read().await.clone();
        if session_parent.is_empty() {
            return Err(RecorderError::ApiError {
                error: "Douyin live session id is empty while starting an archive".to_string(),
            });
        }
        tokio::fs::write(
            work_dir
                .with_filename(DOUYIN_SESSION_PARENT_FILE)
                .full_path(),
            session_parent,
        )
        .await?;

        // Setup danmu store
        let danmu_file_path = work_dir.with_filename("events.jsonl");
        let danmu_path = danmu_file_path.full_path();
        let Some(danmu_storage) = DanmuStorage::new(&danmu_path).await else {
            let message = format!(
                "无法创建完整弹幕文件 {}，已停止本次录制；请检查 Docker 挂载目录空间和权限",
                danmu_path.display()
            );
            if !self
                .extra
                .danmu_persist_degraded
                .swap(true, atomic::Ordering::Relaxed)
            {
                let _ = self.event_channel.send(RecorderEvent::UserNotification {
                    title: "抖音弹幕保存失败".to_string(),
                    body: message.clone(),
                });
            }
            let _ = tokio::fs::remove_dir_all(&work_dir.full_path()).await;
            tokio::time::sleep(Duration::from_secs(
                self.update_interval.load(atomic::Ordering::Relaxed).max(5),
            ))
            .await;
            return Err(RecorderError::IoError(std::io::Error::other(message)));
        };
        if let Err(error) = self.replace_danmu_storage(danmu_storage).await {
            let message = format!(
                "无法封口上一段完整弹幕，未切换到新录制段 {}；程序会重试，请检查 Docker 挂载目录：{}",
                danmu_path.display(),
                error
            );
            if !self
                .extra
                .danmu_persist_degraded
                .swap(true, atomic::Ordering::Relaxed)
            {
                let _ = self.event_channel.send(RecorderEvent::UserNotification {
                    title: "抖音弹幕保存失败".to_string(),
                    body: message.clone(),
                });
            }
            let _ = tokio::fs::remove_dir_all(&work_dir.full_path()).await;
            tokio::time::sleep(Duration::from_secs(
                self.update_interval.load(atomic::Ordering::Relaxed).max(5),
            ))
            .await;
            return Err(RecorderError::IoError(std::io::Error::other(message)));
        }
        self.extra
            .danmu_persist_degraded
            .store(false, atomic::Ordering::Relaxed);

        // Start danmu task
        *self.live_id.write().await = live_id.to_string();

        let mut danmu_task = self.danmu_task.lock().await;
        let needs_danmu_task = danmu_task
            .as_ref()
            .map(|task| task.is_finished())
            .unwrap_or(true);
        if needs_danmu_task {
            // Dropping an already-finished handle is safe. A running handle is
            // retained across HLS retries for the same Douyin live session.
            let _ = danmu_task.take();
            let self_clone = self.clone();
            log::info!("Start fetching danmu for Douyin session");
            *danmu_task = Some(tokio::spawn(async move {
                if let Err(error) = self_clone.danmu().await {
                    log::error!("Douyin danmu task stopped with an error: {error}");
                }
            }));
        }
        drop(danmu_task);

        let _ = self.event_channel.send(RecorderEvent::RecordStart {
            recorder: self.info().await,
        });

        // Cover servers can be slow or blocked independently of the live APIs.
        // Fetch in the background so neither opening danmu nor starting the HLS
        // recorder misses the beginning of a Douyin live.
        let cover_url = room_info.room_cover;
        if !cover_url.trim().is_empty() {
            let cover_client = self.client.clone();
            let cover_path = work_dir.with_filename("cover.jpg").full_path();
            tokio::spawn(async move {
                match tokio::time::timeout(
                    Duration::from_secs(20),
                    api::download_file(&cover_client, &cover_url, &cover_path),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => log::warn!("Failed to download Douyin cover: {error}"),
                    Err(_) => log::warn!("Timed out downloading Douyin cover"),
                }
            });
        }

        let hls_stream =
            construct_stream_from_variant(live_id, &stream_url, Format::TS, Codec::Avc)
                .await
                .map_err(|_| RecorderError::NoStreamAvailable)?;
        let hls_recorder = HlsRecorder::new(
            self.room_id.to_string(),
            Arc::new(hls_stream),
            self.client.clone(),
            None,
            self.event_channel.clone(),
            work_dir.full_path(),
            self.enabled.clone(),
        )
        .await;
        if let Err(e) = hls_recorder {
            log::error!("[{}]Hls recorder creation error: {}", self.room_id, e);
            return Err(e);
        }

        let hls_recorder = hls_recorder.unwrap();
        if let Err(e) = hls_recorder.start().await {
            log::error!("[{}]Error from hls recorder: {}", self.room_id, e);
            return Err(e);
        }

        Ok(())
    }
}

fn douyin_danmu_room_id(platform_live_id: &str) -> Result<String, RecorderError> {
    let room_id = platform_live_id.trim();
    if room_id.is_empty() {
        return Err(RecorderError::ApiError {
            error: "Douyin live room id is empty; refusing to connect danmu to room 0".to_string(),
        });
    }
    // Keep the provider ID as a string. Converting it to i64 can overflow when
    // Douyin expands its ID space and used to silently redirect the connection
    // to room 0, losing every danmu while video recording continued.
    Ok(room_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_recorder(cache_dir: PathBuf) -> DouyinRecorder {
        let (event_tx, _) = broadcast::channel(8);
        DouyinRecorder::new(
            "room",
            "sec_uid",
            &Account::default(),
            cache_dir,
            event_tx,
            Arc::new(|_| {}),
            Arc::new(atomic::AtomicU64::new(30)),
            true,
        )
        .await
        .unwrap()
    }

    #[test]
    fn danmu_room_id_preserves_values_larger_than_i64() {
        assert_eq!(
            douyin_danmu_room_id("184467440737095516150").unwrap(),
            "184467440737095516150"
        );
    }

    #[test]
    fn danmu_room_id_never_falls_back_to_zero_when_missing() {
        let error = douyin_danmu_room_id("  ").unwrap_err();
        assert!(error.to_string().contains("room id is empty"));
    }

    #[test]
    fn direct_hls_url_is_used_when_stream_data_is_missing() {
        assert_eq!(
            select_stream_url("https://example.com/fallback.m3u8", ""),
            Some("https://example.com/fallback.m3u8".to_string())
        );
    }

    #[test]
    fn stream_data_origin_is_preferred_over_direct_hls_fallback() {
        let stream_data = serde_json::json!({
            "data": {
                "origin": {
                    "main": {
                        "flv": "",
                        "hls": "https://example.com/origin.m3u8"
                    }
                }
            }
        })
        .to_string();

        assert_eq!(
            select_stream_url("https://example.com/fallback.m3u8", &stream_data),
            Some("https://example.com/origin.m3u8".to_string())
        );
    }

    #[test]
    fn reconnect_deduplicator_is_bounded_and_allows_expired_ids() {
        let mut deduplicator = EventDeduplicator::default();
        assert!(deduplicator.accept("WebcastChatMessage:1".to_string()));
        assert!(!deduplicator.accept("WebcastChatMessage:1".to_string()));

        for id in 2..=(DOUYIN_DEDUP_CAPACITY + 1) {
            assert!(deduplicator.accept(format!("WebcastChatMessage:{id}")));
        }
        assert_eq!(deduplicator.order.len(), DOUYIN_DEDUP_CAPACITY);
        assert!(deduplicator.accept("WebcastChatMessage:1".to_string()));
    }

    #[tokio::test]
    async fn storage_switch_seals_and_commits_the_previous_generation() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-generation-{}",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        let recorder = test_recorder(cache_dir.clone()).await;
        let first_path = cache_dir.join("first.jsonl");
        let second_path = cache_dir.join("second.jsonl");
        let first = DanmuStorage::new(&first_path).await.unwrap();
        let second = DanmuStorage::new(&second_path).await.unwrap();

        let first_generation = recorder.replace_danmu_storage(first).await.unwrap();
        let first_event = LiveEvent::new(
            "douyin",
            "room",
            "danmu",
            serde_json::json!({ "content": "first", "id": "1" }),
        )
        .with_raw(serde_json::json!({
            "id": "1",
            "method": "WebcastChatMessage"
        }));
        let first_key = DouyinRecorder::event_dedup_key(&first_event).unwrap();
        assert!(
            recorder
                .persist_event(&first_event, Some(first_key.clone()))
                .await
        );
        assert_eq!(
            recorder.extra.pending_event_ids.lock().await.as_slice(),
            &[PendingEventId {
                storage_generation: first_generation,
                key: first_key.clone(),
            }]
        );

        let second_generation = recorder.replace_danmu_storage(second).await.unwrap();
        assert!(second_generation > first_generation);
        assert!(recorder.extra.pending_event_ids.lock().await.is_empty());
        assert!(recorder
            .extra
            .recent_event_ids
            .lock()
            .await
            .seen
            .contains(&first_key));
        let first_events = DanmuStorage::read_events(&first_path).await;
        assert_eq!(first_events.len(), 1);
        assert_eq!(first_events[0].data["content"], "first");

        drop(recorder);
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn failed_barrier_releases_pending_ids_for_safe_replay() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-barrier-{}",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        let recorder = test_recorder(cache_dir.clone()).await;
        recorder
            .extra
            .danmu_storage_generation
            .store(7, atomic::Ordering::Relaxed);
        recorder
            .extra
            .pending_event_ids
            .lock()
            .await
            .push(PendingEventId {
                storage_generation: 7,
                key: "WebcastChatMessage:replay".to_string(),
            });

        let (reply, result) = tokio::sync::oneshot::channel();
        recorder
            .process_danmu_message(DanmuMessageType::PersistBarrier(reply))
            .await;

        assert!(result.await.unwrap().is_err());
        assert!(recorder.extra.pending_event_ids.lock().await.is_empty());
        assert!(!recorder
            .extra
            .recent_event_ids
            .lock()
            .await
            .seen
            .contains("WebcastChatMessage:replay"));
        drop(recorder);
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn active_session_persistence_surfaces_failure_and_can_retry() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-active-retry-{}",
            uuid::Uuid::new_v4()
        ));
        let recorder = test_recorder(cache_dir.clone()).await;
        tokio::fs::write(&cache_dir, b"blocks child directories")
            .await
            .unwrap();

        assert!(recorder.persist_active_session("session-1").await.is_err());

        tokio::fs::remove_file(&cache_dir).await.unwrap();
        recorder.persist_active_session("session-1").await.unwrap();
        let marker = active_session_path(&cache_dir, "room");
        assert_eq!(
            tokio::fs::read_to_string(&marker).await.unwrap(),
            "session-1"
        );

        // Every poll verifies disk state, so an externally removed marker is
        // recreated even though the in-memory session ID did not change.
        tokio::fs::remove_file(&marker).await.unwrap();
        recorder.persist_active_session("session-1").await.unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&marker).await.unwrap(),
            "session-1"
        );

        drop(recorder);
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn recovered_live_end_rotates_until_marker_ack() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-live-end-retry-{}",
            uuid::Uuid::new_v4()
        ));
        persist_pending_live_end(&cache_dir, "room", "pending-session")
            .await
            .unwrap();
        let (event_tx, mut event_rx) = broadcast::channel(8);
        let recorder = DouyinRecorder::new(
            "room",
            "sec_uid",
            &Account::default(),
            cache_dir.clone(),
            event_tx,
            Arc::new(|_| {}),
            Arc::new(atomic::AtomicU64::new(30)),
            true,
        )
        .await
        .unwrap();

        recorder.emit_recovered_pending_live_ends().await;
        assert!(matches!(
            event_rx.try_recv(),
            Ok(RecorderEvent::LiveEnd { .. })
        ));
        assert_eq!(recorder.extra.pending_live_ends.lock().await.len(), 1);

        // A successful broadcast send is not completion; resend while the
        // durable marker still exists so a lagged manager can recover.
        recorder.emit_recovered_pending_live_ends().await;
        assert!(matches!(
            event_rx.try_recv(),
            Ok(RecorderEvent::LiveEnd { .. })
        ));
        assert_eq!(recorder.extra.pending_live_ends.lock().await.len(), 1);

        assert!(acknowledge_live_end(&cache_dir, "room", "pending-session").await);
        recorder.emit_recovered_pending_live_ends().await;
        assert!(recorder.extra.pending_live_ends.lock().await.is_empty());
        assert!(matches!(
            event_rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        drop(recorder);
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn current_live_end_is_queued_even_when_broadcast_has_no_receiver() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-undelivered-live-end-{}",
            uuid::Uuid::new_v4()
        ));
        let (event_tx, event_rx) = broadcast::channel(1);
        drop(event_rx);
        let recorder = DouyinRecorder::new(
            "room",
            "sec_uid",
            &Account::default(),
            cache_dir.clone(),
            event_tx,
            Arc::new(|_| {}),
            Arc::new(atomic::AtomicU64::new(30)),
            true,
        )
        .await
        .unwrap();
        *recorder.platform_live_id.write().await = "undelivered-session".to_string();
        recorder
            .persist_active_session("undelivered-session")
            .await
            .unwrap();

        recorder.emit_live_end_and_reset().await;

        assert_eq!(
            recorder.extra.pending_live_ends.lock().await.as_slices().0,
            &[PendingLiveEnd {
                session_id: "undelivered-session".to_string(),
                marker_persisted: true,
            }]
        );
        assert!(
            pending_live_end_marker_exists(&cache_dir, "room", "undelivered-session")
                .await
                .unwrap()
        );

        drop(recorder);
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn live_end_waits_for_pending_marker_before_broadcast() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-gated-live-end-{}",
            uuid::Uuid::new_v4()
        ));
        let (event_tx, mut event_rx) = broadcast::channel(8);
        let recorder = DouyinRecorder::new(
            "room",
            "sec_uid",
            &Account::default(),
            cache_dir.clone(),
            event_tx,
            Arc::new(|_| {}),
            Arc::new(atomic::AtomicU64::new(30)),
            true,
        )
        .await
        .unwrap();
        *recorder.platform_live_id.write().await = "gated-session".to_string();
        tokio::fs::write(&cache_dir, b"blocks pending marker directory")
            .await
            .unwrap();

        recorder.emit_live_end_and_reset().await;

        let mut saw_live_end = false;
        while let Ok(event) = event_rx.try_recv() {
            saw_live_end |= matches!(event, RecorderEvent::LiveEnd { .. });
        }
        assert!(!saw_live_end);
        assert!(
            !recorder
                .extra
                .pending_live_ends
                .lock()
                .await
                .front()
                .unwrap()
                .marker_persisted
        );

        tokio::fs::remove_file(&cache_dir).await.unwrap();
        recorder.emit_recovered_pending_live_ends().await;
        assert!(matches!(
            event_rx.try_recv(),
            Ok(RecorderEvent::LiveEnd { .. })
        ));
        assert!(
            recorder
                .extra
                .pending_live_ends
                .lock()
                .await
                .front()
                .unwrap()
                .marker_persisted
        );

        drop(recorder);
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn recording_reset_preserves_session_parent_until_live_end() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-reset-{}",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        let (event_tx, _) = broadcast::channel(8);
        let recorder = DouyinRecorder::new(
            "room",
            "sec_uid",
            &Account::default(),
            cache_dir.clone(),
            event_tx,
            Arc::new(|_| {}),
            Arc::new(atomic::AtomicU64::new(30)),
            true,
        )
        .await
        .unwrap();
        *recorder.platform_live_id.write().await = "session-parent".to_string();
        *recorder.live_id.write().await = "failed-last-segment".to_string();
        *recorder.danmu_storage.write().await =
            DanmuStorage::new(&cache_dir.join("events.jsonl")).await;

        recorder.reset_recording().await;

        assert_eq!(
            recorder.platform_live_id.read().await.as_str(),
            "session-parent"
        );
        assert!(recorder.live_id.read().await.is_empty());
        assert!(recorder.danmu_storage.read().await.is_some());

        recorder.reset_live().await;
        assert!(recorder.platform_live_id.read().await.is_empty());
        assert!(recorder.danmu_storage.read().await.is_none());
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn interrupted_session_marker_waits_for_manager_acknowledgement() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-recovery-{}",
            uuid::Uuid::new_v4()
        ));
        let marker = active_session_path(&cache_dir, "room");
        tokio::fs::create_dir_all(marker.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&marker, "recovered-session")
            .await
            .unwrap();
        let (event_tx, mut event_rx) = broadcast::channel(8);
        let recorder = DouyinRecorder::new(
            "room",
            "sec_uid",
            &Account::default(),
            cache_dir.clone(),
            event_tx,
            Arc::new(|_| {}),
            Arc::new(atomic::AtomicU64::new(30)),
            true,
        )
        .await
        .unwrap();

        assert_eq!(
            recorder.platform_live_id.read().await.as_str(),
            "recovered-session"
        );
        recorder.emit_live_end_and_reset().await;

        let RecorderEvent::LiveEnd {
            recorder: ended, ..
        } = event_rx.try_recv().unwrap()
        else {
            panic!("expected recovered LiveEnd")
        };
        assert_eq!(ended.platform_live_id, "recovered-session");
        let pending_marker = pending_live_ends_path(&cache_dir, "room")
            .join(encode_session_marker_name("recovered-session"));
        assert!(!tokio::fs::try_exists(&marker).await.unwrap());
        assert!(tokio::fs::try_exists(&pending_marker).await.unwrap());
        assert!(acknowledge_live_end(&cache_dir, "room", "recovered-session").await);
        assert!(!tokio::fs::try_exists(&pending_marker).await.unwrap());
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn stale_live_end_cannot_clear_newer_session_marker() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-stale-ack-{}",
            uuid::Uuid::new_v4()
        ));
        let marker = active_session_path(&cache_dir, "room");
        tokio::fs::create_dir_all(marker.parent().unwrap())
            .await
            .unwrap();
        persist_pending_live_end(&cache_dir, "room", "old-session")
            .await
            .unwrap();
        tokio::fs::write(&marker, "new-session").await.unwrap();

        assert!(acknowledge_live_end(&cache_dir, "room", "old-session").await);
        assert_eq!(
            tokio::fs::read_to_string(&marker).await.unwrap(),
            "new-session"
        );
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn same_id_recovery_ack_preserves_new_active_incarnation() {
        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-douyin-same-id-{}",
            uuid::Uuid::new_v4()
        ));
        persist_pending_live_end(&cache_dir, "room", "reused-session")
            .await
            .unwrap();
        let marker = active_session_path(&cache_dir, "room");
        tokio::fs::write(&marker, "reused-session").await.unwrap();

        // Startup recognizes active==pending as the stale half of an interrupted
        // transition and removes it before accepting a new incarnation.
        let recorder = test_recorder(cache_dir.clone()).await;
        assert!(recorder.platform_live_id.read().await.is_empty());
        assert!(!tokio::fs::try_exists(&marker).await.unwrap());

        recorder
            .persist_active_session("reused-session")
            .await
            .unwrap();
        assert!(acknowledge_live_end(&cache_dir, "room", "reused-session").await);
        assert_eq!(
            tokio::fs::read_to_string(&marker).await.unwrap(),
            "reused-session"
        );

        // A delayed pending-marker retry for an old same-ID LiveEnd must not
        // remove the marker belonging to the currently live incarnation.
        recorder.room_info.write().await.status = true;
        *recorder.platform_live_id.write().await = "reused-session".to_string();
        recorder
            .extra
            .pending_live_ends
            .lock()
            .await
            .front_mut()
            .unwrap()
            .marker_persisted = false;
        recorder.emit_recovered_pending_live_ends().await;
        assert_eq!(
            tokio::fs::read_to_string(&marker).await.unwrap(),
            "reused-session"
        );
        assert!(acknowledge_live_end(&cache_dir, "room", "reused-session").await);
        assert!(acknowledge_live_end(&cache_dir, "room", "reused-session").await);

        drop(recorder);
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[test]
    fn detects_douyin_session_replacement_without_offline_poll() {
        assert_eq!(
            session_transition(true, "old-session", true, "new-session"),
            SessionTransition {
                end_previous: true,
                start_current: true,
            }
        );
    }
}

#[async_trait]
impl crate::traits::RecorderTrait<DouyinExtra> for DouyinRecorder {
    async fn stop(&self) {
        self.quit.store(true, atomic::Ordering::Relaxed);
        // Serialize against a LiveEnd already draining in the record task.
        // Taking this lock before aborting prevents cancellation from detaching
        // a danmu JoinHandle that was already removed from shared state.
        let _shutdown_guard = self.extra.danmu_shutdown.lock().await;
        if let Some(record_task) = self.record_task.lock().await.take() {
            record_task.abort();
            let _ = record_task.await;
        }
        // Docker shutdown must use the same lossless close path as LiveEnd.
        // The default trait implementation aborts the websocket consumer and
        // can discard accepted events that are still in its bounded queue.
        self.stop_danmu_locked().await;
    }

    async fn run(&self) {
        let self_clone = self.clone();
        *self.record_task.lock().await = Some(tokio::spawn(async move {
            while !self_clone.quit.load(atomic::Ordering::Relaxed) {
                if self_clone.check_status().await {
                    // Live status is ok, start recording
                    if self_clone.should_record().await {
                        self_clone
                            .is_recording
                            .store(true, atomic::Ordering::Relaxed);
                        let live_id = Utc::now().timestamp_millis().to_string();
                        if let Err(e) = self_clone.update_entries(&live_id).await {
                            log::error!("[{}]Update entries error: {}", self_clone.room_id, e);
                        }
                    }
                    if self_clone.is_recording.load(atomic::Ordering::Relaxed) {
                        let _ = self_clone.event_channel.send(RecorderEvent::RecordEnd {
                            recorder: self_clone.info().await,
                        });
                    }
                    self_clone
                        .is_recording
                        .store(false, atomic::Ordering::Relaxed);
                    // A single Douyin live can contain several HLS recording
                    // attempts. Keep platform_live_id until a confirmed
                    // LiveEnd so all valid attempts retain the same parent and
                    // automatic whole-session generation can still find them.
                    self_clone.reset_recording().await;
                    // Check status again after some seconds
                    let secs = random::<u64>() % 5;
                    tokio::time::sleep(Duration::from_secs(secs)).await;
                    continue;
                }

                tokio::time::sleep(Duration::from_secs(
                    self_clone.update_interval.load(atomic::Ordering::Relaxed),
                ))
                .await;
            }
            log::info!("[{}]Recording thread quit.", self_clone.room_id);
        }));
    }
}
