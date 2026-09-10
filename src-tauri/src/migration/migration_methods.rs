use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use base64::Engine;

use crate::database::Database;
use crate::recorder_manager::RecorderManagerError;
use recorder::entry::EntryStore;
use recorder::platforms::douyin::{recovery_session_parent_fallback, DOUYIN_SESSION_PARENT_FILE};
use recorder::platforms::PlatformType;

async fn archived_file_size(path: &std::path::Path) -> Result<u64, std::io::Error> {
    let mut size = 0_u64;
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            size = size.saturating_add(entry.metadata().await?.len());
        }
    }
    Ok(size)
}

async fn has_nonempty_douyin_danmu(path: &std::path::Path) -> bool {
    for file_name in ["events.jsonl", "danmu.txt"] {
        if tokio::fs::metadata(path.join(file_name))
            .await
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
        {
            return true;
        }
    }
    false
}

async fn playlist_has_local_media_segment(
    archive_path: &Path,
    playlist_path: &Path,
) -> Result<bool, std::io::Error> {
    let bytes = tokio::fs::read(playlist_path).await?;
    let Ok((_, playlist)) = m3u8_rs::parse_media_playlist(&bytes) else {
        log::warn!(
            "preserving archive with an unreadable playlist as data-only: {}",
            playlist_path.display()
        );
        return Ok(false);
    };

    for segment in playlist.segments {
        // HLS downloads keep the URI path but remove query/fragment suffixes.
        // Reject absolute or parent-relative paths so migration cannot mistake
        // an unrelated file outside the archive for recorded media.
        let uri_path = segment.uri.split(['?', '#']).next().unwrap_or_default();
        let relative_path = Path::new(uri_path);
        if uri_path.is_empty()
            || relative_path.components().any(|component| {
                matches!(
                    component,
                    Component::Prefix(_) | Component::RootDir | Component::ParentDir
                )
            })
        {
            continue;
        }

        if tokio::fs::metadata(archive_path.join(relative_path))
            .await
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
        {
            return Ok(true);
        }
    }

    Ok(false)
}

pub async fn try_rebuild_archives(
    db: &Arc<Database>,
    cache_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let rooms = db.get_recorders().await?;
    for room in rooms {
        let room_id = room.room_id;
        let room_cache_path = cache_path.join(format!("{}/{}", room.platform, room_id));
        let platform = PlatformType::from_str(room.platform.as_str()).map_err(|_| {
            RecorderManagerError::InvalidPlatformType {
                platform: room.platform.to_string(),
            }
        })?;
        let active_session_parent = if platform == PlatformType::Douyin {
            recovery_session_parent_fallback(&cache_path, &room_id).await
        } else {
            None
        };
        let mut files = match tokio::fs::read_dir(&room_cache_path).await {
            Ok(files) => files,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        while let Some(file) = files.next_entry().await? {
            if file.file_type().await?.is_dir() {
                // use folder name as live_id
                let live_id = file.file_name();
                let Some(live_id) = live_id.to_str().map(str::to_string) else {
                    log::warn!(
                        "ignoring archive folder with a non-Unicode name: {:?}",
                        file.path()
                    );
                    continue;
                };
                if live_id.starts_with('.') {
                    // Room-level recovery metadata such as
                    // `.pending-live-ends` is not an archive attempt.
                    continue;
                }
                let record_path = file.path();
                let entry_store = EntryStore::new(record_path.to_string_lossy().as_ref()).await;
                let existing_record = db.get_record(&room_id, &live_id).await;

                // EntryStore treats missing, unreadable and corrupt entries.log
                // alike. Never delete user recordings based on `is_empty`.
                // A playlist is video evidence only when at least one listed
                // media segment exists on disk. A header-only playlist is a
                // normal crash/early-stop artifact and must not trigger an
                // endless whole-session FFmpeg retry.
                let (archive_duration, archive_size) = if entry_store.is_empty() {
                    let playlist_path = record_path.join("playlist.m3u8");
                    let playlist_size = tokio::fs::metadata(&playlist_path)
                        .await
                        .ok()
                        .filter(|metadata| metadata.is_file() && metadata.len() > 0)
                        .map(|metadata| metadata.len());
                    let has_playlist = playlist_size.is_some();
                    let usable_playlist_size = match playlist_size {
                        Some(size) if platform == PlatformType::Douyin => {
                            playlist_has_local_media_segment(&record_path, &playlist_path)
                                .await?
                                .then_some(size)
                        }
                        size => size,
                    };
                    if let Some(playlist_size) = usable_playlist_size {
                        (
                            0.0,
                            archived_file_size(&record_path).await?.max(playlist_size),
                        )
                    } else if platform == PlatformType::Douyin
                        && (has_playlist || has_nonempty_douyin_danmu(&record_path).await)
                    {
                        // A crash can happen after events.jsonl is created but
                        // before RecordStart reaches the database or any video
                        // segment is committed. Keep the attempt discoverable
                        // for full-danmu export, but size=0 must continue to mean
                        // that no playable archive exists for auto generation.
                        log::info!(
                            "rebuilding data-only Douyin archive: {}",
                            record_path.display()
                        );
                        (0.0, 0)
                    } else {
                        log::warn!(
                            "preserving archive directory without readable entries, playlist, or Douyin danmu: {}",
                            record_path.display()
                        );
                        continue;
                    }
                } else {
                    (entry_store.total_duration(), entry_store.total_size())
                };

                // A per-attempt marker is authoritative. In particular, an
                // archive row may already exist from a previous startup where
                // the session relationship was not known yet. Repair that row
                // before recovered LiveEnd handling queries by parent id.
                let persisted_session_parent = if platform == PlatformType::Douyin {
                    tokio::fs::read_to_string(record_path.join(DOUYIN_SESSION_PARENT_FILE))
                        .await
                        .ok()
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty())
                } else {
                    None
                };

                // check if live_id is in db
                if let Ok(record) = existing_record {
                    let repaired_parent_id = persisted_session_parent
                        .as_deref()
                        .filter(|parent_id| record.parent_id.as_str() != *parent_id);
                    if let Some(parent_id) = repaired_parent_id {
                        db.update_record_parent_id(&live_id, parent_id).await?;
                    }
                    if record.size == 0 {
                        db.update_record_delta(&live_id, archive_duration, archive_size)
                            .await?;
                    }
                    continue;
                }

                // create a record for this live_id
                let persisted_parent = if platform == PlatformType::Douyin {
                    persisted_session_parent.or_else(|| active_session_parent.clone())
                } else {
                    None
                };
                let parent_id = persisted_parent.as_deref().unwrap_or(&live_id);
                let record = db
                    .add_record(
                        platform,
                        parent_id,
                        &live_id,
                        &room_id,
                        &format!("UnknownLive {live_id}"),
                        None,
                    )
                    .await?;
                db.update_record_delta(&live_id, archive_duration, archive_size)
                    .await?;

                log::info!("rebuild archive {record:?}");
            }
        }
    }
    Ok(())
}

pub async fn try_convert_live_covers(
    db: &Arc<Database>,
    cache_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let rooms = db.get_recorders().await?;
    for room in rooms {
        let room_id = room.room_id;
        let room_cache_path = cache_path.join(format!("{}/{}", room.platform, room_id));
        let records = db.get_records(&room_id, 0, 999_999_999).await?;
        for record in &records {
            let record_path = room_cache_path.join(record.live_id.clone());
            let cover = record.cover.clone();
            if cover.is_none() {
                continue;
            }

            let cover = cover.unwrap();
            if cover.starts_with("data:") {
                let base64 = cover.split("base64,").nth(1).unwrap();
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(base64)
                    .unwrap();
                let path = record_path.join("cover.jpg");
                tokio::fs::write(&path, bytes).await?;

                log::info!("convert live cover: {}", path.display());
                // update record
                db.update_record_cover(
                    record.live_id.as_str(),
                    Some(format!(
                        "{}/{}/{}/cover.jpg",
                        room.platform, room_id, record.live_id
                    )),
                )
                .await?;
            }
        }
    }
    Ok(())
}

pub async fn try_convert_clip_covers(
    db: &Arc<Database>,
    output_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let videos = db.get_all_videos().await?;
    log::debug!("videos: {}", videos.len());
    for video in &videos {
        let cover = video.cover.clone();
        if cover.starts_with("data:") {
            let base64 = cover.split("base64,").nth(1).unwrap();
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(base64)
                .unwrap();

            let video_file_path = output_path.join(video.file.clone());
            let cover_file_path = video_file_path.with_extension("jpg");
            log::debug!("cover_file_path: {}", cover_file_path.display());
            tokio::fs::write(&cover_file_path, bytes).await?;

            log::info!("convert clip cover: {}", cover_file_path.display());
            // update record
            db.update_video_cover(
                video.id,
                cover_file_path.file_name().unwrap().to_str().unwrap(),
            )
            .await?;
        }
    }
    Ok(())
}

pub async fn try_add_parent_id_to_records(
    db: &Arc<Database>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rooms = db.get_recorders().await?;
    for room in &rooms {
        let records = db.get_records(&room.room_id, 0, 999_999_999).await?;
        for record in &records {
            if record.parent_id.is_empty() {
                db.update_record_parent_id(record.live_id.as_str(), record.live_id.as_str())
                    .await?;
            }
        }
    }
    Ok(())
}

pub async fn try_convert_entry_to_m3u8(
    db: &Arc<Database>,
    cache_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let rooms = db.get_recorders().await?;
    for room in &rooms {
        let records = db.get_records(&room.room_id, 0, 999_999_999).await?;
        for record in &records {
            let record_path = cache_path.join(format!(
                "{}/{}/{}",
                room.platform, room.room_id, record.live_id
            ));
            let entry_file = record_path.join("entries.log");
            let m3u8_file_path = record_path.join("playlist.m3u8");
            if !entry_file.exists() || m3u8_file_path.exists() {
                continue;
            }
            let entry_store = EntryStore::new(record_path.to_str().unwrap()).await;
            if entry_store.is_empty() {
                continue;
            }
            let m3u8_content = entry_store.manifest(true, true, None);

            tokio::fs::write(&m3u8_file_path, m3u8_content).await?;
            log::info!(
                "Convert entry to m3u8: {} => {}",
                entry_file.display(),
                m3u8_file_path.display()
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn migration_database() -> Arc<Database> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE recorders (
                room_id TEXT PRIMARY KEY, created_at TEXT, platform TEXT,
                auto_start INTEGER, extra TEXT
            )",
        )
        .execute(&pool)
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

        let db = Arc::new(Database::new());
        db.set(pool).await;
        db
    }

    #[tokio::test]
    async fn rebuild_repairs_existing_douyin_parent_from_attempt_marker() {
        let db = migration_database().await;
        db.add_recorder(PlatformType::Douyin, "room", "")
            .await
            .unwrap();
        db.add_record(
            PlatformType::Douyin,
            "attempt-1",
            "attempt-1",
            "room",
            "old row",
            None,
        )
        .await
        .unwrap();
        db.update_record_delta("attempt-1", 1.0, 1).await.unwrap();

        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-parent-rebuild-{}",
            uuid::Uuid::new_v4()
        ));
        let attempt_dir = cache_dir
            .join(PlatformType::Douyin.as_str())
            .join("room")
            .join("attempt-1");
        tokio::fs::create_dir_all(&attempt_dir).await.unwrap();
        tokio::fs::write(attempt_dir.join("playlist.m3u8"), "#EXTM3U\n")
            .await
            .unwrap();
        tokio::fs::write(
            attempt_dir.join(DOUYIN_SESSION_PARENT_FILE),
            "session-parent",
        )
        .await
        .unwrap();

        try_rebuild_archives(&db, cache_dir.clone()).await.unwrap();

        let repaired = db.get_record("room", "attempt-1").await.unwrap();
        assert_eq!(repaired.parent_id, "session-parent");
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn rebuild_keeps_douyin_danmu_only_attempts_at_zero_video_size() {
        let db = migration_database().await;
        db.add_recorder(PlatformType::Douyin, "room", "")
            .await
            .unwrap();

        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-danmu-only-rebuild-{}",
            uuid::Uuid::new_v4()
        ));
        let room_dir = cache_dir.join(PlatformType::Douyin.as_str()).join("room");
        for (live_id, file_name) in [
            ("jsonl-attempt", "events.jsonl"),
            ("legacy-attempt", "danmu.txt"),
        ] {
            let attempt_dir = room_dir.join(live_id);
            tokio::fs::create_dir_all(&attempt_dir).await.unwrap();
            tokio::fs::write(
                attempt_dir.join(DOUYIN_SESSION_PARENT_FILE),
                "session-parent",
            )
            .await
            .unwrap();
            tokio::fs::write(attempt_dir.join(file_name), "saved danmu\n")
                .await
                .unwrap();
        }

        let empty_attempt = room_dir.join("empty-attempt");
        tokio::fs::create_dir_all(&empty_attempt).await.unwrap();
        tokio::fs::write(
            empty_attempt.join(DOUYIN_SESSION_PARENT_FILE),
            "session-parent",
        )
        .await
        .unwrap();

        try_rebuild_archives(&db, cache_dir.clone()).await.unwrap();

        let archives = db
            .get_archives_by_parent_id("room", "session-parent")
            .await
            .unwrap();
        assert_eq!(archives.len(), 2);
        let mut live_ids = archives
            .iter()
            .map(|archive| archive.live_id.as_str())
            .collect::<Vec<_>>();
        live_ids.sort_unstable();
        assert_eq!(live_ids, ["jsonl-attempt", "legacy-attempt"]);
        assert!(archives
            .iter()
            .all(|archive| archive.size == 0 && archive.length == 0.0));
        assert!(db.get_record("room", "empty-attempt").await.is_err());
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }

    #[tokio::test]
    async fn rebuild_requires_a_real_douyin_playlist_segment_for_video_size() {
        let db = migration_database().await;
        db.add_recorder(PlatformType::Douyin, "room", "")
            .await
            .unwrap();

        let cache_dir = std::env::temp_dir().join(format!(
            "bili-shadowreplay-playlist-rebuild-{}",
            uuid::Uuid::new_v4()
        ));
        let room_dir = cache_dir.join(PlatformType::Douyin.as_str()).join("room");
        for live_id in ["header-only", "missing-segment", "real-segment"] {
            let attempt_dir = room_dir.join(live_id);
            tokio::fs::create_dir_all(&attempt_dir).await.unwrap();
            tokio::fs::write(
                attempt_dir.join(DOUYIN_SESSION_PARENT_FILE),
                "session-parent",
            )
            .await
            .unwrap();
        }

        tokio::fs::write(room_dir.join("header-only/playlist.m3u8"), "#EXTM3U\n")
            .await
            .unwrap();
        tokio::fs::write(
            room_dir.join("missing-segment/playlist.m3u8"),
            "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:1.0,\nmissing.ts\n",
        )
        .await
        .unwrap();
        tokio::fs::write(
            room_dir.join("real-segment/playlist.m3u8"),
            "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:1.0,\nsegment.ts?token=test\n",
        )
        .await
        .unwrap();
        tokio::fs::write(room_dir.join("real-segment/segment.ts"), b"media")
            .await
            .unwrap();

        try_rebuild_archives(&db, cache_dir.clone()).await.unwrap();

        let archives = db
            .get_archives_by_parent_id("room", "session-parent")
            .await
            .unwrap();
        assert_eq!(archives.len(), 3);
        for archive in archives {
            match archive.live_id.as_str() {
                "header-only" | "missing-segment" => assert_eq!(archive.size, 0),
                "real-segment" => assert!(archive.size > 0),
                unexpected => panic!("unexpected archive {unexpected}"),
            }
        }

        assert!(room_dir.join("header-only/playlist.m3u8").exists());
        assert!(room_dir.join("missing-segment/playlist.m3u8").exists());
        let _ = tokio::fs::remove_dir_all(cache_dir).await;
    }
}
