use std::{
    io,
    path::{Path, PathBuf},
};

use danmu_stream::LiveEvent;
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, BufReader, SeekFrom},
    sync::RwLock,
};

#[derive(Clone, Serialize, Debug)]
pub struct DanmuEntry {
    pub ts: i64,
    pub content: String,
    pub user_name: Option<String>,
}

pub struct DanmuStorage {
    file_path: PathBuf,
    writer: RwLock<DanmuWriter>,
}

struct DanmuWriter {
    file: File,
    /// The last byte offset known to contain only complete records. This is set
    /// when a partial append cannot be rolled back immediately. No later write
    /// or ACK barrier may proceed until the file is truncated to this offset.
    poisoned_at: Option<u64>,
}

impl DanmuWriter {
    async fn recover_if_poisoned(&mut self) -> io::Result<()> {
        let Some(reliable_len) = self.poisoned_at else {
            return Ok(());
        };
        self.file.set_len(reliable_len).await?;
        self.poisoned_at = None;
        Ok(())
    }
}

async fn repair_unterminated_tail(file_path: &PathBuf, writer: &mut File) -> io::Result<()> {
    const SCAN_CHUNK_SIZE: u64 = 8 * 1024;

    let file_len = writer.metadata().await?.len();
    if file_len == 0 {
        return Ok(());
    }

    let mut reader = File::open(file_path).await?;
    reader.seek(SeekFrom::End(-1)).await?;
    let mut final_byte = [0_u8; 1];
    reader.read_exact(&mut final_byte).await?;
    if final_byte[0] == b'\n' {
        return Ok(());
    }

    let mut line_start = 0_u64;
    let mut search_end = file_len;
    let mut scan_buffer = vec![0_u8; SCAN_CHUNK_SIZE as usize];
    while search_end > 0 {
        let chunk_len = search_end.min(SCAN_CHUNK_SIZE);
        let chunk_start = search_end - chunk_len;
        reader.seek(SeekFrom::Start(chunk_start)).await?;
        reader
            .read_exact(&mut scan_buffer[..chunk_len as usize])
            .await?;
        if let Some(newline) = scan_buffer[..chunk_len as usize]
            .iter()
            .rposition(|byte| *byte == b'\n')
        {
            line_start = chunk_start + newline as u64 + 1;
            break;
        }
        search_end = chunk_start;
    }

    let tail_len = usize::try_from(file_len - line_start).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "unterminated danmu record exceeds addressable memory",
        )
    })?;
    let mut tail = vec![0_u8; tail_len];
    reader.seek(SeekFrom::Start(line_start)).await?;
    reader.read_exact(&mut tail).await?;
    let valid_complete_record = std::str::from_utf8(&tail)
        .ok()
        .and_then(|line| parse_event_line(line.trim_end_matches('\r')))
        .is_some();

    if valid_complete_record {
        writer.write_all(b"\n").await?;
    } else {
        // A process can be killed between writing bytes and the terminating
        // newline. Truncate only that incomplete final record so future valid
        // events remain readable and downloadable.
        writer.set_len(line_start).await?;
        log::warn!(
            "Removed an incomplete trailing danmu record from {}",
            file_path.display()
        );
    }

    Ok(())
}

impl DanmuStorage {
    pub async fn new(file_path: &PathBuf) -> Option<DanmuStorage> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .map_err(|e| {
                log::error!("Failed to open danmu file for append: {}", e);
                e
            })
            .ok()?;
        // Preserve a valid legacy/JSONL final record, but remove a partial
        // JSON write left by a killed Docker process before appending.
        if let Err(error) = repair_unterminated_tail(file_path, &mut file).await {
            log::error!("Failed to repair danmu append boundary: {error}");
            return None;
        }
        Some(DanmuStorage {
            file_path: file_path.clone(),
            writer: RwLock::new(DanmuWriter {
                file,
                poisoned_at: None,
            }),
        })
    }

    pub async fn add_event(&self, event: &LiveEvent) -> Result<(), std::io::Error> {
        let Ok(mut line) = serde_json::to_string(event) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "serialize live event failed",
            ));
        };
        line.push('\n');
        let mut writer = self.writer.write().await;
        writer.recover_if_poisoned().await?;
        let original_len = writer.file.metadata().await?.len();
        if let Err(write_error) = writer.file.write_all(line.as_bytes()).await {
            // write_all may have appended a prefix before returning an error.
            // Roll it back so a retry cannot turn two fragments into one
            // newline-terminated but permanently invalid JSONL record.
            if let Err(rollback_error) = writer.file.set_len(original_len).await {
                writer.poisoned_at = Some(original_len);
                return Err(io::Error::new(
                    write_error.kind(),
                    format!(
                        "danmu write failed ({write_error}) and rollback to {original_len} bytes failed ({rollback_error})"
                    ),
                ));
            }
            return Err(write_error);
        }
        Ok(())
    }

    pub async fn add_line(&self, ts: i64, content: &str) -> Result<(), std::io::Error> {
        self.add_event(&LiveEvent {
            ts,
            platform: "unknown".to_string(),
            room_id: String::new(),
            event_type: "danmu".to_string(),
            data: serde_json::json!({ "content": content }),
            raw: Value::Null,
        })
        .await
    }

    /// Finish Tokio's outstanding file operation. Providers use this as a
    /// lightweight per-frame persistence barrier before acknowledging data.
    pub async fn flush(&self) -> Result<(), std::io::Error> {
        let mut writer = self.writer.write().await;
        writer.recover_if_poisoned().await?;
        writer.file.flush().await
    }

    /// Flush and sync the final tail at LiveEnd/SIGTERM. Keeping `sync_data`
    /// out of the per-frame ACK path avoids pathological Docker bind-mount I/O.
    pub async fn sync(&self) -> Result<(), std::io::Error> {
        let mut writer = self.writer.write().await;
        writer.recover_if_poisoned().await?;
        writer.file.flush().await?;
        writer.file.sync_data().await
    }

    /// Repair a write which was cancelled while the blocking file operation was
    /// in flight. Normal write errors are rolled back directly by `add_event`.
    pub async fn repair_tail(&self) -> Result<(), std::io::Error> {
        let mut writer = self.writer.write().await;
        writer.recover_if_poisoned().await?;
        repair_unterminated_tail(&self.file_path, &mut writer.file).await
    }

    /// Return the complete persisted events without discarding provider data.
    pub async fn get_events(&self) -> Vec<LiveEvent> {
        Self::read_events(&self.file_path).await
    }

    /// Read without opening an append writer or repairing/truncating the tail.
    /// This is safe while another recorder instance is actively writing.
    pub async fn read_events(file_path: &Path) -> Vec<LiveEvent> {
        let Ok(file) = File::open(file_path).await else {
            log::error!("Failed to read danmu file: {file_path:?}");
            return Vec::new();
        };
        let mut lines = BufReader::new(file).lines();
        let mut events = Vec::new();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if let Some(event) = parse_event_line(&line) {
                        events.push(event);
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    log::error!("Failed to read danmu line from {:?}: {error}", file_path);
                    break;
                }
            }
        }
        events
    }

    // get entries with ts relative to live start time
    pub async fn get_entries(&self, live_start_ts: i64) -> Vec<DanmuEntry> {
        Self::read_entries(&self.file_path, live_start_ts).await
    }

    /// Read display entries without modifying a concurrently written file.
    pub async fn read_entries(file_path: &Path, live_start_ts: i64) -> Vec<DanmuEntry> {
        let Ok(file) = File::open(file_path).await else {
            log::error!("Failed to read danmu file: {file_path:?}");
            return Vec::new();
        };
        let mut lines = BufReader::new(file).lines();
        let mut danmus = Vec::new();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let Some(event) = parse_event_line(&line) else {
                        continue;
                    };
                    if event.event_type != "danmu" {
                        continue;
                    }
                    let ts = event.ts.saturating_sub(live_start_ts);
                    if ts < 0 {
                        continue;
                    }
                    let Some(content) = event.data.get("content").and_then(Value::as_str) else {
                        continue;
                    };
                    danmus.push(DanmuEntry {
                        ts,
                        content: content.to_string(),
                        user_name: event
                            .data
                            .get("user_name")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    });
                }
                Ok(None) => break,
                Err(error) => {
                    log::error!("Failed to read danmu line from {:?}: {error}", file_path);
                    break;
                }
            }
        }
        danmus
    }
}

fn parse_event_line(line: &str) -> Option<LiveEvent> {
    if let Ok(event) = serde_json::from_str::<LiveEvent>(line) {
        return Some(event);
    }

    // Read old recordings as a migration aid. New writes are always JSONL and
    // never append to the legacy format.
    let (ts, content) = line.split_once(':')?;
    Some(LiveEvent {
        ts: ts.parse().ok()?,
        platform: "unknown".to_string(),
        room_id: String::new(),
        event_type: "danmu".to_string(),
        data: serde_json::json!({ "content": content }),
        raw: Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persists_and_reloads_complete_events_without_an_in_memory_raw_cache() {
        let path = std::env::temp_dir().join(format!(
            "bili-shadowreplay-danmu-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let storage = DanmuStorage::new(&path).await.unwrap();
        storage
            .add_event(&LiveEvent {
                ts: 1_788_868_414_771,
                platform: "douyin".to_string(),
                room_id: "123".to_string(),
                event_type: "danmu".to_string(),
                data: serde_json::json!({
                    "user_name": "枯枝邀明月",
                    "content": "终于能播了"
                }),
                raw: serde_json::json!({
                    "id": "7683131321120630299",
                    "method": "WebcastChatMessage",
                    "content": "终于能播了"
                }),
            })
            .await
            .unwrap();

        let events = storage.get_events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].raw["id"], "7683131321120630299");
        assert_eq!(storage.get_entries(0).await[0].content, "终于能播了");

        drop(storage);
        let _ = tokio::fs::remove_file(path).await;
    }

    #[test]
    fn reads_legacy_timestamp_content_lines() {
        let event = parse_event_line("1234:legacy:content").unwrap();
        assert_eq!(event.ts, 1234);
        assert_eq!(event.data["content"], "legacy:content");
        assert!(event.raw.is_null());
    }

    #[tokio::test]
    async fn appending_jsonl_does_not_merge_with_unterminated_legacy_content() {
        let path = std::env::temp_dir().join(format!(
            "bili-shadowreplay-legacy-danmu-{}.txt",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::write(&path, b"1234:legacy").await.unwrap();
        let storage = DanmuStorage::new(&path).await.unwrap();
        storage.add_line(5678, "new").await.unwrap();

        let events = storage.get_events().await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data["content"], "legacy");
        assert_eq!(events[1].data["content"], "new");

        drop(storage);
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn incomplete_json_tail_is_removed_before_new_events_are_appended() {
        let path = std::env::temp_dir().join(format!(
            "bili-shadowreplay-partial-danmu-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::write(&path, b"1234:legacy\n{\"ts\":")
            .await
            .unwrap();
        let storage = DanmuStorage::new(&path).await.unwrap();
        storage.add_line(5678, "after restart").await.unwrap();

        let events = storage.get_events().await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data["content"], "legacy");
        assert_eq!(events[1].data["content"], "after restart");
        assert!(!tokio::fs::read_to_string(&path)
            .await
            .unwrap()
            .contains("{\"ts\":"));

        drop(storage);
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn readonly_access_never_repairs_an_active_partial_tail() {
        let path = std::env::temp_dir().join(format!(
            "bili-shadowreplay-active-danmu-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let bytes = b"1234:complete\n{\"ts\":";
        tokio::fs::write(&path, bytes).await.unwrap();

        let events = DanmuStorage::read_events(&path).await;

        assert_eq!(events.len(), 1);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), bytes);
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn poisoned_writer_truncates_to_reliable_offset_before_retrying() {
        let path = std::env::temp_dir().join(format!(
            "bili-shadowreplay-poisoned-danmu-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let complete = b"1234:complete\n";
        tokio::fs::write(&path, complete).await.unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).await.unwrap();
        file.write_all(b"{\"ts\":").await.unwrap();
        file.flush().await.unwrap();

        // Model the state left when write_all appended a prefix and the
        // immediate set_len rollback also failed. The next operation must use
        // the saved reliable boundary rather than append after that prefix.
        let storage = DanmuStorage {
            file_path: path.clone(),
            writer: RwLock::new(DanmuWriter {
                file,
                poisoned_at: Some(complete.len() as u64),
            }),
        };
        storage.add_line(5678, "after recovery").await.unwrap();
        storage.flush().await.unwrap();

        let events = storage.get_events().await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data["content"], "complete");
        assert_eq!(events[1].data["content"], "after recovery");
        assert!(!tokio::fs::read_to_string(&path)
            .await
            .unwrap()
            .contains("{\"ts\":{\"ts\":"));

        drop(storage);
        let _ = tokio::fs::remove_file(path).await;
    }
}
