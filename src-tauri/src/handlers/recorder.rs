use std::str::FromStr;

use crate::danmu2ass;
use crate::database::record::RecordRow;
use crate::database::recorder::RecorderRow;
use crate::database::task::TaskRow;
use crate::progress::progress_reporter::EventEmitter;
use crate::progress::progress_reporter::ProgressReporter;
use crate::progress::progress_reporter::ProgressReporterTrait;
use crate::recorder_manager::{GenerateWholeClipParams, RecorderList};
use crate::state::State;
use crate::state_type;
use crate::task::Task;
use crate::task::TaskPriority;
use crate::webhook::events;
use danmu_stream::LiveEvent;
use recorder::account::Account;
use recorder::danmu::DanmuEntry;
use recorder::platforms::bilibili;
use recorder::platforms::douyin;
use recorder::platforms::PlatformType;
use recorder::RecorderInfo;

#[cfg(feature = "gui")]
use tauri::State as TauriState;

use serde::Deserialize;
use serde::Serialize;
use serde_json::{json, Value};

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_recorder_list(state: state_type!()) -> Result<RecorderList, ()> {
    Ok(state.recorder_manager.get_recorder_list().await)
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn add_recorder(
    state: state_type!(),
    platform: String,
    room_id: String,
    mut extra: String,
) -> Result<RecorderRow, String> {
    log::info!("Add recorder: {platform} {room_id}");
    let platform = PlatformType::from_str(&platform).unwrap();
    let account = match platform {
        PlatformType::BiliBili => {
            if let Ok(account) = state.db.get_account_by_platform("bilibili").await {
                Ok(account.to_account())
            } else {
                log::error!("No available bilibili account found");
                Err("没有可用账号，请先添加账号".to_string())
            }
        }
        PlatformType::Douyin => {
            let account = state
                .db
                .get_account_by_platform("douyin")
                .await
                .map_err(|_| {
                    log::error!("No available douyin account found");
                    "没有可用账号，请先添加账号".to_string()
                })?
                .to_account();
            let client = reqwest::Client::new();
            let sec_uid = douyin::api::get_room_owner_sec_uid(&client, &account, &room_id)
                .await
                .map_err(|e| e.to_string())?;
            extra = sec_uid;
            Ok(account)
        }
        PlatformType::Huya => {
            if let Ok(account) = state.db.get_account_by_platform("huya").await {
                Ok(account.to_account())
            } else {
                Ok(Account::default())
            }
        }
        PlatformType::Kuaishou => {
            if let Ok(account) = state.db.get_account_by_platform("kuaishou").await {
                Ok(account.to_account())
            } else {
                Ok(Account::default())
            }
        }
        PlatformType::TikTok => {
            if let Ok(account) = state.db.get_account_by_platform("tiktok").await {
                Ok(account.to_account())
            } else {
                Ok(Account::default())
            }
        }
        PlatformType::Xiaohongshu => {
            if let Ok(account) = state.db.get_account_by_platform("xiaohongshu").await {
                Ok(account.to_account())
            } else {
                Ok(Account::default())
            }
        }
        PlatformType::Weibo => {
            if let Ok(account) = state.db.get_account_by_platform("weibo").await {
                Ok(account.to_account())
            } else {
                Ok(Account::default())
            }
        }
        _ => Err("不支持的平台".to_string()),
    };

    match account {
        Ok(account) => match state
            .recorder_manager
            .add_recorder(&account, platform, &room_id, &extra, true)
            .await
        {
            Ok(()) => {
                let room = state.db.add_recorder(platform, &room_id, &extra).await?;
                state
                    .db
                    .new_message("添加直播间", &format!("添加了新直播间 {room_id}"))
                    .await?;
                // post webhook event
                let event = events::new_webhook_event(
                    events::RECORDER_ADDED,
                    events::Payload::Recorder(room.clone()),
                );
                if let Err(e) = state.webhook_poster.post_event(&event).await {
                    log::error!("Post webhook event error: {e}");
                }
                Ok(room)
            }
            Err(e) => {
                log::error!("Failed to add recorder: {e}");
                Err(format!("添加失败: {e}"))
            }
        },
        Err(e) => {
            log::error!("Failed to add recorder: {e}");
            Err(format!("添加失败: {e}"))
        }
    }
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn remove_recorder(
    state: state_type!(),
    platform: String,
    room_id: String,
) -> Result<(), String> {
    log::info!("Remove recorder: {platform} {room_id}");
    let platform = PlatformType::from_str(&platform).unwrap();
    match state
        .recorder_manager
        .remove_recorder(platform, &room_id)
        .await
    {
        Ok(recorder) => {
            state
                .db
                .new_message("移除直播间", &format!("移除了直播间 {room_id}"))
                .await?;
            // post webhook event
            let event = events::new_webhook_event(
                events::RECORDER_REMOVED,
                events::Payload::Recorder(recorder),
            );
            if let Err(e) = state.webhook_poster.post_event(&event).await {
                log::error!("Post webhook event error: {e}");
            }
            log::info!("Removed recorder: {} {}", platform.as_str(), room_id);
            Ok(())
        }
        Err(e) => {
            log::error!("Failed to remove recorder: {e}");
            Err(e.to_string())
        }
    }
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_room_info(
    state: state_type!(),
    platform: String,
    room_id: String,
) -> Result<RecorderInfo, String> {
    let platform = PlatformType::from_str(&platform).unwrap();
    if let Some(info) = state
        .recorder_manager
        .get_recorder_info(platform, &room_id)
        .await
    {
        Ok(info)
    } else {
        Err("Not found".to_string())
    }
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_archive_disk_usage(state: state_type!()) -> Result<i64, String> {
    Ok(state.recorder_manager.get_archive_disk_usage().await?)
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_archives(
    state: state_type!(),
    room_id: String,
    offset: i64,
    limit: i64,
) -> Result<Vec<RecordRow>, String> {
    Ok(state
        .recorder_manager
        .get_archives(&room_id, offset, limit)
        .await?)
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_archive(
    state: state_type!(),
    room_id: String,
    live_id: String,
) -> Result<RecordRow, String> {
    Ok(state
        .recorder_manager
        .get_archive(&room_id, &live_id)
        .await?)
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_archives_by_parent_id(
    state: state_type!(),
    room_id: String,
    parent_id: String,
) -> Result<Vec<RecordRow>, String> {
    Ok(state
        .db
        .get_archives_by_parent_id(&room_id, &parent_id)
        .await?)
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_archive_subtitle(
    state: state_type!(),
    platform: String,
    room_id: String,
    live_id: String,
) -> Result<String, String> {
    let platform = PlatformType::from_str(&platform)?;
    Ok(state
        .recorder_manager
        .get_archive_subtitle(platform, &room_id, &live_id)
        .await?)
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn generate_archive_subtitle(
    state: state_type!(),
    platform: String,
    room_id: String,
    live_id: String,
) -> Result<String, String> {
    let platform = PlatformType::from_str(&platform)?;
    Ok(state
        .recorder_manager
        .generate_archive_subtitle(platform, &room_id, &live_id)
        .await?)
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn delete_archive(
    state: state_type!(),
    platform: String,
    room_id: String,
    live_id: String,
) -> Result<(), String> {
    let platform = PlatformType::from_str(&platform)?;
    let to_delete = state
        .recorder_manager
        .delete_archive(platform, &room_id, &live_id)
        .await?;
    state
        .db
        .new_message(
            "删除历史缓存",
            &format!("删除了房间 {room_id} 的历史缓存 {live_id}"),
        )
        .await?;
    // post webhook event
    let event =
        events::new_webhook_event(events::ARCHIVE_DELETED, events::Payload::Archive(to_delete));
    if let Err(e) = state.webhook_poster.post_event(&event).await {
        log::error!("Post webhook event error: {e}");
    }
    Ok(())
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn delete_archives(
    state: state_type!(),
    platform: String,
    room_id: String,
    live_ids: Vec<String>,
) -> Result<(), String> {
    let platform = PlatformType::from_str(&platform)?;
    let to_deletes = state
        .recorder_manager
        .delete_archives(
            platform,
            &room_id,
            &live_ids
                .iter()
                .map(std::string::String::as_str)
                .collect::<Vec<&str>>(),
        )
        .await?;
    state
        .db
        .new_message(
            "删除历史缓存",
            &format!("删除了房间 {} 的历史缓存 {}", room_id, live_ids.join(", ")),
        )
        .await?;
    for to_delete in to_deletes {
        // post webhook event
        let event =
            events::new_webhook_event(events::ARCHIVE_DELETED, events::Payload::Archive(to_delete));
        if let Err(e) = state.webhook_poster.post_event(&event).await {
            log::error!("Post webhook event error: {e}");
        }
    }
    Ok(())
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_danmu_record(
    state: state_type!(),
    platform: String,
    room_id: String,
    live_id: String,
) -> Result<Vec<DanmuEntry>, String> {
    let platform = PlatformType::from_str(&platform)?;
    Ok(state
        .recorder_manager
        .load_danmus(platform, &room_id, &live_id)
        .await?)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportDanmuOptions {
    platform: String,
    room_id: String,
    live_id: String,
    x: i64,
    y: i64,
    #[serde(default)]
    offset: f64,
    #[serde(default)]
    local_offset: f64,
    ass: bool,
    #[serde(default)]
    full: bool,
}

fn seconds_to_millis(seconds: f64, field: &str) -> Result<i64, String> {
    let millis = seconds * 1000.0;
    if !millis.is_finite() || millis < i64::MIN as f64 || millis > i64::MAX as f64 {
        return Err(format!("{field} is outside the supported time range"));
    }
    Ok(millis.round() as i64)
}

fn full_danmu_export_entry(event: &LiveEvent) -> Value {
    if event.raw.as_object().is_some_and(|raw| !raw.is_empty()) {
        return event.raw.clone();
    }

    let id = event
        .data
        .get("id")
        .or_else(|| event.data.get("message_id"))
        .cloned()
        .unwrap_or(Value::Null);
    let method = event
        .data
        .get("method")
        .cloned()
        .unwrap_or_else(|| Value::String("danmu".to_string()));
    let user = event.data.get("user").cloned().unwrap_or_else(|| {
        json!({
            "id": event.data.get("user_id").cloned().unwrap_or(Value::Null),
            "name": event.data.get("user_name").cloned().unwrap_or(Value::Null),
        })
    });
    let content = event.data.get("content").cloned().unwrap_or(Value::Null);

    json!({
        "id": id,
        "method": method,
        "user": user,
        "content": content,
        "time": event.ts,
        "platform": event.platform,
        "roomId": event.room_id,
        "data": event.data,
    })
}

pub(crate) fn export_full_danmu_event(event: &LiveEvent) -> Result<Option<String>, String> {
    if event.event_type != "danmu" {
        return Ok(None);
    }

    serde_json::to_string(&full_danmu_export_entry(event))
        .map(Some)
        .map_err(|e| e.to_string())
}

/// Convert one persisted danmu record into the flattened JSONL format used by
/// downloads. New recordings contain a serialized `LiveEvent`; the fallback
/// keeps old `timestamp:content` recordings downloadable without pretending
/// that metadata which was never recorded can be reconstructed.
pub(crate) fn export_persisted_danmu_line(
    line: &str,
    platform: &str,
    room_id: &str,
) -> Result<Option<String>, String> {
    if line.trim().is_empty() {
        return Ok(None);
    }

    let event = match serde_json::from_str::<LiveEvent>(line) {
        Ok(event) => event,
        Err(json_error) => {
            let Some((ts, content)) = line.split_once(':') else {
                return Err(format!("Invalid persisted danmu event: {json_error}"));
            };
            let ts = ts
                .parse::<i64>()
                .map_err(|_| format!("Invalid persisted danmu event: {json_error}"))?;
            LiveEvent {
                ts,
                platform: platform.to_string(),
                room_id: room_id.to_string(),
                event_type: "danmu".to_string(),
                data: json!({ "content": content }),
                raw: Value::Null,
            }
        }
    };

    export_full_danmu_event(&event)
}

fn export_full_danmu_jsonl(events: &[LiveEvent]) -> Result<String, String> {
    let mut output = String::new();
    for event in events {
        let Some(line) = export_full_danmu_event(event)? else {
            continue;
        };
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&line);
    }
    Ok(output)
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn export_danmu(
    state: state_type!(),
    options: ExportDanmuOptions,
) -> Result<String, String> {
    let platform = PlatformType::from_str(&options.platform)?;
    if options.full {
        let events = state
            .recorder_manager
            .load_danmu_events(platform, &options.room_id, &options.live_id)
            .await?;
        return export_full_danmu_jsonl(&events);
    }

    let mut danmus = state
        .recorder_manager
        .load_danmus(platform, &options.room_id, &options.live_id)
        .await?;

    log::debug!("First danmu entry: {:?}", danmus.first());
    let timeline_origin = seconds_to_millis(options.offset + options.local_offset, "offset")?;
    for d in &mut danmus {
        d.ts = d.ts.saturating_sub(timeline_origin);
    }

    let has_range = options.x != 0 || options.y != 0;
    if has_range {
        if options.x < 0 || options.y <= options.x {
            return Err("Export range must satisfy 0 <= x < y".to_string());
        }
        let range_start = options
            .x
            .checked_mul(1000)
            .ok_or("Export range start is too large")?;
        let range_end = options
            .y
            .checked_mul(1000)
            .ok_or("Export range end is too large")?;
        danmus.retain(|entry| entry.ts >= range_start && entry.ts <= range_end);
        for entry in &mut danmus {
            entry.ts -= range_start;
        }
    } else {
        danmus.retain(|entry| entry.ts >= 0);
    }

    if options.ass {
        Ok(danmu2ass::danmu_to_ass(
            danmus,
            state.config.read().await.danmu_ass_options.clone(),
        ))
    } else {
        // map and join entries
        Ok(danmus
            .iter()
            .map(|e| format!("{}:{}", e.ts, e.content))
            .collect::<Vec<_>>()
            .join("\n"))
    }
}

#[cfg(test)]
mod export_danmu_tests {
    use super::*;

    #[test]
    fn full_export_keeps_expected_fields_and_raw_payload() {
        let event = LiveEvent {
            ts: 1_788_868_414_771,
            platform: "douyin".to_string(),
            room_id: "123".to_string(),
            event_type: "danmu".to_string(),
            data: json!({
                "id": "7683131321120630299",
                "method": "WebcastChatMessage",
                "user": { "id": "MS4w.test", "name": "枯枝邀明月" },
                "content": "终于能播了"
            }),
            raw: json!({
                "id": "7683131321120630299",
                "method": "WebcastChatMessage",
                "user": { "id": "MS4w.test", "name": "枯枝邀明月" },
                "content": "终于能播了",
                "time": 1_788_868_414_771_i64,
                "payloadHex": "0102"
            }),
        };

        let output = export_full_danmu_jsonl(&[event]).unwrap();
        let exported: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(exported["id"], "7683131321120630299");
        assert_eq!(exported["method"], "WebcastChatMessage");
        assert_eq!(exported["user"]["name"], "枯枝邀明月");
        assert_eq!(exported["content"], "终于能播了");
        assert_eq!(exported["time"], 1_788_868_414_771_i64);
        assert_eq!(exported["payloadHex"], "0102");
        assert!(exported.get("raw").is_none());
    }

    #[test]
    fn export_time_conversion_rejects_non_finite_values() {
        assert_eq!(seconds_to_millis(1.5, "offset").unwrap(), 1500);
        assert!(seconds_to_millis(f64::NAN, "offset").is_err());
        assert!(seconds_to_millis(f64::INFINITY, "offset").is_err());
    }

    #[test]
    fn full_export_excludes_non_chat_events() {
        let event = LiveEvent {
            ts: 1,
            platform: "douyin".to_string(),
            room_id: "123".to_string(),
            event_type: "gift".to_string(),
            data: json!({ "gift_name": "花束" }),
            raw: Value::Null,
        };

        assert!(export_full_danmu_jsonl(&[event]).unwrap().is_empty());
    }

    #[test]
    fn persisted_line_export_flattens_raw_douyin_event_without_losing_fields() {
        let event = LiveEvent {
            ts: 1_788_868_414_771,
            platform: "douyin".to_string(),
            room_id: "123".to_string(),
            event_type: "danmu".to_string(),
            data: json!({ "content": "终于能播了" }),
            raw: json!({
                "id": "7683131321120630299",
                "method": "WebcastChatMessage",
                "user": {
                    "id": "MS4w.test",
                    "shortId": "2988955578",
                    "displayId": "dyr8cty6m73n",
                    "name": "枯枝邀明月",
                    "gender": 1,
                    "avatar": "https://example.com/avatar.jpeg",
                    "currentTargetAnchorId": "105460512869",
                    "fansClub": [{ "anchorId": "105460512869", "level": 9 }]
                },
                "content": "终于能播了",
                "time": 1_788_868_414_771_i64,
                "payloadHex": "0102"
            }),
        };
        let persisted = serde_json::to_string(&event).unwrap();

        let output = export_persisted_danmu_line(&persisted, "douyin", "123")
            .unwrap()
            .unwrap();
        let exported: Value = serde_json::from_str(&output).unwrap();

        assert_eq!(exported, event.raw);
        assert!(exported.get("raw").is_none());
        assert!(exported.get("data").is_none());
    }

    #[test]
    fn persisted_line_export_supports_legacy_timestamp_content() {
        let output = export_persisted_danmu_line("1234:包含:冒号", "douyin", "5678")
            .unwrap()
            .unwrap();
        let exported: Value = serde_json::from_str(&output).unwrap();

        assert_eq!(exported["time"], 1234);
        assert_eq!(exported["platform"], "douyin");
        assert_eq!(exported["roomId"], "5678");
        assert_eq!(exported["content"], "包含:冒号");
    }

    #[test]
    fn persisted_line_export_rejects_corrupt_records() {
        assert!(export_persisted_danmu_line("not-json", "douyin", "123").is_err());
    }
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn send_danmaku(
    state: state_type!(),
    uid: String,
    room_id: String,
    message: String,
) -> Result<(), String> {
    let account = state.db.get_account("bilibili", &uid).await?;
    let client = reqwest::Client::new();
    match bilibili::api::send_danmaku(&client, &account.to_account(), &room_id, &message).await {
        Ok(()) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_total_length(state: state_type!()) -> Result<f64, String> {
    match state.db.get_total_length().await {
        Ok(total_length) => Ok(total_length),
        Err(e) => Err(format!("Failed to get total length: {e}")),
    }
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_today_record_count(state: state_type!()) -> Result<i64, String> {
    match state.db.get_today_record_count().await {
        Ok(count) => Ok(count),
        Err(e) => Err(format!("Failed to get today record count: {e}")),
    }
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_recent_record(
    state: state_type!(),
    room_id: String,
    offset: i64,
    limit: i64,
) -> Result<Vec<RecordRow>, String> {
    match state.db.get_recent_record(&room_id, offset, limit).await {
        Ok(records) => Ok(records),
        Err(e) => Err(format!("Failed to get recent record: {e}")),
    }
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn set_enable(
    state: state_type!(),
    platform: String,
    room_id: String,
    enabled: bool,
) -> Result<(), String> {
    log::info!("Set enable for recorder {platform} {room_id} {enabled}");
    let platform = PlatformType::from_str(&platform)?;
    state
        .recorder_manager
        .set_enable(platform, &room_id, enabled)
        .await;
    Ok(())
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn fetch_hls(state: state_type!(), uri: String) -> Result<Vec<u8>, String> {
    // Handle wildcard pattern in the URI
    let uri = if uri.contains("/hls/") {
        uri.split("/hls/").last().unwrap_or(&uri).to_string()
    } else {
        uri
    };
    state
        .recorder_manager
        .handle_hls_request(&uri)
        .await
        .map_err(|e| e.to_string())
}

#[cfg_attr(feature = "gui", tauri::command)]
pub async fn generate_whole_clip(
    state: state_type!(),
    encode_danmu: bool,
    platform: String,
    room_id: String,
    parent_id: String,
    selected_live_ids: Option<Vec<String>>,
    output_name: Option<String>,
) -> Result<TaskRow, String> {
    log::info!("Generate whole clip for {platform} {room_id} {parent_id}");

    let task = state
        .db
        .generate_task(
            "generate_whole_clip",
            "",
            &serde_json::json!({
                "platform": platform,
                "room_id": room_id,
                "parent_id": parent_id,
                "encode_danmu": encode_danmu,
                "selected_live_ids": selected_live_ids,
                "output_name": output_name,
            })
            .to_string(),
        )
        .await?;

    #[cfg(feature = "gui")]
    let emitter = EventEmitter::new(state.app_handle.clone());
    #[cfg(feature = "headless")]
    let emitter = EventEmitter::new(state.progress_manager.get_event_sender());
    let reporter = ProgressReporter::new(state.db.clone(), &emitter, &task.id).await?;

    log::info!("Create task: {} {}", task.id, task.task_type);
    // create a tokio task to run in background
    #[cfg(feature = "gui")]
    let state_clone = (*state).clone();
    #[cfg(feature = "headless")]
    let state_clone = state.clone();

    let task_id = task.id.clone();
    state
        .task_manager
        .add_task(Task::new(
            task_id.clone(),
            TaskPriority::Normal,
            async move {
                match state_clone
                    .recorder_manager
                    .generate_whole_clip(
                        Some(&reporter),
                        GenerateWholeClipParams {
                            encode_danmu,
                            platform,
                            room_id,
                            parent_id,
                            selected_live_ids,
                            output_name,
                        },
                    )
                    .await
                {
                    Ok(()) => {
                        reporter.finish(true, "切片生成完成").await;
                        let _ = state_clone
                            .db
                            .update_task(&task_id, "success", "切片生成完成", None)
                            .await;
                        Ok(())
                    }
                    Err(e) => {
                        reporter.finish(false, &format!("切片生成失败: {e}")).await;
                        let _ = state_clone
                            .db
                            .update_task(&task_id, "failed", &format!("切片生成失败: {e}"), None)
                            .await;
                        Err(format!("切片生成失败: {e}"))
                    }
                }
            },
        ))
        .await?;
    Ok(task)
}
