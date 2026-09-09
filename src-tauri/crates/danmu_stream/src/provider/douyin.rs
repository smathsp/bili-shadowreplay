mod messages;

use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use deno_core::v8;
use deno_core::JsRuntime;
use deno_core::RuntimeOptions;
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use log::debug;
use log::{error, info};
use messages::*;
use prost::bytes::Bytes;
use prost::Message;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_tungstenite::{
    connect_async, tungstenite::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};

use crate::{provider::DanmuProvider, DanmuMessageType, DanmuStreamError, LiveEvent};
use serde_json::json;

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

type WsReadType = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type WsWriteType =
    futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WsMessage>;

pub struct DouyinDanmu {
    room_id: String,
    cookie: String,
    stop: Arc<RwLock<bool>>,
    write: Arc<RwLock<Option<WsWriteType>>>,
}

impl DouyinDanmu {
    async fn connect_and_handle(
        &self,
        tx: mpsc::UnboundedSender<DanmuMessageType>,
    ) -> Result<(), DanmuStreamError> {
        let url = self.get_wss_url().await?;

        let request = tokio_tungstenite::tungstenite::http::Request::builder()
            .uri(url)
            .header(
                tokio_tungstenite::tungstenite::http::header::COOKIE,
                self.cookie.as_str(),
            )
            .header(
                tokio_tungstenite::tungstenite::http::header::REFERER,
                "https://live.douyin.com/",
            )
            .header(
                tokio_tungstenite::tungstenite::http::header::USER_AGENT,
                USER_AGENT,
            )
            .header(
                tokio_tungstenite::tungstenite::http::header::HOST,
                "webcast5-ws-web-hl.douyin.com",
            )
            .header(
                tokio_tungstenite::tungstenite::http::header::UPGRADE,
                "websocket",
            )
            .header(
                tokio_tungstenite::tungstenite::http::header::CONNECTION,
                "Upgrade",
            )
            .header(
                tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_VERSION,
                "13",
            )
            .header(
                tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_EXTENSIONS,
                "permessage-deflate; client_max_window_bits",
            )
            .header(
                tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_KEY,
                "V1Yza5x1zcfkembl6u/0Pg==",
            )
            .body(())
            .unwrap();

        let (ws_stream, response) =
            connect_async(request)
                .await
                .map_err(|e| DanmuStreamError::WebsocketError {
                    err: format!("Failed to connect to douyin websocket: {}", e),
                })?;

        // Log the response status for debugging
        info!("WebSocket connection response: {:?}", response.status());

        let (write, read) = ws_stream.split();
        *self.write.write().await = Some(write);
        self.handle_connection(read, tx).await
    }

    async fn get_wss_url(&self) -> Result<String, DanmuStreamError> {
        // Create a new V8 runtime
        let mut runtime = JsRuntime::new(RuntimeOptions::default());

        // Add global CryptoJS object
        let crypto_js = include_str!("douyin/crypto-js.min.js");
        runtime
            .execute_script(
                "<crypto-js.min.js>",
                deno_core::FastString::from_static(crypto_js),
            )
            .map_err(|e| DanmuStreamError::WebsocketError {
                err: format!("Failed to execute crypto-js: {}", e),
            })?;

        // Load and execute the sign.js file
        let js_code = include_str!("douyin/webmssdk.js");
        runtime
            .execute_script("<sign.js>", deno_core::FastString::from_static(js_code))
            .map_err(|e| DanmuStreamError::WebsocketError {
                err: format!("Failed to execute JavaScript: {}", e),
            })?;

        // Call the get_wss_url function
        let sign_call = format!("get_wss_url(\"{}\")", self.room_id);
        let result = runtime
            .execute_script("<sign_call>", deno_core::FastString::from(sign_call))
            .map_err(|e| DanmuStreamError::WebsocketError {
                err: format!("Failed to execute JavaScript: {}", e),
            })?;

        // Get the result from the V8 runtime
        let scope = &mut runtime.handle_scope();
        let local = v8::Local::new(scope, result);
        let url = local.to_string(scope).unwrap().to_rust_string_lossy(scope);

        debug!("Douyin wss url: {}", url);

        Ok(url)
    }

    async fn handle_connection(
        &self,
        mut read: WsReadType,
        tx: mpsc::UnboundedSender<DanmuMessageType>,
    ) -> Result<(), DanmuStreamError> {
        // Start heartbeat task with error handling
        let (tx_write, mut _rx_write) = mpsc::channel(32);
        let tx_write_clone = tx_write.clone();
        let stop = Arc::clone(&self.stop);
        let heartbeat_handle = tokio::spawn(async move {
            let mut last_heartbeat = SystemTime::now();
            let mut consecutive_failures = 0;
            const MAX_FAILURES: u32 = 3;

            loop {
                if *stop.read().await {
                    log::info!("Stopping douyin danmu stream");
                    break;
                }

                tokio::time::sleep(HEARTBEAT_INTERVAL).await;

                match Self::send_heartbeat(&tx_write_clone).await {
                    Ok(_) => {
                        last_heartbeat = SystemTime::now();
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        error!("Failed to send heartbeat: {}", e);
                        consecutive_failures += 1;

                        if consecutive_failures >= MAX_FAILURES {
                            error!("Too many consecutive heartbeat failures, closing connection");
                            break;
                        }

                        // Check if we've exceeded the maximum time without a successful heartbeat
                        if let Ok(duration) = last_heartbeat.elapsed() {
                            if duration > HEARTBEAT_INTERVAL * 2 {
                                error!("No successful heartbeat for too long, closing connection");
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Main message handling loop
        let room_id = self.room_id.clone();
        let stop = Arc::clone(&self.stop);
        let write = Arc::clone(&self.write);
        let message_handle = tokio::spawn(async move {
            while let Some(msg) =
                read.try_next()
                    .await
                    .map_err(|e| DanmuStreamError::WebsocketError {
                        err: format!("Failed to read message: {}", e),
                    })?
            {
                if *stop.read().await {
                    log::info!("Stopping douyin danmu stream");
                    break;
                }

                match msg {
                    WsMessage::Binary(data) => {
                        if let Ok(Some(ack)) = handle_binary_message(&data, &tx, &room_id).await {
                            if let Some(write) = write.write().await.as_mut() {
                                if let Err(e) =
                                    write.send(WsMessage::binary(ack.encode_to_vec())).await
                                {
                                    error!("Failed to send ack: {}", e);
                                }
                            }
                        }
                    }
                    WsMessage::Close(_) => {
                        info!("WebSocket connection closed");
                        break;
                    }
                    WsMessage::Ping(data) => {
                        // Respond to ping with pong
                        if let Err(e) = tx_write.send(WsMessage::Pong(data)).await {
                            error!("Failed to send pong: {}", e);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            Ok::<(), DanmuStreamError>(())
        });

        // Wait for either the heartbeat or message handling to complete
        tokio::select! {
            result = heartbeat_handle => {
                if let Err(e) = result {
                    error!("Heartbeat task failed: {}", e);
                }
            }
            result = message_handle => {
                if let Err(e) = result {
                    error!("Message handling task failed: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn send_heartbeat(tx: &mpsc::Sender<WsMessage>) -> Result<(), DanmuStreamError> {
        // heartbeat message: 3A 02 68 62
        tx.send(WsMessage::binary(vec![0x3A, 0x02, 0x68, 0x62]))
            .await
            .map_err(|e| DanmuStreamError::WebsocketError {
                err: format!("Failed to send heartbeat message: {}", e),
            })?;
        Ok(())
    }
}

async fn handle_binary_message(
    data: &[u8],
    tx: &mpsc::UnboundedSender<DanmuMessageType>,
    room_id: &str,
) -> Result<Option<PushFrame>, DanmuStreamError> {
    // First decode the PushFrame
    let push_frame = PushFrame::decode(Bytes::from(data.to_vec())).map_err(|e| {
        DanmuStreamError::WebsocketError {
            err: format!("Failed to decode PushFrame: {}", e),
        }
    })?;

    // Decompress the payload
    let mut decoder = GzDecoder::new(push_frame.payload.as_slice());
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| DanmuStreamError::WebsocketError {
            err: format!("Failed to decompress payload: {}", e),
        })?;

    // Decode the Response from decompressed payload
    let response = Response::decode(Bytes::from(decompressed)).map_err(|e| {
        DanmuStreamError::WebsocketError {
            err: format!("Failed to decode Response: {}", e),
        }
    })?;

    // if payload_package.needAck:
    // obj = PushFrame()
    // obj.payloadType = 'ack'
    // obj.logId = log_id
    // obj.payloadType = payload_package.internalExt
    // ack = obj.SerializeToString()
    let mut ack = None;
    if response.need_ack {
        let ack_msg = PushFrame {
            payload_type: "ack".to_string(),
            log_id: push_frame.log_id,
            payload_encoding: "".to_string(),
            payload: vec![],
            seq_id: 0,
            service: 0,
            method: 0,
            headers_list: vec![],
        };

        debug!("Need to respond ack: {:?}", ack_msg);

        ack = Some(ack_msg);
    }

    for message in response.messages_list {
        match message.method.as_str() {
            "WebcastChatMessage" => {
                let chat_msg =
                    DouyinChatMessage::decode(message.payload.as_slice()).map_err(|e| {
                        DanmuStreamError::WebsocketError {
                            err: format!("Failed to decode chat message: {}", e),
                        }
                    })?;
                let event = douyin_chat_event(
                    room_id,
                    &message.method,
                    message.msg_id,
                    &message.payload,
                    chat_msg,
                );
                debug!("Received danmu event: {:?}", event);
                tx.send(DanmuMessageType::Event(event)).map_err(|e| {
                    DanmuStreamError::WebsocketError {
                        err: format!("Failed to send message to channel: {}", e),
                    }
                })?;
            }
            "WebcastGiftMessage" => {
                let gift_msg = GiftMessage::decode(message.payload.as_slice()).map_err(|e| {
                    DanmuStreamError::WebsocketError {
                        err: format!("Failed to decode gift message: {}", e),
                    }
                })?;
                if let Some(user) = gift_msg.user {
                    if let Some(gift) = gift_msg.gift {
                        log::debug!("Received gift: {} from user: {}", gift.name, user.nick_name);
                        let event = LiveEvent::new(
                            "douyin",
                            room_id,
                            "gift",
                            json!({
                                "user_id": user.id,
                                "user_name": user.nick_name,
                                "gift_name": gift.name,
                                "count": gift_msg.repeat_count.max(gift_msg.combo_count).max(1),
                                "diamond_count": gift.diamond_count,
                            }),
                        )
                        .with_raw(json!({
                            "method": "WebcastGiftMessage",
                            "payload_hex": hex::encode(&message.payload),
                        }));
                        tx.send(DanmuMessageType::Event(event)).map_err(|e| {
                            DanmuStreamError::WebsocketError {
                                err: format!("Failed to send gift event: {}", e),
                            }
                        })?;
                    }
                }
            }
            "WebcastLikeMessage" => {
                let like_msg = LikeMessage::decode(message.payload.as_slice()).map_err(|e| {
                    DanmuStreamError::WebsocketError {
                        err: format!("Failed to decode like message: {}", e),
                    }
                })?;
                if let Some(user) = like_msg.user {
                    log::debug!(
                        "Received {} likes from user: {}",
                        like_msg.count,
                        user.nick_name
                    );
                    let event = LiveEvent::new(
                        "douyin",
                        room_id,
                        "like",
                        json!({
                            "user_id": user.id,
                            "user_name": user.nick_name,
                            "count": like_msg.count,
                            "total": like_msg.total,
                        }),
                    )
                    .with_raw(json!({
                        "method": "WebcastLikeMessage",
                        "payload_hex": hex::encode(&message.payload),
                    }));
                    tx.send(DanmuMessageType::Event(event)).map_err(|e| {
                        DanmuStreamError::WebsocketError {
                            err: format!("Failed to send like event: {}", e),
                        }
                    })?;
                }
            }
            "WebcastMemberMessage" => {
                let member_msg =
                    MemberMessage::decode(message.payload.as_slice()).map_err(|e| {
                        DanmuStreamError::WebsocketError {
                            err: format!("Failed to decode member message: {}", e),
                        }
                    })?;
                if let Some(user) = member_msg.user {
                    log::debug!(
                        "Member joined: {} (Action: {})",
                        user.nick_name,
                        member_msg.action_description
                    );
                    let event = LiveEvent::new(
                        "douyin",
                        room_id,
                        "enter",
                        json!({
                            "user_id": user.id,
                            "user_name": user.nick_name,
                            "action": member_msg.action_description,
                        }),
                    )
                    .with_raw(json!({
                        "method": "WebcastMemberMessage",
                        "payload_hex": hex::encode(&message.payload),
                    }));
                    tx.send(DanmuMessageType::Event(event)).map_err(|e| {
                        DanmuStreamError::WebsocketError {
                            err: format!("Failed to send member event: {}", e),
                        }
                    })?;
                }
            }
            _ => {
                debug!("Unknown message: {:?}", message);
            }
        }
    }

    Ok(ack)
}

fn douyin_timestamp_millis(event_time: u64) -> i64 {
    let timestamp = if event_time == 0 {
        chrono::Utc::now().timestamp_millis() as u64
    } else if event_time < 10_000_000_000 {
        event_time.saturating_mul(1000)
    } else {
        event_time
    };
    i64::try_from(timestamp).unwrap_or(i64::MAX)
}

fn douyin_avatar_url(user: &User) -> Option<String> {
    [
        user.avatar_thumb.as_ref(),
        user.avatar_medium.as_ref(),
        user.avatar_large.as_ref(),
    ]
    .into_iter()
    .flatten()
    .flat_map(|image| image.url_list_list.iter())
    .find(|url| !url.is_empty())
    .cloned()
}

fn douyin_fans_clubs(user: &User) -> Vec<(i64, i32)> {
    let Some(fans_club) = &user.fans_club else {
        return Vec::new();
    };
    let mut clubs = Vec::new();
    if let Some(data) = &fans_club.data {
        if data.anchor_id != 0 {
            clubs.push((data.anchor_id, data.level));
        }
    }
    let mut preferred = fans_club
        .prefer_data
        .values()
        .filter(|data| data.anchor_id != 0)
        .map(|data| (data.anchor_id, data.level))
        .collect::<Vec<_>>();
    preferred.sort_unstable();
    for club in preferred {
        if !clubs.contains(&club) {
            clubs.push(club);
        }
    }
    clubs
}

fn douyin_chat_event(
    room_id: &str,
    method: &str,
    envelope_message_id: i64,
    payload: &[u8],
    chat_msg: DouyinChatMessage,
) -> LiveEvent {
    let timestamp_source = if chat_msg.event_time != 0 {
        chat_msg.event_time
    } else {
        chat_msg
            .common
            .as_ref()
            .map(|common| common.create_time)
            .unwrap_or_default()
    };
    let timestamp = douyin_timestamp_millis(timestamp_source);
    let message_id = chat_msg
        .common
        .as_ref()
        .map(|common| common.msg_id)
        .filter(|id| *id != 0)
        .map(|id| id.to_string())
        .unwrap_or_else(|| envelope_message_id.to_string());
    let user = chat_msg.user.unwrap_or_default();
    let user_id = if !user.sec_uid.is_empty() {
        user.sec_uid.clone()
    } else if !user.id_str.is_empty() {
        user.id_str.clone()
    } else {
        user.id.to_string()
    };
    let fans_clubs = douyin_fans_clubs(&user);
    let current_target_anchor_id = fans_clubs
        .first()
        .map(|(anchor_id, _)| anchor_id.to_string());
    let fans_club = fans_clubs
        .iter()
        .map(|(anchor_id, level)| {
            json!({
                "anchorId": anchor_id.to_string(),
                "level": level,
            })
        })
        .collect::<Vec<_>>();
    let user_data = json!({
        "id": user_id,
        "numericId": user.id.to_string(),
        "shortId": user.short_id.to_string(),
        "displayId": user.display_id,
        "name": user.nick_name,
        "gender": user.gender,
        "avatar": douyin_avatar_url(&user),
        "currentTargetAnchorId": current_target_anchor_id,
        "fansClub": fans_club,
    });
    let content = chat_msg.content;
    let raw = json!({
        "id": message_id,
        "method": method,
        "user": user_data,
        "content": content,
        "time": timestamp,
        "payloadHex": hex::encode(payload),
    });

    LiveEvent {
        ts: timestamp,
        platform: "douyin".to_string(),
        room_id: room_id.to_string(),
        event_type: "danmu".to_string(),
        data: json!({
            "id": raw["id"],
            "method": raw["method"],
            "user": raw["user"],
            "user_id": raw["user"]["id"],
            "user_name": raw["user"]["name"],
            "content": raw["content"],
            "color": 0xffffff,
        }),
        raw,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_complete_chat_event() {
        let chat = DouyinChatMessage {
            common: Some(Common {
                msg_id: 7_683_131_321_120_630_299,
                ..Default::default()
            }),
            user: Some(User {
                id: 2_988_955_578,
                short_id: 2_988_955_578,
                nick_name: "枯枝邀明月".to_string(),
                gender: 1,
                display_id: "dyr8cty6m73n".to_string(),
                sec_uid: "MS4w.test".to_string(),
                avatar_thumb: Some(Image {
                    url_list_list: vec!["https://example.com/avatar.jpeg".to_string()],
                    ..Default::default()
                }),
                fans_club: Some(FansClub {
                    data: Some(FansClubData {
                        level: 9,
                        anchor_id: 105_460_512_869,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            content: "终于能播了".to_string(),
            event_time: 1_788_868_414_771,
            ..Default::default()
        };

        let event = douyin_chat_event("123", "WebcastChatMessage", 0, &[1, 2], chat);
        assert_eq!(event.ts, 1_788_868_414_771);
        assert_eq!(event.data["content"], "终于能播了");
        assert_eq!(event.raw["id"], "7683131321120630299");
        assert_eq!(event.raw["method"], "WebcastChatMessage");
        assert_eq!(event.raw["user"]["id"], "MS4w.test");
        assert_eq!(event.raw["user"]["shortId"], "2988955578");
        assert_eq!(event.raw["user"]["fansClub"][0]["level"], 9);
        assert_eq!(event.raw["user"]["currentTargetAnchorId"], "105460512869");
        assert_eq!(event.raw["payloadHex"], "0102");
    }

    #[test]
    fn converts_second_timestamps_to_milliseconds() {
        assert_eq!(douyin_timestamp_millis(1_788_868_414), 1_788_868_414_000);
    }

    #[test]
    fn falls_back_to_common_create_time() {
        let event = douyin_chat_event(
            "123",
            "WebcastChatMessage",
            1,
            &[],
            DouyinChatMessage {
                common: Some(Common {
                    create_time: 1_788_868_414,
                    ..Default::default()
                }),
                content: "test".to_string(),
                ..Default::default()
            },
        );
        assert_eq!(event.ts, 1_788_868_414_000);
    }
}

#[async_trait]
impl DanmuProvider for DouyinDanmu {
    async fn new(identifier: &str, room_id: &str) -> Result<Self, DanmuStreamError> {
        Ok(Self {
            room_id: room_id.to_string(),
            cookie: identifier.to_string(),
            stop: Arc::new(RwLock::new(false)),
            write: Arc::new(RwLock::new(None)),
        })
    }

    async fn start(
        &self,
        tx: mpsc::UnboundedSender<DanmuMessageType>,
    ) -> Result<(), DanmuStreamError> {
        let mut retry_count = 0;
        const RETRY_DELAY: Duration = Duration::from_secs(5);
        info!(
            "Douyin WebSocket connection started, room_id: {}",
            self.room_id
        );

        loop {
            if *self.stop.read().await {
                break;
            }

            match self.connect_and_handle(tx.clone()).await {
                Ok(_) => {
                    info!(
                        "Douyin WebSocket connection closed normally, room_id: {}",
                        self.room_id
                    );
                    retry_count = 0;
                }
                Err(e) => {
                    error!("Douyin WebSocket connection error: {}", e);
                    retry_count += 1;
                }
            }

            info!(
                "Retrying connection in {} seconds... (Attempt {}), room_id: {}",
                RETRY_DELAY.as_secs(),
                retry_count,
                self.room_id
            );
            tokio::time::sleep(RETRY_DELAY).await;
        }

        Ok(())
    }

    async fn stop(&self) -> Result<(), DanmuStreamError> {
        *self.stop.write().await = true;
        if let Some(mut write) = self.write.write().await.take() {
            if let Err(e) = write.close().await {
                error!("Failed to close WebSocket connection: {}", e);
            }
        }
        Ok(())
    }
}
