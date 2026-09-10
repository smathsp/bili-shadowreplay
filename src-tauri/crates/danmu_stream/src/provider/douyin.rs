mod messages;

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

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
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{header, HeaderValue, Request},
        Message as WsMessage,
    },
    MaybeTlsStream, WebSocketStream,
};

use crate::{provider::DanmuProvider, DanmuMessageType, DanmuStreamError, LiveEvent};
use serde_json::json;

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const WEBSOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_PAYLOAD: &[u8] = &[0x3A, 0x02, 0x68, 0x62];

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
    fn websocket_request(&self, url: &str) -> Result<Request<()>, DanmuStreamError> {
        // Let tungstenite generate the Host and WebSocket handshake headers from
        // the signed URL. Douyin currently returns an `-lf` host; hard-coding an
        // `-hl` Host header makes the HTTP host disagree with both the URL and
        // TLS SNI and can cause intermittent handshake failures.
        let mut request =
            url.into_client_request()
                .map_err(|error| DanmuStreamError::WebsocketError {
                    err: format!("Failed to build douyin websocket request: {error}"),
                })?;
        let headers = request.headers_mut();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_str(&self.cookie).map_err(|error| {
                DanmuStreamError::WebsocketError {
                    err: format!("Invalid douyin cookie header: {error}"),
                }
            })?,
        );
        headers.insert(
            header::REFERER,
            HeaderValue::from_static("https://live.douyin.com/"),
        );
        headers.insert(header::USER_AGENT, HeaderValue::from_static(USER_AGENT));
        Ok(request)
    }

    async fn connect_and_handle(
        &self,
        tx: mpsc::Sender<DanmuMessageType>,
    ) -> Result<(), DanmuStreamError> {
        let url = self.get_wss_url().await?;
        let request = self.websocket_request(&url)?;

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
        let result = self.handle_connection(read, tx).await;
        *self.write.write().await = None;
        result
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
        tx: mpsc::Sender<DanmuMessageType>,
    ) -> Result<(), DanmuStreamError> {
        let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // `interval` ticks immediately once. Consume that tick so the first
        // heartbeat is sent after the configured interval.
        heartbeat.tick().await;

        loop {
            if *self.stop.read().await {
                info!("Stopping douyin danmu stream");
                break;
            }
            tokio::select! {
                _ = heartbeat.tick() => {
                    if *self.stop.read().await {
                        info!("Stopping douyin danmu stream");
                        break;
                    }
                    self.send_ws_message(Self::heartbeat_message(), "heartbeat").await?;
                }
                message = read.try_next() => {
                    let Some(message) = message.map_err(|error| {
                        DanmuStreamError::WebsocketError {
                            err: format!("Failed to read message: {error}"),
                        }
                    })? else {
                        info!("Douyin WebSocket stream ended");
                        break;
                    };

                    match message {
                        WsMessage::Binary(data) => {
                            let (ack, events) = decode_binary_message(&data, &self.room_id)?;
                            // Hand every decoded event to the bounded recorder
                            // queue before acknowledging the frame. During
                            // backpressure `forward_event` keeps the connection
                            // alive with heartbeats, and a crash leaves the frame
                            // unacknowledged so Douyin may replay it on reconnect.
                            let has_events = !events.is_empty();
                            for event in events {
                                self.forward_event(&tx, event, &mut heartbeat).await?;
                            }
                            if ack.is_some() && has_events {
                                self.wait_for_persistence(&tx, &mut heartbeat).await?;
                            }
                            if let Some(ack) = ack {
                                if *self.stop.read().await {
                                    // The frame was already received before
                                    // shutdown. Persist it, but do not ACK a
                                    // connection which is being closed.
                                    info!("Stopping douyin danmu stream after final frame");
                                    break;
                                } else {
                                    self.send_ws_message(
                                        WsMessage::binary(ack.encode_to_vec()),
                                        "ack",
                                    )
                                    .await?;
                                }
                            }
                        }
                        WsMessage::Close(_) => {
                            info!("WebSocket connection closed");
                            break;
                        }
                        WsMessage::Ping(data) => {
                            self.send_ws_message(WsMessage::Pong(data), "pong").await?;
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    fn heartbeat_message() -> WsMessage {
        WsMessage::binary(HEARTBEAT_PAYLOAD.to_vec())
    }

    async fn send_ws_message(
        &self,
        message: WsMessage,
        message_type: &str,
    ) -> Result<(), DanmuStreamError> {
        let mut write = self.write.write().await;
        let write = write
            .as_mut()
            .ok_or_else(|| DanmuStreamError::WebsocketError {
                err: format!("Cannot send douyin {message_type}: websocket writer is unavailable"),
            })?;
        match tokio::time::timeout(WEBSOCKET_WRITE_TIMEOUT, write.send(message)).await {
            Ok(result) => result.map_err(|error| DanmuStreamError::WebsocketError {
                err: format!("Failed to send douyin {message_type}: {error}"),
            }),
            Err(_) => Err(DanmuStreamError::WebsocketError {
                err: format!(
                    "Timed out sending douyin {message_type} after {} seconds",
                    WEBSOCKET_WRITE_TIMEOUT.as_secs()
                ),
            }),
        }
    }

    async fn forward_event(
        &self,
        tx: &mpsc::Sender<DanmuMessageType>,
        event: DanmuMessageType,
        heartbeat: &mut tokio::time::Interval,
    ) -> Result<(), DanmuStreamError> {
        let send = tx.send(event);
        tokio::pin!(send);
        loop {
            tokio::select! {
                result = &mut send => {
                    return result.map_err(|error| DanmuStreamError::WebsocketError {
                        err: format!("Failed to send message to channel: {error}"),
                    });
                }
                _ = heartbeat.tick() => {
                    // Once graceful shutdown starts, keep waiting for recorder
                    // capacity instead of failing this accepted event merely
                    // because the websocket writer was closed by `stop`.
                    if !*self.stop.read().await {
                        self.send_ws_message(Self::heartbeat_message(), "heartbeat").await?;
                    }
                }
            }
        }
    }

    async fn wait_for_persistence(
        &self,
        tx: &mpsc::Sender<DanmuMessageType>,
        heartbeat: &mut tokio::time::Interval,
    ) -> Result<(), DanmuStreamError> {
        let (persisted_tx, persisted_rx) = tokio::sync::oneshot::channel();
        self.forward_event(
            tx,
            DanmuMessageType::PersistBarrier(persisted_tx),
            heartbeat,
        )
        .await?;
        tokio::pin!(persisted_rx);
        loop {
            tokio::select! {
                result = &mut persisted_rx => {
                    return match result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(error)) => Err(DanmuStreamError::WebsocketError {
                            err: format!("Douyin event persistence failed before ACK: {error}"),
                        }),
                        Err(error) => Err(DanmuStreamError::WebsocketError {
                            err: format!("Douyin persistence barrier was dropped before ACK: {error}"),
                        }),
                    };
                }
                _ = heartbeat.tick() => {
                    if !*self.stop.read().await {
                        self.send_ws_message(Self::heartbeat_message(), "heartbeat").await?;
                    }
                }
            }
        }
    }
}

fn decode_binary_message(
    data: &[u8],
    room_id: &str,
) -> Result<(Option<PushFrame>, Vec<DanmuMessageType>), DanmuStreamError> {
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

    let ack = build_ack(&push_frame, &response);
    let mut events = Vec::new();

    for message in response.messages_list {
        match message.method.as_str() {
            "WebcastChatMessage" => {
                let chat_msg = match DouyinChatMessage::decode(message.payload.as_slice()) {
                    Ok(chat_msg) => chat_msg,
                    Err(error) => {
                        error!("Skipping malformed Douyin chat message: {error}");
                        continue;
                    }
                };
                let event = douyin_chat_event(
                    room_id,
                    &message.method,
                    message.msg_id,
                    &message.payload,
                    chat_msg,
                );
                debug!("Received danmu event: {:?}", event);
                events.push(DanmuMessageType::Event(event));
            }
            "WebcastGiftMessage" => {
                let gift_msg = match GiftMessage::decode(message.payload.as_slice()) {
                    Ok(gift_msg) => gift_msg,
                    Err(error) => {
                        error!("Skipping malformed Douyin gift message: {error}");
                        continue;
                    }
                };
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
                        events.push(DanmuMessageType::Event(event));
                    }
                }
            }
            "WebcastLikeMessage" => {
                let like_msg = match LikeMessage::decode(message.payload.as_slice()) {
                    Ok(like_msg) => like_msg,
                    Err(error) => {
                        error!("Skipping malformed Douyin like message: {error}");
                        continue;
                    }
                };
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
                    events.push(DanmuMessageType::Event(event));
                }
            }
            "WebcastMemberMessage" => {
                let member_msg = match MemberMessage::decode(message.payload.as_slice()) {
                    Ok(member_msg) => member_msg,
                    Err(error) => {
                        error!("Skipping malformed Douyin member message: {error}");
                        continue;
                    }
                };
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
                    events.push(DanmuMessageType::Event(event));
                }
            }
            _ => {
                debug!("Unknown message: {:?}", message);
            }
        }
    }

    Ok((ack, events))
}

fn build_ack(push_frame: &PushFrame, response: &Response) -> Option<PushFrame> {
    response.need_ack.then(|| PushFrame {
        payload_type: "ack".to_string(),
        log_id: push_frame.log_id,
        payload_encoding: "".to_string(),
        // Douyin uses internal_ext as the cursor acknowledged by the client.
        // Sending an empty payload leaves the webcast service unacknowledged.
        payload: response.internal_ext.as_bytes().to_vec(),
        seq_id: 0,
        service: 0,
        method: 0,
        headers_list: vec![],
    })
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
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write as _;

    fn test_provider(cookie: &str) -> DouyinDanmu {
        DouyinDanmu {
            room_id: "123".to_string(),
            cookie: cookie.to_string(),
            stop: Arc::new(RwLock::new(false)),
            write: Arc::new(RwLock::new(None)),
        }
    }

    #[test]
    fn websocket_request_uses_the_signed_url_host() {
        let provider = test_provider("sessionid=test");
        let request = provider
            .websocket_request(
                "wss://webcast5-ws-web-lf.douyin.com/webcast/im/push/v2/?room_id=123",
            )
            .unwrap();

        assert_eq!(
            request.headers().get(header::HOST).unwrap(),
            "webcast5-ws-web-lf.douyin.com"
        );
        assert_eq!(
            request.headers().get(header::COOKIE).unwrap(),
            "sessionid=test"
        );
        assert!(request.headers().contains_key(header::SEC_WEBSOCKET_KEY));
    }

    #[test]
    fn heartbeat_uses_the_douyin_wire_payload() {
        assert_eq!(
            DouyinDanmu::heartbeat_message().into_data().as_ref(),
            HEARTBEAT_PAYLOAD
        );
    }

    #[test]
    fn ack_carries_internal_extension_cursor() {
        let push_frame = PushFrame {
            log_id: 42,
            ..Default::default()
        };
        let response = Response {
            need_ack: true,
            internal_ext: "cursor=next".to_string(),
            ..Default::default()
        };

        let ack = build_ack(&push_frame, &response).unwrap();
        assert_eq!(ack.payload_type, "ack");
        assert_eq!(ack.log_id, 42);
        assert_eq!(ack.payload, b"cursor=next");
    }

    #[test]
    fn ack_is_omitted_when_server_does_not_request_it() {
        let push_frame = PushFrame::default();
        let response = Response::default();

        assert!(build_ack(&push_frame, &response).is_none());
    }

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

    #[test]
    fn malformed_child_message_does_not_poison_valid_chat_or_frame_ack() {
        let chat = DouyinChatMessage {
            content: "valid chat".to_string(),
            event_time: 1_788_868_414_771,
            ..Default::default()
        };
        let response = Response {
            messages_list: vec![
                CommonMessage {
                    method: "WebcastGiftMessage".to_string(),
                    payload: vec![0xff],
                    ..Default::default()
                },
                CommonMessage {
                    method: "WebcastChatMessage".to_string(),
                    payload: chat.encode_to_vec(),
                    msg_id: 42,
                    ..Default::default()
                },
            ],
            internal_ext: "cursor-after-frame".to_string(),
            need_ack: true,
            ..Default::default()
        };
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&response.encode_to_vec()).unwrap();
        let frame = PushFrame {
            log_id: 7,
            payload: encoder.finish().unwrap(),
            ..Default::default()
        };

        let (ack, events) = decode_binary_message(&frame.encode_to_vec(), "room").unwrap();

        assert_eq!(ack.unwrap().payload, b"cursor-after-frame");
        assert_eq!(events.len(), 1);
        let DanmuMessageType::Event(event) = &events[0] else {
            panic!("expected valid chat event")
        };
        assert_eq!(event.data["content"], "valid chat");
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

    async fn start(&self, tx: mpsc::Sender<DanmuMessageType>) -> Result<(), DanmuStreamError> {
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

            if *self.stop.read().await {
                break;
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
            match tokio::time::timeout(WEBSOCKET_WRITE_TIMEOUT, write.close()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => error!("Failed to close WebSocket connection: {error}"),
                Err(_) => error!(
                    "Timed out closing Douyin WebSocket after {} seconds",
                    WEBSOCKET_WRITE_TIMEOUT.as_secs()
                ),
            }
        }
        Ok(())
    }
}
