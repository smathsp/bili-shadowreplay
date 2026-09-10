use crate::account::Account;
use crate::errors::RecorderError;
use crate::utils::user_agent_generator;
use deno_core::JsRuntime;
use deno_core::RuntimeOptions;
use regex::Regex;
use reqwest::Client;
use uuid::Uuid;

use super::response::DouyinRoomInfoResponse;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct DouyinBasicRoomInfo {
    pub room_id_str: String,
    pub room_title: String,
    pub cover: Option<String>,
    pub status: i64,
    pub hls_url: String,
    pub stream_data: String,
    // user related
    pub user_name: String,
    pub user_avatar: String,
    pub sec_user_id: String,
}

fn setup_js_runtime() -> Result<JsRuntime, RecorderError> {
    // Create a new V8 runtime
    let mut runtime = JsRuntime::new(RuntimeOptions::default());

    // Add global CryptoJS object
    let crypto_js = include_str!("js/a_bogus.js");
    runtime
        .execute_script(
            "<a_bogus.js>",
            deno_core::FastString::from_static(crypto_js),
        )
        .map_err(|e| RecorderError::JsRuntimeError(format!("Failed to execute crypto-js: {e}")))?;
    Ok(runtime)
}

async fn generate_a_bogus(params: &str, user_agent: &str) -> Result<String, RecorderError> {
    let mut runtime = setup_js_runtime()?;
    // Call the get_wss_url function
    let sign_call = format!("generate_a_bogus(\"{params}\", \"{user_agent}\")");
    let result = runtime
        .execute_script("<sign_call>", deno_core::FastString::from(sign_call))
        .map_err(|e| RecorderError::JsRuntimeError(format!("Failed to execute JavaScript: {e}")))?;

    // Get the result from the V8 runtime
    let mut scope = runtime.handle_scope();
    let local = deno_core::v8::Local::new(&mut scope, result);
    let url = local
        .to_string(&mut scope)
        .unwrap()
        .to_rust_string_lossy(&mut scope);
    Ok(url)
}

async fn generate_ms_token() -> String {
    // generate a random 32 characters uuid string
    let uuid = Uuid::new_v4();
    uuid.to_string()
}

pub fn generate_user_agent_header() -> reqwest::header::HeaderMap {
    let user_agent = user_agent_generator::UserAgentGenerator::new().generate(false);
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("user-agent", user_agent.parse().unwrap());
    headers
}

fn insert_cookie_header(
    headers: &mut reqwest::header::HeaderMap,
    cookies: &str,
) -> Result<(), RecorderError> {
    headers.insert(
        reqwest::header::COOKIE,
        cookies.parse().map_err(|error| RecorderError::ApiError {
            error: format!("Invalid douyin cookie header: {error}"),
        })?,
    );
    Ok(())
}

fn parse_web_room_info_response(
    data: DouyinRoomInfoResponse,
    room_id: &str,
    sec_user_id: &str,
) -> Result<DouyinBasicRoomInfo, RecorderError> {
    // Douyin returns HTTP 200 for some authentication and risk-control errors.
    // Never interpret those payloads as an offline room: doing so would close
    // the current live session and trigger a premature whole-session export.
    if data.status_code != 0 {
        return Err(RecorderError::ApiError {
            error: format!(
                "Douyin web room API returned status_code {}",
                data.status_code
            ),
        });
    }

    // The Web enter API legitimately returns an empty room list after a live
    // ends. Treat that as an offline status instead of forcing the less stable
    // H5 fallback; otherwise a failed fallback keeps the recorder stuck in its
    // previous `live` state and LiveEnd is never emitted.
    if data.data.room_status != 0 && data.data.data.is_empty() {
        return Ok(DouyinBasicRoomInfo {
            room_id_str: room_id.to_string(),
            room_title: String::new(),
            cover: None,
            status: data.data.room_status,
            hls_url: String::new(),
            stream_data: String::new(),
            user_name: data.data.user.nickname,
            user_avatar: data
                .data
                .user
                .avatar_thumb
                .url_list
                .first()
                .cloned()
                .unwrap_or_default(),
            sec_user_id: sec_user_id.to_string(),
        });
    }

    let room = data
        .data
        .data
        .first()
        .ok_or_else(|| RecorderError::ApiError {
            error: "Douyin room info response did not include room data".to_string(),
        })?;

    let cover = room
        .cover
        .as_ref()
        .and_then(|cover| cover.url_list.first().cloned());
    let user_avatar = data
        .data
        .user
        .avatar_thumb
        .url_list
        .first()
        .cloned()
        .unwrap_or_default();
    let owner_sec_user_id = room
        .owner
        .as_ref()
        .map(|owner| owner.sec_uid.as_str())
        .filter(|value| !value.is_empty())
        .or_else(|| (!data.data.user.sec_uid.is_empty()).then_some(data.data.user.sec_uid.as_str()))
        .unwrap_or(sec_user_id)
        .to_string();

    Ok(DouyinBasicRoomInfo {
        room_id_str: room.id_str.clone(),
        sec_user_id: owner_sec_user_id,
        cover,
        room_title: room.title.clone(),
        user_name: data.data.user.nickname.clone(),
        user_avatar,
        status: data.data.room_status,
        hls_url: room
            .stream_url
            .as_ref()
            .map(|stream_url| stream_url.hls_pull_url.clone())
            .unwrap_or_default(),
        stream_data: room
            .stream_url
            .as_ref()
            .map(|s| s.live_core_sdk_data.pull_data.stream_data.clone())
            .unwrap_or_default(),
    })
}

pub async fn get_room_info(
    client: &Client,
    account: &Account,
    room_id: &str,
    sec_user_id: &str,
) -> Result<DouyinBasicRoomInfo, RecorderError> {
    let mut headers = generate_user_agent_header();
    headers.insert("Referer", "https://live.douyin.com/".parse().unwrap());
    insert_cookie_header(&mut headers, &account.cookies)?;
    let ms_token = generate_ms_token().await;
    let user_agent = headers.get("user-agent").unwrap().to_str().unwrap();
    let params = format!(
            "aid=6383&app_name=douyin_web&live_id=1&device_platform=web&language=zh-CN&enter_from=web_live&cookie_enabled=true&screen_width=1920&screen_height=1080&browser_language=zh-CN&browser_platform=MacIntel&browser_name=Chrome&browser_version=122.0.0.0&web_rid={room_id}&ms_token={ms_token}");
    let a_bogus = generate_a_bogus(&params, user_agent).await?;
    // log::debug!("params: {params}");
    // log::debug!("user_agent: {user_agent}");
    // log::debug!("a_bogus: {a_bogus}");
    let url = format!(
            "https://live.douyin.com/webcast/room/web/enter/?aid=6383&app_name=douyin_web&live_id=1&device_platform=web&language=zh-CN&enter_from=web_live&cookie_enabled=true&screen_width=1920&screen_height=1080&browser_language=zh-CN&browser_platform=MacIntel&browser_name=Chrome&browser_version=122.0.0.0&web_rid={room_id}&ms_token={ms_token}&a_bogus={a_bogus}"
        );

    let resp = client.get(&url).headers(headers).send().await?;

    let status = resp.status();
    let text = resp.text().await?;

    if text.is_empty() {
        log::debug!("Empty room info response, trying H5 API");
        return get_room_info_h5(client, account, room_id, sec_user_id).await;
    }

    if status.is_success() {
        if let Ok(data) = serde_json::from_str::<DouyinRoomInfoResponse>(&text) {
            match parse_web_room_info_response(data, room_id, sec_user_id) {
                Ok(info) => return Ok(info),
                Err(e) => {
                    log::warn!("Invalid douyin room info response: {e}; trying H5 API");
                    return get_room_info_h5(client, account, room_id, sec_user_id).await;
                }
            }
        }
        log::error!("Failed to parse room info response: {text}");
        return get_room_info_h5(client, account, room_id, sec_user_id).await;
    }

    log::error!("Failed to get room info: {status}");
    return get_room_info_h5(client, account, room_id, sec_user_id).await;
}

pub async fn get_room_info_h5(
    client: &Client,
    account: &Account,
    room_id: &str,
    sec_user_id: &str,
) -> Result<DouyinBasicRoomInfo, RecorderError> {
    // 参考biliup实现，构建完整的URL参数
    let room_id_str = room_id.to_string();
    // https://webcast.amemv.com/webcast/room/reflow/info/?type_id=0&live_id=1&version_code=99.99.99&app_id=1128&room_id=10000&sec_user_id=MS4wLjAB&aid=6383&device_platform=web&browser_language=zh-CN&browser_platform=Win32&browser_name=Mozilla&browser_version=5.0
    let url_params = [
        ("type_id", "0"),
        ("live_id", "1"),
        ("version_code", "99.99.99"),
        ("app_id", "1128"),
        ("room_id", &room_id_str),
        ("sec_user_id", sec_user_id),
        ("aid", "6383"),
        ("device_platform", "web"),
    ];

    // 构建URL
    let query_string = url_params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let url = format!("https://webcast.amemv.com/webcast/room/reflow/info/?{query_string}");

    let mut headers = generate_user_agent_header();
    headers.insert("Referer", "https://live.douyin.com/".parse().unwrap());
    insert_cookie_header(&mut headers, &account.cookies)?;

    let resp = client.get(&url).headers(headers).send().await?;

    let status = resp.status();
    let text = resp.text().await?;

    if status.is_success() {
        // Try to parse as H5 response format
        if let Ok(h5_data) =
            serde_json::from_str::<super::response::DouyinH5RoomInfoResponse>(&text)
        {
            // Extract RoomBasicInfo from H5 response
            let room = &h5_data.data.room;
            let owner = &room.owner;

            let cover = room
                .cover
                .as_ref()
                .and_then(|c| c.url_list.first().cloned());
            let hls_url = room
                .stream_url
                .as_ref()
                .map(|s| s.hls_pull_url.clone())
                .unwrap_or_default();

            return Ok(DouyinBasicRoomInfo {
                room_id_str: room.id_str.clone(),
                room_title: room.title.clone(),
                cover,
                status: if room.status == 2 { 0 } else { 1 },
                hls_url,
                user_name: owner.nickname.clone(),
                user_avatar: owner
                    .avatar_thumb
                    .url_list
                    .first()
                    .cloned()
                    .unwrap_or_default(),
                sec_user_id: owner.sec_uid.clone(),
                stream_data: room
                    .stream_url
                    .as_ref()
                    .map(|s| s.live_core_sdk_data.pull_data.stream_data.clone())
                    .unwrap_or_default(),
            });
        }

        // If that fails, try to parse as a generic JSON to see what we got
        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&text) {
            // Check if it's an error response
            if let Some(status_code) = json_value
                .get("status_code")
                .and_then(serde_json::Value::as_i64)
            {
                if status_code != 0 {
                    let error_msg = json_value
                        .get("data")
                        .and_then(|v| v.get("message").and_then(|v| v.as_str()))
                        .unwrap_or("Unknown error");

                    if status_code == 10011 {
                        return Err(RecorderError::ApiError {
                            error: error_msg.to_string(),
                        });
                    }

                    return Err(RecorderError::ApiError {
                        error: format!(
                            "API returned error status_code: {status_code} - {error_msg}"
                        ),
                    });
                }
            }

            // 检查是否是"invalid session"错误
            if let Some(status_message) = json_value.get("status_message").and_then(|v| v.as_str())
            {
                if status_message.contains("invalid session") {
                    return Err(RecorderError::ApiError { error:
                            "Invalid session - please check your cookies. Make sure you have valid sessionid, passport_csrf_token, and other authentication cookies from douyin.com".to_string(),
                        });
                }
            }

            return Err(RecorderError::ApiError {
                error: format!("Failed to parse h5 room info response: {text}"),
            });
        }
        log::error!("Failed to parse h5 room info response: {text}");
        return Err(RecorderError::ApiError {
            error: format!("Failed to parse h5 room info response: {text}"),
        });
    }

    log::error!("Failed to get h5 room info: {status}");
    Err(RecorderError::ApiError {
        error: format!("Failed to get h5 room info: {status} {text}"),
    })
}

pub async fn get_user_info(
    client: &Client,
    account: &Account,
) -> Result<super::response::User, RecorderError> {
    // Use the IM spotlight relation API to get user info
    let url = "https://www.douyin.com/aweme/v1/web/im/spotlight/relation/";
    let mut headers = generate_user_agent_header();
    headers.insert("Referer", "https://www.douyin.com/".parse().unwrap());
    insert_cookie_header(&mut headers, &account.cookies)?;

    let resp = client.get(url).headers(headers).send().await?;

    let status = resp.status();
    let text = resp.text().await?;

    if status.is_success() {
        if let Ok(data) = serde_json::from_str::<super::response::DouyinRelationResponse>(&text) {
            if data.status_code == 0 {
                let owner_sec_uid = &data.owner_sec_uid;

                // Find the user's own info in the followings list by matching sec_uid
                if let Some(followings) = &data.followings {
                    for following in followings {
                        if following.sec_uid == *owner_sec_uid {
                            let user = super::response::User {
                                id_str: following.uid.clone(),
                                sec_uid: following.sec_uid.clone(),
                                nickname: following.nickname.clone(),
                                avatar_thumb: following.avatar_thumb.clone(),
                                follow_info: super::response::FollowInfo::default(),
                                foreign_user: 0,
                                open_id_str: String::new(),
                            };
                            return Ok(user);
                        }
                    }
                }

                // If not found in followings, create a minimal user info from owner_sec_uid
                let user = super::response::User {
                    id_str: String::new(), // We don't have the numeric UID
                    sec_uid: owner_sec_uid.clone(),
                    nickname: "抖音用户".to_string(), // Default nickname
                    avatar_thumb: super::response::AvatarThumb { url_list: vec![] },
                    follow_info: super::response::FollowInfo::default(),
                    foreign_user: 0,
                    open_id_str: String::new(),
                };
                return Ok(user);
            }
        } else {
            log::error!("Failed to parse user info response: {text}");
            return Err(RecorderError::ApiError {
                error: format!("Failed to parse user info response: {text}"),
            });
        }
    }

    log::error!("Failed to get user info: {status}");

    Err(RecorderError::ApiError {
        error: format!("Failed to get user info: {status} {text}"),
    })
}

pub async fn get_room_owner_sec_uid(
    client: &Client,
    account: &Account,
    room_id: &str,
) -> Result<String, RecorderError> {
    // Prefer the structured room response, which ties the owner to the
    // requested web_rid. The HTML regex remains only as a compatibility
    // fallback for offline or changed API responses.
    if let Ok(info) = get_room_info(client, account, room_id, "").await {
        if !info.sec_user_id.is_empty() {
            return Ok(info.sec_user_id);
        }
    }

    let url = format!("https://live.douyin.com/{room_id}");
    let mut headers = generate_user_agent_header();
    headers.insert("Referer", "https://live.douyin.com/".parse().unwrap());
    if !account.cookies.is_empty() {
        insert_cookie_header(&mut headers, &account.cookies)?;
    }
    let resp = client.get(url).headers(headers).send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(RecorderError::ApiError {
            error: format!("Failed to get room owner sec uid: {status} {text}"),
        });
    }
    // match to get sec_uid from text like \"sec_uid\":\"MS4wLjABAAAAdFmmud36bynPjXOvoMjatb42856_zryHsGmlkpIECDA\"
    let sec_uid = Regex::new(r#"\\"sec_uid\\":\\"(.*?)\\""#)
        .unwrap()
        .captures(&text)
        .and_then(|c| c.get(1))
        .ok_or_else(|| RecorderError::ApiError {
            error: "Failed to find sec_uid in room page".to_string(),
        })?
        .as_str()
        .to_string();
    Ok(sec_uid)
}

/// Download file from url to path
pub async fn download_file(client: &Client, url: &str, path: &Path) -> Result<(), RecorderError> {
    if !path.parent().unwrap().exists() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    }
    let response = client.get(url).send().await?;
    let bytes = response.bytes().await?;
    let mut file = tokio::fs::File::create(&path).await?;
    let mut content = std::io::Cursor::new(bytes);
    tokio::io::copy(&mut content, &mut file).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platforms::douyin::response;

    fn web_room_response_with_room(
        room: Option<response::Daum>,
        room_status: i64,
    ) -> DouyinRoomInfoResponse {
        DouyinRoomInfoResponse {
            data: response::Data {
                data: room.into_iter().collect(),
                room_status,
                user: response::User {
                    nickname: "主播".to_string(),
                    avatar_thumb: response::AvatarThumb { url_list: vec![] },
                    ..Default::default()
                },
                ..Default::default()
            },
            status_code: 0,
            ..Default::default()
        }
    }

    #[test]
    fn parse_web_room_info_response_accepts_offline_empty_room_data() {
        let info = parse_web_room_info_response(
            web_room_response_with_room(None, 1),
            "requested-room",
            "sec_uid",
        )
        .expect("offline empty room data should be accepted");

        assert_eq!(info.status, 1);
        assert_eq!(info.room_id_str, "requested-room");
        assert!(info.hls_url.is_empty());
    }

    #[test]
    fn parse_web_room_info_response_rejects_live_empty_room_data() {
        let err = parse_web_room_info_response(
            web_room_response_with_room(None, 0),
            "requested-room",
            "sec_uid",
        )
        .expect_err("live empty room data should be rejected");

        assert!(matches!(err, RecorderError::ApiError { .. }));
    }

    #[test]
    fn parse_web_room_info_response_rejects_api_error_as_offline() {
        let mut response = web_room_response_with_room(None, 1);
        response.status_code = 10011;

        let error = parse_web_room_info_response(response, "requested-room", "sec_uid")
            .expect_err("API errors must not be treated as an ended live");

        assert!(error.to_string().contains("status_code 10011"));
    }

    #[test]
    fn parse_web_room_info_response_allows_empty_image_lists() {
        let room = response::Daum {
            id_str: "room_id".to_string(),
            title: "直播标题".to_string(),
            cover: Some(response::Cover { url_list: vec![] }),
            ..Default::default()
        };

        let info = parse_web_room_info_response(
            web_room_response_with_room(Some(room), 0),
            "requested-room",
            "sec_uid",
        )
        .expect("empty image url lists should not fail room parsing");

        assert_eq!(info.room_id_str, "room_id");
        assert_eq!(info.room_title, "直播标题");
        assert_eq!(info.cover, None);
        assert_eq!(info.user_avatar, "");
    }

    #[tokio::test]
    async fn test_get_room_owner_sec_uid() {
        let client = Client::new();
        let sec_uid = get_room_owner_sec_uid(&client, &Account::default(), "200525029536")
            .await
            .unwrap();
        assert_eq!(
            sec_uid,
            "MS4wLjABAAAAdFmmud36bynPjXOvoMjatb42856_zryHsGmlkpIECDA"
        );
    }
}
