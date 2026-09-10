import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { extname } from "node:path";
import test from "node:test";

const apiSource = await readFile(
  new URL("../../src-tauri/src/http_server/api_server.rs", import.meta.url),
  "utf8",
);
const mainSource = await readFile(
  new URL("../../src-tauri/src/main.rs", import.meta.url),
  "utf8",
);
const handlerModulesSource = await readFile(
  new URL("../../src-tauri/src/handlers/mod.rs", import.meta.url),
  "utf8",
);
const recorderManagerSource = await readFile(
  new URL("../../src-tauri/src/recorder_manager.rs", import.meta.url),
  "utf8",
);
const recorderHandlerSource = await readFile(
  new URL("../../src-tauri/src/handlers/recorder.rs", import.meta.url),
  "utf8",
);
const videoEditingSource = await readFile(
  new URL("../../src-tauri/src/handlers/video_editing.rs", import.meta.url),
  "utf8",
);
const toolSource = await readFile(
  new URL("../../src/lib/agent/tools.ts", import.meta.url),
  "utf8",
);
const settingSource = await readFile(
  new URL("../../src/page/Setting.svelte", import.meta.url),
  "utf8",
);
const playerSource = await readFile(
  new URL("../../src/lib/components/Player.svelte", import.meta.url),
  "utf8",
);
const douyinProviderSource = await readFile(
  new URL(
    "../../src-tauri/crates/danmu_stream/src/provider/douyin.rs",
    import.meta.url,
  ),
  "utf8",
);
const douyinRecorderSource = await readFile(
  new URL(
    "../../src-tauri/crates/recorder/src/platforms/douyin.rs",
    import.meta.url,
  ),
  "utf8",
);

async function collectFrontendSources(directory) {
  const sources = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const url = new URL(`${entry.name}${entry.isDirectory() ? "/" : ""}`, directory);
    if (entry.isDirectory()) {
      sources.push(...(await collectFrontendSources(url)));
    } else if ([".ts", ".svelte"].includes(extname(entry.name))) {
      sources.push(await readFile(url, "utf8"));
    }
  }
  return sources;
}

test("every confirmed agent tool has a Docker API or browser fallback", () => {
  const invokedCommands = new Set(
    [...toolSource.matchAll(/invoke(?:<[^>]+>)?\s*\(\s*["']([\w]+)["']/g)].map(
      (match) => match[1],
    ),
  );
  const apiCommands = new Set(
    [...apiSource.matchAll(/["']\/api\/([\w]+)["']/g)].map(
      (match) => match[1],
    ),
  );
  const browserFallbacks = new Set(["open_clip"]);
  const missing = [...invokedCommands].filter(
    (command) => !apiCommands.has(command) && !browserFallbacks.has(command),
  );

  assert.deepEqual(missing, []);
});

test("every portable desktop command has a same-name Docker API", () => {
  const invokeHandlerBlock = mainSource.match(/generate_handler!\[([\s\S]*?)\]\)/)?.[1] ?? "";
  const desktopCommands = new Set(
    [...invokeHandlerBlock.matchAll(/crate::[\w:]+::([\w]+),/g)].map(
      (match) => match[1],
    ),
  );
  const apiCommands = new Set(
    [...apiSource.matchAll(/["']\/api\/([\w]+)["']/g)].map(
      (match) => match[1],
    ),
  );
  const desktopOnlyOrReplaced = new Set([
    "export_to_file",
    "fetch_hls",
    "file_exists",
    "open_clip",
    "open_live",
    "open_log_folder",
    "show_in_folder",
  ]);
  const missing = [...desktopCommands].filter(
    (command) => !apiCommands.has(command) && !desktopOnlyOrReplaced.has(command),
  );

  assert.deepEqual(missing, []);
});

test("every frontend command has a Docker API or an explicit web fallback", async () => {
  const frontendSources = (await collectFrontendSources(
    new URL("../../src/", import.meta.url),
  )).map((source) => source.replace(/^\s*\/\/.*$/gm, ""));
  const invokedCommands = new Set(
    frontendSources.flatMap((source) =>
      [...source.matchAll(/invoke(?:<[^>]+>)?\s*\(\s*["']([\w]+)["']/g)].map(
        (match) => match[1],
      ),
    ),
  );
  const apiCommands = new Set(
    [...apiSource.matchAll(/["']\/api\/([\w]+)["']/g)].map(
      (match) => match[1],
    ),
  );
  const explicitWebFallbacks = new Set([
    "export_to_file",
    "fetch_hls",
    "open_clip",
    "open_live",
    "open_log_folder",
  ]);
  const missing = [...invokedCommands].filter(
    (command) => !apiCommands.has(command) && !explicitWebFallbacks.has(command),
  );

  assert.deepEqual(missing, []);
});

test("Docker settings remain visible with a same-origin endpoint", () => {
  assert.doesNotMatch(settingSource, /TAURI_ENV\s*\|\|\s*endpoint\s*!==?\s*["']{2}/);
  assert.match(settingSource, /updateContainerCachePath/);
  assert.match(settingSource, /updateContainerOutputPath/);
});

test("headless-only API dependencies and live notifications are wired", () => {
  assert.doesNotMatch(
    handlerModulesSource,
    /#\[cfg\(feature = "gui"\)\]\s*pub mod video_editing/,
  );
  assert.match(
    recorderManagerSource,
    /\.emit\(&RecorderEvent::LiveStart \{ recorder \}\);/,
  );
  assert.match(
    recorderManagerSource,
    /self\.emitter\.emit\(&RecorderEvent::LiveEnd \{/,
  );
  assert.match(videoEditingSource, /\.join\("playlist\.m3u8"\)/);
  assert.doesNotMatch(videoEditingSource, /\.join\("output\.mp4"\)/);
  assert.match(recorderHandlerSource, /offset:\s*f64/);
  assert.match(recorderHandlerSource, /local_offset:\s*f64/);
});

test("Docker streams complete Douyin danmu without JSON response buffering", () => {
  assert.match(
    apiSource,
    /route\("\/api\/export_danmu_file",\s*get\(handler_export_danmu_file\)\)/,
  );
  assert.match(apiSource, /Body::from_stream\(stream\)/);
  assert.match(
    recorderHandlerSource,
    /Docker 完整弹幕请使用流式 GET \/api\/export_danmu_file/,
  );
  assert.match(playerSource, /format === "jsonl"/);
  assert.match(playerSource, /\/api\/export_danmu_file\?/);
  assert.match(playerSource, /method: "HEAD"/);
  assert.match(playerSource, /if \(!preflight\.ok\)/);
});

test("Douyin websocket sends heartbeat and ACK data over the real connection", () => {
  assert.doesNotMatch(douyinProviderSource, /mut _rx_write/);
  assert.doesNotMatch(douyinProviderSource, /webcast5-ws-web-hl\.douyin\.com/);
  assert.match(
    douyinProviderSource,
    /send\(DouyinDanmu::heartbeat_message\(\)\)\.await/,
  );
  assert.match(
    douyinProviderSource,
    /WsMessage::Ping\(data\)[\s\S]*?send\(WsMessage::Pong\(data\)\)\.await/,
  );
  assert.match(douyinProviderSource, /MAX_PENDING_FRAMES/);
  assert.match(douyinProviderSource, /MAX_PENDING_FRAME_BYTES/);
  assert.match(
    douyinProviderSource,
    /payload:\s*response\.internal_ext\.as_bytes\(\)\.to_vec\(\)/,
  );
  assert.match(douyinProviderSource, /let \(ack, events\) = decode_binary_message/);
  assert.match(
    douyinProviderSource,
    /for event in events[\s\S]*?DanmuMessageType::PersistBarrier/,
  );
  assert.match(
    douyinProviderSource,
    /result = async \{ active\.as_mut\(\)\.unwrap\(\)\.await \}[\s\S]*?if let Some\(ack\) = result\?[\s\S]*?send\(WsMessage::binary\(ack\.encode_to_vec\(\)\)\)/,
  );
  assert.match(douyinProviderSource, /pump_connection/);
  assert.match(douyinProviderSource, /persist_frame/);
  assert.match(douyinProviderSource, /DanmuMessageType::PersistBarrier/);
});

test("Douyin realtime danmu cannot displace lifecycle events", () => {
  assert.match(douyinRecorderSource, /realtime_event_sink/);
  assert.match(
    recorderManagerSource,
    /let realtime_event_sink = Arc::new\(move \|event: RecorderEvent\|/,
  );
});

test("Douyin Docker recordings fail visibly and recover live sessions after restart", () => {
  assert.match(douyinRecorderSource, /DOUYIN_ACTIVE_SESSION_FILE/);
  assert.match(
    douyinRecorderSource,
    /try_load_active_session\(&cache_dir, room_id\)\.await\?/,
  );
  assert.match(douyinRecorderSource, /struct ActiveSessionMarker/);
  assert.match(douyinRecorderSource, /new_incarnation_token/);
  assert.match(douyinRecorderSource, /platform_session_id/);
  assert.match(douyinRecorderSource, /live_start_pending/);
  assert.match(douyinRecorderSource, /emit_live_end_and_reset/);
  assert.match(douyinRecorderSource, /DOUYIN_DANMU_SHUTDOWN_TIMEOUT/);
  assert.match(douyinRecorderSource, /session_transition/);
  assert.match(recorderManagerSource, /acknowledge_live_end/);
  assert.match(recorderManagerSource, /DOUYIN_WHOLE_COMPLETIONS_DIR/);
  assert.match(recorderManagerSource, /douyin_whole_output_name/);
  assert.match(recorderManagerSource, /\.partial\.mp4/);
  assert.match(
    recorderManagerSource,
    /douyin_playlist_has_local_media_segment/,
  );
  assert.match(
    douyinRecorderSource,
    /let Some\(danmu_storage\) = DanmuStorage::new\(&danmu_path\)\.await else/,
  );
  assert.match(douyinRecorderSource, /抖音弹幕保存失败/);
});
