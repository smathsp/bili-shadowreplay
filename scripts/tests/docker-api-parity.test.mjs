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
