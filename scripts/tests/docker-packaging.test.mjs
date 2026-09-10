import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function read(relativePath) {
  return readFile(new URL(`../../${relativePath}`, import.meta.url), "utf8");
}

const [dockerfile, dockerignore, compose, gpuCompose, workflow, dockerDocs] =
  await Promise.all([
    read("Dockerfile"),
    read(".dockerignore"),
    read("docker_compose.yaml"),
    read("docker_compose.gpu.yaml"),
    read(".github/workflows/package.yml"),
    read("docs/getting-started/installation/docker.md"),
  ]);

test("runtime image contains required assets and health tooling", () => {
  assert.match(dockerfile, /COPY .*silero_vad\.onnx .*silero_vad\.onnx/);
  assert.match(dockerfile, /HEALTHCHECK[\s\S]*\/api\/health/);
  assert.match(dockerfile, /ENTRYPOINT \["\/usr\/bin\/tini", "--"\]/);
  assert.doesNotMatch(dockerfile, /^COPY \. \.$/m);
});

test("Docker context excludes local data and large test fixtures", () => {
  for (const ignoredPath of [
    "docs",
    "src-tauri/tests",
    "data",
    "cache",
    "output",
    "whisper_model.bin",
  ]) {
    assert.match(dockerignore, new RegExp(`^${ignoredPath}$`, "m"));
  }
});

test("base Compose is safe and portable while GPU support is optional", () => {
  assert.match(
    compose,
    /ghcr\.io\/smathsp\/bili-shadowreplay:latest/,
  );
  assert.match(compose, /BSR_BIND_ADDRESS:-127\.0\.0\.1/);
  assert.match(compose, /stop_grace_period:\s*30s/);
  assert.match(compose, /\/api\/health/);
  assert.doesNotMatch(compose, /^\s*devices:/m);
  assert.doesNotMatch(compose, /^\s*WHISPER_MODEL:/m);
  assert.match(gpuCompose, /^\s*devices:/m);
  assert.match(gpuCompose, /\/dev\/dri:\/dev\/dri/);
});

test("Docker workflow builds natively and publishes an atomic manifest", () => {
  assert.match(workflow, /runner:\s*ubuntu-24\.04-arm/);
  assert.match(workflow, /platform:\s*linux\/amd64/);
  assert.match(workflow, /platform:\s*linux\/arm64/);
  assert.doesNotMatch(workflow, /setup-qemu-action/);
  assert.match(workflow, /push-by-digest=true/);
  assert.match(workflow, /buildx imagetools create/);
  assert.match(workflow, /cache-from:\s*type=gha,scope=docker-/);
  assert.match(workflow, /Verify published architectures/);
  assert.match(workflow, /Smoke test image runtime/);
  assert.match(workflow, /Verify GHCR push access/);
});

test("Docker documentation points to the published custom image", () => {
  assert.match(
    dockerDocs,
    /ghcr\.io\/smathsp\/bili-shadowreplay:latest/,
  );
  assert.doesNotMatch(dockerDocs, /ghcr\.io\/xinrea\/bili-shadowreplay/);
});
