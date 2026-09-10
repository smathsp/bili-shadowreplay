import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function read(relativePath) {
  return readFile(new URL(`../../${relativePath}`, import.meta.url), "utf8");
}

const [
  dockerfile,
  dockerignore,
  compose,
  gpuCompose,
  workflow,
  releaseWorkflow,
  dockerDocs,
] = await Promise.all([
    read("Dockerfile"),
    read(".dockerignore"),
    read("docker_compose.yaml"),
    read("docker_compose.gpu.yaml"),
    read(".github/workflows/package.yml"),
    read(".github/workflows/main.yml"),
    read("docs/getting-started/installation/docker.md"),
  ]);

test("runtime image contains required assets and health tooling", () => {
  assert.match(dockerfile, /COPY .*silero_vad\.onnx .*silero_vad\.onnx/);
  assert.match(dockerfile, /HEALTHCHECK[\s\S]*\/api\/health/);
  assert.match(dockerfile, /ENTRYPOINT \["\/usr\/bin\/tini", "--"\]/);
  assert.doesNotMatch(dockerfile, /^COPY \. \.$/m);
});

test("Rust image contains the native build tools required by CMake", () => {
  const rustBase = dockerfile.match(
    /FROM rust:[\s\S]*?(?=\nFROM rust-base AS rust-planner)/,
  )?.[0];

  assert.ok(rustBase, "rust-base stage should exist");
  assert.match(rustBase, /^\s*cmake \\/m);
  assert.match(rustBase, /^\s*make \\/m);
  assert.match(rustBase, /^\s*g\+\+ \\/m);
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
  assert.match(compose, /stop_grace_period:\s*60s/);
  assert.match(compose, /\/api\/health/);
  assert.doesNotMatch(compose, /^\s*devices:/m);
  assert.doesNotMatch(compose, /^\s*WHISPER_MODEL:/m);
  assert.match(gpuCompose, /^\s*devices:/m);
  assert.match(gpuCompose, /\/dev\/dri:\/dev\/dri/);
  assert.match(dockerDocs, /--stop-timeout\s+60/);
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
  assert.match(workflow, /release_version:/);
  assert.match(workflow, /Docker releases must be dispatched from main/);
  assert.match(
    workflow,
    /registry-preflight:[\s\S]*?actions\/checkout@v7[\s\S]*?require\('\.\/package\.json'\)\.version/,
  );
  assert.match(workflow, /require\('\.\/package\.json'\)\.version/);
  assert.match(workflow, /GITHUB_REF_TYPE["']?\s*==\s*["']tag["']/);
  assert.match(workflow, /tag_version=\$\{GITHUB_REF_NAME#v\}/);
  assert.match(workflow, /GITHUB_REF_NAME["']?\s*!=\s*v\*/);
  assert.match(workflow, /tag_version["']?\s*=~\s*\$semver_regex/);
  assert.match(workflow, /tag_version[\s\S]*?package_version/);
  assert.match(
    workflow,
    /publish_latest:\s*\$\{\{ steps\.validate\.outputs\.publish_latest \}\}/,
  );
  assert.equal(
    workflow.match(
      /type=raw,value=latest,enable=\$\{\{ needs\.registry-preflight\.outputs\.publish_latest == 'true' \}\}/g,
    )?.length,
    2,
  );
  assert.doesNotMatch(
    workflow,
    /type=raw,value=latest,enable=\$\{\{[^\n]*startsWith\(github\.ref/,
  );
  assert.match(
    workflow,
    /type=semver,pattern=\{\{version\}\},value=\$\{\{ inputs\.release_version \}\}/,
  );
  assert.match(
    workflow,
    /github\.event_name == 'workflow_dispatch'[\s\S]*?type=raw,value=latest/,
  );
});

test("Release workflow only builds tags and has full history for changelogs", () => {
  assert.match(
    releaseWorkflow,
    /publish-tauri:\s*\n\s*if: startsWith\(github\.ref, 'refs\/tags\/v'\)/,
  );
  assert.match(
    releaseWorkflow,
    /actions\/checkout@v7[\s\S]*?fetch-depth:\s*0/,
  );
  assert.match(releaseWorkflow, /Wait for the matching Docker image/);
});

test("Docker documentation points to the published custom image", () => {
  assert.match(
    dockerDocs,
    /ghcr\.io\/smathsp\/bili-shadowreplay:latest/,
  );
  assert.doesNotMatch(dockerDocs, /ghcr\.io\/xinrea\/bili-shadowreplay/);
});
