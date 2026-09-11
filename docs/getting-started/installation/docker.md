# Docker 部署

BiliBili ShadowReplay 的 Docker 版包含 Web 控制界面，适合在 NAS、服务器或其他
无图形界面的设备上使用。镜像同时支持 `linux/amd64` 和 `linux/arm64`，Docker 会
自动选择匹配宿主机的架构。

> [!WARNING]
> Web 管理接口目前没有内置账号验证。Compose 默认只允许本机访问，请不要把
> 3000 端口直接暴露到公网。远程使用时建议通过带登录验证和 HTTPS 的反向代理访问。

## 使用 Compose（推荐）

下载仓库中的 `docker_compose.yaml`，在同一目录运行：

```bash
docker compose -f docker_compose.yaml pull
docker compose -f docker_compose.yaml up -d
```

启动后打开 <http://127.0.0.1:3000>。查看运行状态和日志：

```bash
docker compose -f docker_compose.yaml ps
docker compose -f docker_compose.yaml logs -f
```

默认使用的成品镜像是：

```text
ghcr.io/smathsp/bili-shadowreplay:latest
```

如需固定版本，可在启动前设置 `BILI_SHADOWREPLAY_IMAGE`，例如：

```bash
export BILI_SHADOWREPLAY_IMAGE=ghcr.io/smathsp/bili-shadowreplay:2.22.5
docker compose -f docker_compose.yaml up -d
```

## 数据与模型

Compose 会在当前目录创建并持久保存以下目录：

- `data`：配置、数据库和日志；
- `cache`：录制缓存；
- `output`：完整录像、切片、字幕和弹幕导出；
- `models`：可选的 Whisper 模型文件。

把 Whisper 模型放进宿主机的 `models` 目录后，在 Web 设置页填写容器内路径，
例如 `/app/models/ggml-model.bin`。镜像已自带语音活动检测所需的
`silero_vad.onnx`，无需额外下载。

Web 设置页可以修改缓存和输出路径。容器会立即使用新路径；若希望重建容器后仍
保留内容，请确保新路径也位于已映射的 `/app/data`、`/app/cache`、`/app/output`
或 `/app/models` 中。

## 抖音弹幕导出

Web 界面的“导出弹幕资料为 JSONL”会逐行导出抖音聊天消息，每行严格只包含
`displayId`、`name`、`avatar`、`content` 四个字段。旧录像也会按这个精简格式
导出；内部录制文件仍保留原始数据，用于断线恢复、去重和后续兼容。

```json
{"displayId":"bgz_1","name":"114514研究所—白教授","avatar":"https://example.com/avatar.jpeg","content":"弹幕内容"}
```

## 局域网访问

仅在可信局域网内，可把监听地址改为所有网卡：

```bash
BSR_BIND_ADDRESS=0.0.0.0 docker compose -f docker_compose.yaml up -d
```

也可用 `BSR_PORT` 修改宿主机端口：

```bash
BSR_PORT=8080 docker compose -f docker_compose.yaml up -d
```

这只改变端口映射，不会增加登录验证；公网访问仍应使用带身份验证的反向代理。

容器默认使用 `Asia/Shanghai` 时区。其他地区可在启动时设置 `TZ`，例如
`TZ=Asia/Tokyo`。Compose 同时限制了 Docker 标准输出日志的大小和保留份数，避免
长期运行耗尽磁盘。

## Intel / AMD 核显加速（Linux，可选）

确认宿主机存在 `/dev/dri` 后，同时加载 GPU 覆盖文件：

```bash
docker compose \
  -f docker_compose.yaml \
  -f docker_compose.gpu.yaml \
  up -d
```

没有 `/dev/dri` 的设备不要加载这个文件。程序会自动检测 VAAPI 设备，并在可用时
使用硬件编码。

## 直接使用 Docker

不使用 Compose 时，可运行：

```bash
docker run -d \
  --name bili-shadowreplay \
  --restart unless-stopped \
  --stop-timeout 60 \
  -p 127.0.0.1:3000:3000 \
  -v "$PWD/data:/app/data" \
  -v "$PWD/cache:/app/cache" \
  -v "$PWD/output:/app/output" \
  -v "$PWD/models:/app/models" \
  -e DATA_DIR=/app/data \
  -e CONFIG_PATH=/app/data/config.toml \
  -e CACHE_DIR=/app/cache \
  -e OUTPUT_DIR=/app/output \
  -e LOG_DIR=/app/data/logs \
  -e TZ=Asia/Shanghai \
  ghcr.io/smathsp/bili-shadowreplay:latest
```

升级镜像时，持久化目录中的配置和数据不会丢失：

```bash
docker compose -f docker_compose.yaml pull
docker compose -f docker_compose.yaml up -d
```

容器健康检查会访问 `/api/health`。若状态显示为 `unhealthy`，先查看容器日志，
并确认 3000 端口没有被其他程序占用。
