# Docker 部署

BiliBili ShadowReplay 提供了服务端部署的能力，提供 Web 控制界面，可以用于在服务器等无图形界面环境下部署使用。

官方镜像支持 `linux/amd64` 和 `linux/arm64`。Docker 会根据宿主机架构自动
选择对应镜像；在 Android Termux 中运行请参考 [Termux 部署](./termux)。

## 镜像获取

```bash
# 拉取最新版本
docker pull ghcr.io/xinrea/bili-shadowreplay:latest
# 拉取指定版本
docker pull ghcr.io/xinrea/bili-shadowreplay:2.5.0
# 速度太慢？从镜像源拉取
docker pull ghcr.nju.edu.cn/xinrea/bili-shadowreplay:latest
```

## 镜像使用

使用方法：

```bash
sudo docker run -it -d\
    -p 3000:3000 \
    -v $DATA_DIR:/app/data \
    -v $CACHE_DIR:/app/cache \
    -v $OUTPUT_DIR:/app/output \
    -v $WHISPER_MODEL:/app/whisper_model.bin \
    -e DATA_DIR=/app/data \
    -e CONFIG_PATH=/app/data/config.toml \
    -e CACHE_DIR=/app/cache \
    -e OUTPUT_DIR=/app/output \
    -e LOG_DIR=/app/data/logs \
    -e WHISPER_MODEL=/app/whisper_model.bin \
    --name bili-shadowreplay \
    ghcr.io/xinrea/bili-shadowreplay:latest
```

其中：

- `$DATA_DIR`：为数据目录，对应于桌面版的数据目录，

  Windows 下位于 `C:\Users\{用户名}\AppData\Roaming\cn.vjoi.bilishadowreplay`;

  MacOS 下位于 `/Users/{user}/Library/Application Support/cn.vjoi.bilishadowreplay`

- `$CACHE_DIR`：为缓存目录，对应于桌面版的缓存目录；
- `$OUTPUT_DIR`：为输出目录，对应于桌面版的输出目录；
- `$WHISPER_MODEL`：为 Whisper 模型文件路径，对应于桌面版的 Whisper 模型文件路径。

配置文件和日志会分别保存到 `$DATA_DIR/config.toml` 与 `$DATA_DIR/logs`。Web
设置页可以直接修改容器内的缓存、输出和 Whisper 模型路径；这些路径需要映射到
宿主机卷，才能在重建容器后继续使用。`CACHE_DIR` 和 `OUTPUT_DIR` 是首次创建
配置文件时使用的默认值，之后可以由 Web 设置页持久修改；`WHISPER_MODEL`
环境变量会在每次启动时覆盖配置文件中的模型路径，如需在 Web 设置页管理模型
路径，请移除此环境变量。
浏览器通知需要保持页面打开，并在 HTTPS 或 localhost 页面中授权通知权限。
