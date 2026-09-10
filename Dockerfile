# Build the architecture-independent frontend once on the native builder.
FROM --platform=$BUILDPLATFORM node:22-bookworm AS frontend-builder

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY package.json yarn.lock ./

# Allow enough time for large packages when the registry is slow.
RUN yarn install --frozen-lockfile --network-timeout 600000

# Copy only files used by the frontend build. Keeping Rust, documentation and
# runtime data out of this layer prevents unrelated changes from invalidating it.
COPY index*.html ./
COPY vite.config.ts svelte.config.js postcss.config.cjs tailwind.config.js ./
COPY tsconfig*.json ./
COPY public ./public
COPY src ./src

# Build frontend
RUN yarn build

# Build Rust backend. cargo-chef keeps third-party dependencies in a separate
# Docker layer, so application source changes do not force a full rebuild.
FROM rust:1.90-slim AS rust-base

WORKDIR /app/src-tauri

# Install required system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    pkg-config \
    libssl-dev \
    glib-2.0-dev \
    libclang-dev \
    g++ \
    wget \
    xz-utils \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --version 0.1.78 --locked

FROM rust-base AS rust-planner

COPY src-tauri .
RUN cargo chef prepare --recipe-path recipe.json

FROM rust-base AS rust-builder

COPY --from=rust-planner /app/src-tauri/recipe.json recipe.json
RUN cargo chef cook \
    --no-default-features \
    --features headless \
    --release \
    --locked \
    --recipe-path recipe.json

COPY src-tauri .

# Sentry DSN baked into the binary at build time (option_env! in main.rs).
# Empty by default so Sentry stays disabled unless a DSN is provided.
ARG SENTRY_ENDPOINT=""
ENV SENTRY_ENDPOINT=${SENTRY_ENDPOINT}

# Build Rust backend
RUN cargo build --no-default-features --features headless --release --locked

# Final stage
FROM debian:trixie-slim AS final

ARG OCI_SOURCE="https://github.com/smathsp/bili-shadowreplay"
LABEL org.opencontainers.image.source="${OCI_SOURCE}"

WORKDIR /app

# Install runtime dependencies, SSL certificates, Chinese fonts and process tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    fonts-wqy-microhei \
    netbase \
    nscd \
    tzdata \
    ffmpeg \
    curl \
    tini \
    && update-ca-certificates \
    && touch /etc/netgroup \
    && mkdir -p /var/run/nscd \
    && chmod 755 /var/run/nscd \
    && rm -rf /var/lib/apt/lists/*

# Add /app to PATH
ENV PATH="/app:${PATH}"

# Copy built frontend
COPY --from=frontend-builder /app/dist ./dist

# Copy built Rust binary and the VAD model used by local Whisper transcription
COPY --from=rust-builder /app/src-tauri/target/release/bili-shadowreplay .
COPY --from=rust-builder /app/src-tauri/silero_vad.onnx ./silero_vad.onnx

# Expose port
EXPOSE 3000

# Check the same HTTP endpoint used by container orchestrators.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["curl", "--fail", "--silent", "--show-error", "--max-time", "4", "http://127.0.0.1:3000/api/health"]

# Run the application
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sh", "-c", "nscd && exec ./bili-shadowreplay"]
