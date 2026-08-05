# syntax=docker.m.daocloud.io/docker/dockerfile:1

ARG RUST_VERSION=1.97.1
ARG IMAGE_REGISTRY=docker.m.daocloud.io
ARG GCR_REGISTRY=m.daocloud.io/gcr.io

# Release builds target this stage with Zig-cross-compiled static binaries in
# docker-bin/. It has no RUN instructions, so arm64 assembly needs no QEMU.
FROM ${IMAGE_REGISTRY}/library/busybox:1.37.0-musl AS runtime-tools

FROM ${GCR_REGISTRY}/distroless/static-debian12:latest AS runtime-base

COPY --from=runtime-tools /bin/busybox /usr/bin/busybox
COPY --from=runtime-tools --chown=10001:10001 /tmp /var/lib/estuary

USER 10001:10001
EXPOSE 8080 9090
VOLUME ["/var/lib/estuary"]

ENV ESTUARY_DATABASE=/var/lib/estuary/estuary.db \
    ESTUARY_LISTEN=0.0.0.0:8080 \
    ESTUARY_ADMIN_LISTEN=0.0.0.0:9090 \
    ESTUARY_LOG_JSON=true \
    RUST_LOG=estuary=info

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/bin/busybox", "wget", "-q", "-O", "/dev/null", "http://127.0.0.1:9090/health/live"]

ENTRYPOINT ["/usr/local/bin/estuary"]

FROM runtime-base AS runtime-prebuilt

ARG TARGETARCH
COPY --chown=10001:10001 docker-bin/linux/${TARGETARCH}/estuary /usr/local/bin/estuary

# The remaining stages keep local `docker compose up --build` self-contained.
FROM ${IMAGE_REGISTRY}/oven/bun:1.3.14 AS web-builder

WORKDIR /build/web
COPY web/package.json web/bun.lock web/bunfig.toml ./
RUN bun install --frozen-lockfile
COPY web/index.html web/tsconfig.json web/tsconfig.app.json web/tsconfig.node.json web/vite.config.ts ./
COPY web/src ./src
RUN bun run test && bun run build

FROM ${IMAGE_REGISTRY}/library/rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
COPY --from=web-builder /build/web/dist ./web/dist
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --locked --release

FROM ${IMAGE_REGISTRY}/library/debian:bookworm-slim AS runtime-local

RUN sed -i \
        -e 's|http://deb.debian.org/debian-security|http://mirrors.ustc.edu.cn/debian-security|g' \
        -e 's|http://deb.debian.org/debian|http://mirrors.ustc.edu.cn/debian|g' \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::ForceIPv4=true update \
    && apt-get -o Acquire::ForceIPv4=true install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 estuary \
    && useradd --uid 10001 --gid estuary --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin estuary \
    && install --directory --owner=10001 --group=10001 --mode=0750 /var/lib/estuary

COPY --from=builder /build/target/release/estuary /usr/local/bin/estuary

USER 10001:10001
EXPOSE 8080 9090
VOLUME ["/var/lib/estuary"]

ENV ESTUARY_DATABASE=/var/lib/estuary/estuary.db \
    ESTUARY_LISTEN=0.0.0.0:8080 \
    ESTUARY_ADMIN_LISTEN=0.0.0.0:9090 \
    ESTUARY_LOG_JSON=true \
    RUST_LOG=estuary=info

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD ["curl", "--fail", "--silent", "--show-error", "http://127.0.0.1:9090/health/live"]

ENTRYPOINT ["/usr/local/bin/estuary"]
