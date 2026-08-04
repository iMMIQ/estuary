# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.97.1
FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --locked --release

FROM debian:bookworm-slim AS runtime

RUN sed -i \
        -e 's|http://deb.debian.org/debian-security|http://mirrors.ustc.edu.cn/debian-security|g' \
        -e 's|http://deb.debian.org/debian|http://mirrors.ustc.edu.cn/debian|g' \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 estuary \
    && useradd --uid 10001 --gid estuary --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin estuary

COPY --from=builder /build/target/release/estuary /usr/local/bin/estuary
COPY config.example.yaml /etc/estuary/config.yaml

USER 10001:10001
EXPOSE 8080 9090

ENV ESTUARY_CONFIG=/etc/estuary/config.yaml \
    RUST_LOG=estuary=info

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD ["curl", "--fail", "--silent", "--show-error", "http://127.0.0.1:9090/health/live"]

ENTRYPOINT ["/usr/local/bin/estuary"]
