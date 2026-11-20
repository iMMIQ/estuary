ARG NODE_IMAGE=node:26.12.1-alpine3.24
ARG BUN_IMAGE=oven/bun:1.4.4-alpine
ARG RUST_IMAGE=rust:1.98.1-alpine3.22
ARG ALPINE_IMAGE=alpine:3.21

FROM ${BUN_IMAGE} AS bun-runtime

FROM ${NODE_IMAGE} AS web-builder
COPY --from=bun-runtime /usr/local/bin/bun /usr/local/bin/bun

WORKDIR /build/web
ARG NPM_REGISTRY=https://registry.npmjs.org
COPY web/package.json web/bun.lock web/bunfig.toml ./
RUN registry="${NPM_REGISTRY%/}" \
    && sed -i -E "s#https://(repo.huaweicloud.com/repository/npm|registry.npmjs.org|registry.npmmirror.com)/#${registry}/#g" bun.lock \
    && bun install --frozen-lockfile --registry="${registry}"
COPY web/ ./
COPY tests/fixtures/ /build/tests/fixtures/
RUN bun run build

FROM ${RUST_IMAGE} AS rust-builder

ARG APK_REPOSITORY=https://dl-cdn.alpinelinux.org/alpine
RUN version="$(cut -d. -f1,2 /etc/alpine-release)" \
    && printf '%s/v%s/main\n%s/v%s/community\n' \
        "${APK_REPOSITORY%/}" "${version}" "${APK_REPOSITORY%/}" "${version}" \
        > /etc/apk/repositories \
    && apk add --no-cache build-base cmake lld perl
WORKDIR /build
ARG CARGO_REGISTRY
ARG ESTUARY_BUILD_VERSION
COPY Cargo.toml Cargo.lock ./
COPY .cargo/config.toml ./.cargo/config.toml
COPY build.rs ./
COPY deploy/build-version.sh ./deploy/build-version.sh
COPY src/ ./src/
COPY vendor/ ./vendor/
COPY examples/ ./examples/
COPY benches/ ./benches/
COPY --from=web-builder /build/web/dist ./web/dist/
RUN sed -i '/^\[source\./,$d' .cargo/config.toml \
    && if [ -n "${CARGO_REGISTRY}" ]; then \
        printf '[source.crates-io]\nreplace-with = "mirror"\n[source.mirror]\nregistry = "%s"\n' \
            "${CARGO_REGISTRY}" >> .cargo/config.toml; \
    fi \
    && cargo build --locked --release

FROM ${ALPINE_IMAGE}

COPY --from=rust-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN addgroup -S -g 10001 estuary \
    && adduser -S -D -H -u 10001 -G estuary estuary \
    && mkdir -p /var/lib/estuary \
    && chown estuary:estuary /var/lib/estuary
COPY --from=rust-builder /build/target/release/estuary /usr/local/bin/estuary

ENV ESTUARY_DATABASE=/var/lib/estuary/estuary.db \
    ESTUARY_LISTEN=0.0.0.0:8080 \
    ESTUARY_ADMIN_LISTEN=0.0.0.0:9090 \
    ESTUARY_LOG_JSON=true

VOLUME ["/var/lib/estuary"]
EXPOSE 8080 9090
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:9090/health/live"]

USER estuary
ENTRYPOINT ["/usr/local/bin/estuary"]
