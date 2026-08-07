ARG BUN_IMAGE=oven/bun:1.3.14-alpine
ARG RUST_IMAGE=rust:1.85-alpine3.21
ARG ALPINE_IMAGE=alpine:3.21

FROM ${BUN_IMAGE} AS web-builder

WORKDIR /build/web
ARG NPM_REGISTRY=https://registry.npmjs.org
COPY web/package.json web/bun.lock web/bunfig.toml ./
RUN registry="${NPM_REGISTRY%/}" \
    && sed -i -E "s#https://(repo.huaweicloud.com/repository/npm|registry.npmjs.org|registry.npmmirror.com)/#${registry}/#g" bun.lock \
    && bun install --frozen-lockfile --registry="${registry}"
COPY web/ ./
RUN bun run build

FROM ${RUST_IMAGE} AS rust-builder

ARG APK_REPOSITORY=https://dl-cdn.alpinelinux.org/alpine
RUN version="$(cut -d. -f1,2 /etc/alpine-release)" \
    && printf '%s/v%s/main\n%s/v%s/community\n' \
        "${APK_REPOSITORY%/}" "${version}" "${APK_REPOSITORY%/}" "${version}" \
        > /etc/apk/repositories \
    && apk add --no-cache build-base cmake perl
WORKDIR /build
ARG CARGO_REGISTRY
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
COPY benches/ ./benches/
COPY --from=web-builder /build/web/dist ./web/dist/
RUN if [ -n "${CARGO_REGISTRY}" ]; then \
        mkdir .cargo; \
        printf '[source.crates-io]\nreplace-with = "mirror"\n[source.mirror]\nregistry = "%s"\n' \
            "${CARGO_REGISTRY}" > .cargo/config.toml; \
    fi \
    && cargo build --locked --release

FROM ${ALPINE_IMAGE}

COPY --from=rust-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN addgroup -S -g 10001 estuary \
    && adduser -S -D -H -u 10001 -G estuary estuary
COPY --from=rust-builder /build/target/release/estuary /usr/local/bin/estuary-initial
RUN set -eux; \
    version="$(/usr/local/bin/estuary-initial --version | awk '{print $2}')"; \
    release="/opt/estuary/releases/${version}"; \
    mkdir -p "${release}" /opt/estuary/state/slots/a /opt/estuary/state/slots/b /var/lib/estuary/run; \
    cp /usr/local/bin/estuary-initial "${release}/estuary"; \
    chmod 0755 "${release}/estuary"; \
    ln -s "${release}" /opt/estuary/state/current; \
    ln -s "${release}" /opt/estuary/state/slots/a/current; \
    ln -s "${release}" /opt/estuary/state/slots/b/current; \
    chown -R estuary:estuary /opt/estuary/state /var/lib/estuary; \
    rm /usr/local/bin/estuary-initial

ENV ESTUARY_DATABASE=/var/lib/estuary/estuary.db \
    ESTUARY_LISTEN=0.0.0.0:8080 \
    ESTUARY_ADMIN_LISTEN=0.0.0.0:9090 \
    ESTUARY_SLOT_B_ADMIN_LISTEN=127.0.0.1:19092 \
    ESTUARY_RELEASE_ROOT=/opt/estuary/releases \
    ESTUARY_STATE_ROOT=/opt/estuary/state \
    ESTUARY_RUNTIME_DIR=/var/lib/estuary/run \
    ESTUARY_LOG_JSON=true

VOLUME ["/opt/estuary", "/var/lib/estuary"]
EXPOSE 8080 9090
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/opt/estuary/state/current/estuary", "status"]

USER estuary
ENTRYPOINT ["/opt/estuary/state/current/estuary"]
CMD ["supervisor"]
