# Deployment Assets

This directory contains deployment helpers shipped with the repository and release tarballs:

- `build-version.sh`: Resolves the build version (`<version>` on matching `v<version>` tags, `<version>+<12-char-sha>` otherwise, or `ESTUARY_BUILD_VERSION`).
- `docker-build.sh`: Auto-selects the fastest reachable official or mainland mirror and builds the Docker image (`estuary:local` by default).
- `env/common.env.example`: Baseline environment variable template for systemd or host deployments.

## Quick Run (Static Binary)

```bash
./estuary \
  --database /var/lib/estuary/estuary.db \
  --listen 0.0.0.0:8080 \
  --admin-listen 127.0.0.1:9090
```

## Full Documentation

See the complete deployment guide for static binaries, Docker builds, Compose runtime, local SQLite requirements, and graceful rolling updates:

- In this repository: [docs/deployment.md](../docs/deployment.md)
- Online (for standalone release archives): <https://github.com/iMMIQ/estuary/blob/main/docs/deployment.md>
