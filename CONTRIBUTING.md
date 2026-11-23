# Contributing

[README](README.md) | [Documentation Index](docs/README.md) | [Architecture](docs/architecture.md)

---

## Prerequisites & Toolchains

Exact pinned versions live in the repository files (do not duplicate version numbers elsewhere):

- **Rust**: [rust-toolchain.toml](rust-toolchain.toml) plus the LLD linker (`ld.lld`). Local builds use `.cargo/config.toml` (`target-cpu=native`, `-fuse-ld=lld`). Note that `.cargo/config.toml` places its `[source.*]` mirror override at the bottom so CI and Docker can strip it independently.
- **Web**: Node.js ([.node-version](.node-version)), Bun, and dependencies pinned in [web/package.json](web/package.json) / `web/bun.lock`.
- **Scripts (optional)**: `python3`, `ruff`, `shellcheck`, `shfmt`, and `actionlint` for CI script checks; Playwright Chromium for browser E2E tests.

Commit both `Cargo.lock` and `web/bun.lock` whenever updating dependencies.

---

## Building & Local Verification

`web/dist/` is excluded from Git and packaged via `crates/estuary-assets` (embedded at compile time in release builds and read live from disk in debug builds), so build the web assets before running release builds or UI asset tests.

Run the same checks enforced by [.github/workflows/checks.yml](.github/workflows/checks.yml):

```bash
# Web checks & build
cd web
bun install --frozen-lockfile
bun run check
bun run deadcode
bun run typecheck
bun run build
bun run test
bun run contract:check
cd ..

# Rust checks & tests
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --lib --bins --tests
cargo test --locked --test integration load_balance_stress -- --ignored
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --document-private-items
```

Browser E2E tests (Playwright):

```bash
cd web
bunx --bun playwright install chromium
bun run test:e2e
```

Script & workflow checks:

```bash
ruff check tests
ruff format --check tests
shellcheck deploy/*.sh
shfmt -d -i 4 -ci deploy/*.sh
actionlint
```

---

## Shared Configuration Contract

Rust is the single source of truth for node schemas, defaults, validation limits, and admin DTOs:

- [crates/estuary-contract](crates/estuary-contract/src/lib.rs): Shared node schemas, defaults, `NODE_INTEGER_LIMITS`, `RESERVED_UPSTREAM_HEADERS`, and admin DTOs.
- [src/config.rs](src/config.rs) and [src/config/contract.rs](src/config/contract.rs): `clap::Args` bindings and typed `NodeConfig` validation.
- [crates/estuary-contract/examples/config_contract.rs](crates/estuary-contract/examples/config_contract.rs): Dev-only generator (`-p estuary-contract --features config-contract`, enabling `ts-rs`) emitting [web/src/generated/config.ts](web/src/generated/config.ts).

Whenever you modify node configuration types, defaults, validation bounds, or shared observability/session-log DTOs:

1. Update Rust first (`crates/estuary-contract/src/`, `src/config.rs`, or `src/config/contract.rs`).
2. Regenerate and verify the checked-in TypeScript contract (never edit `web/src/generated/config.ts` by hand):
   ```bash
   cd web
   bun run contract:generate
   bun run contract:check
   ```
3. Add matching valid/invalid boundary cases to [tests/fixtures/node-config-contract.json](tests/fixtures/node-config-contract.json), which is executed by both Rust (`tests/integration/config_contract.rs`) and TypeScript (`web/src/config-validation.test.ts`).

Keep in mind the deliberate distinction between API deserialization defaults (`max_concurrency = 1`, `provider.type = "openai"`) and UI creation presets (`editor_node_defaults()`: `max_concurrency = 16`, `provider.type = "vllm"`), both defined in Rust. Normal frontend builds read the committed `config.ts` without needing Rust, and normal gateway builds do not link `ts-rs`.

---

## Vendored Protocol Libraries

Estuary vendors a trimmed MIT-licensed subset of `deepseek-recipe` (`deepseek-recipe`, `deepseek-recipe-core`, `deepseek-recipe-macros`) under `vendor/deepseek-recipe/` strictly for Messages/Responses ↔ Chat protocol conversion. See [vendor/deepseek-recipe/ESTUARY.md](vendor/deepseek-recipe/ESTUARY.md) for local pruning notes and upgrade instructions. Do not enable DeepSeek-specific prompt encoders or tokenizers for generic backends.

---

## Local Coding-Agent E2E Tests

Opt-in Python harnesses in `tests/` drive real installed Claude Code, Codex, and OpenCode CLIs through Estuary. They isolate workspaces, CLI settings, and SQLite databases under `target/`, strip cloud credentials, verify actual local MCP / built-in tool execution using a random nonce, and check usage accounting, session logs, and released scheduler leases.

Build the gateway binary first (`cargo build --locked`). Run `python3 <script> --help` for authoritative flags, timeouts, and output paths.

| Script | Backend required | What it exercises |
| --- | --- | --- |
| `tests/claude_mock_e2e.py` | Built-in Chat mock (no model/GPU) | Claude Code SSE text, single/multi-PNG input & `Read` replay, MCP & `Read` tools, usage, session logs. |
| `tests/codex_mock_e2e.py` | Built-in Chat mock (no model/GPU) | Codex Responses streaming, MCP nonce, single/multi-PNG attachments & MCP image results. |
| `tests/codex_goal_e2e.py` | Built-in Chat mock (no model/GPU) | Codex app-server native goals lifecycle, autonomous turns, `agent.goal_objective`, MCP nonce, token budget. |
| `tests/claude_vllm_cpu_e2e.py` | Running local server (`--provider openai\|vllm`) | Real inference with Claude Code (`--tool-mode mcp\|read`, optional `--images`). |
| `tests/codex_cpu_e2e.py` | Running local server (`--provider openai\|vllm`) | Real inference with Codex Responses and MCP nonce (optional `--images`). |
| `tests/opencode_cpu_e2e.py` | Running local server (`--provider openai\|vllm`) | Real inference with OpenCode v2 over `--protocols chat responses`, plus SQLite/DAG integrity. |

### Running Against a Local CPU Server

For local GGUF testing with `llama.cpp` (`--provider openai`), keep downloads under ignored `target/local-e2e/` and pass the inline-system template fixture when testing Claude Code:

```bash
target/local-e2e/llama.cpp/llama-server \
  -m target/local-e2e/Qwen3.5-0.8B-Q4_0.gguf \
  --alias qwen35-cpu --host 127.0.0.1 --port 18000 \
  --ctx-size 16384 --parallel 1 --threads 2 --threads-batch 2 \
  --seed 1 --temp 0 --predict 256 --jinja --reasoning off \
  --chat-template-file tests/fixtures/qwen35-inline-system.jinja
```

Then run any CPU harness against `--upstream http://127.0.0.1:18000/v1 --model qwen35-cpu` (or against Ollama's loopback endpoint `http://127.0.0.1:11434/v1` with `--provider openai`).

---

## Performance Benchmarks

The standalone Rust benchmark measures gateway overhead against an immediate in-process mock upstream (no GPU or vLLM required). Without `ESTUARY_RUN_BENCHMARK=1`, the harness exits immediately:

```bash
# Full HTTP + microbenchmark matrix (Chat/Messages/Responses × 2 KiB/128 KiB/1 MiB × 1/8/32 nodes)
ESTUARY_RUN_BENCHMARK=1 ESTUARY_BENCH_REQUESTS=100 cargo bench --bench performance

# Prefix-directory and response-buffer microbenchmarks only
ESTUARY_RUN_BENCHMARK=1 ESTUARY_BENCH_MICRO_ONLY=1 ESTUARY_BENCH_REQUESTS=1000 cargo bench --bench performance
```

Additional storage/query benchmarks:

```bash
# Session-log list query plan & timing comparison (in-memory SQLite)
python3 benches/session_list.py

# Opt-in 1,000-turn session-log DAG deduplication fixture
cargo test --lib thousand_turn_history -- --ignored --nocapture
```

Run before/after comparisons on the same idle host, Rust toolchain, and release profile.

---

## Releases & Versioning

Before tagging `vX.Y.Z`, update all three version locations in one commit:

1. `[package] version` in [Cargo.toml](Cargo.toml)
2. The `estuary` entry in `Cargo.lock`
3. `"version"` in [web/package.json](web/package.json)

[.github/workflows/release.yml](.github/workflows/release.yml) verifies that the Git tag matches both `Cargo.toml` and `web/package.json`, runs the full check suite, cross-compiles static `amd64`/`arm64` musl binaries with `cargo-zigbuild`, and publishes `.tar.gz` + `.sha256` release artifacts.

---

## Documentation Guidelines

- Keep all technical guides under `docs/` and `CONTRIBUTING.md` in **English**; keep `README.md`, `README.zh-CN.md`, and `README.ja.md` synchronized.
- Follow **Single Source of Truth**: document each setting, route, metric, or security rule in its designated guide and link to it elsewhere instead of duplicating paragraphs.
- Document current behavior only — do not leave retired fields, migration archaeology, PR test plans, or one-off host benchmark numbers in the guides.
