# Performance benchmark

The standalone benchmark starts an in-process mock vLLM-compatible HTTP server and an Estuary public listener. It does not require GPUs or a deployed vLLM cluster.

```bash
ESTUARY_RUN_BENCHMARK=1 ESTUARY_BENCH_REQUESTS=100 cargo bench --bench performance
```

The matrix covers:

- OpenAI Chat Completions, Anthropic Messages through the Chat adapter, and Codex Responses;
- 2 KiB, 128 KiB, and 1 MiB prompts;
- 1, 8, and 32 configured nodes;
- concurrency at four times aggregate node capacity, exercising the single saturated queue;
- bounded prefix preprocessing;
- remote and cache-hit `/tokenize` routing paths;
- approximate and exact KV-aware scheduler selection.

The report includes requests per second, end-to-end p50/p99 latency, mean preprocessing and scheduling time, and Linux process RSS where `/proc/self/status` is available. The mock returns immediately, so this suite measures gateway overhead and queue behavior rather than model inference. Run it on an otherwise idle host, pin CPU frequency when comparing commits, and use the same Rust toolchain and build profile.

The suite is intentionally excluded from normal CI because its full matrix moves large request bodies and latency comparisons are host-sensitive. Without `ESTUARY_RUN_BENCHMARK=1`, the bench executable exits immediately; this also keeps `cargo test --all-targets` fast.
