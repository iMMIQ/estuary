# Performance Benchmark

[Documentation index](README.md) | [Architecture](architecture.md)

The standalone benchmark measures gateway overhead against an in-process mock
model server. It does not require a GPU or deployed vLLM instance.

```bash
ESTUARY_RUN_BENCHMARK=1 \
ESTUARY_BENCH_REQUESTS=100 \
cargo bench --bench performance
```

`ESTUARY_BENCH_REQUESTS` controls the iterations per scenario and defaults to
`100`. Without `ESTUARY_RUN_BENCHMARK=1`, the executable exits immediately.

The benchmark runs:

- OpenAI Chat Completions, Anthropic Messages through the Chat adapter, and
  Codex Responses;
- 2 KiB, 128 KiB, and 1 MiB prompts;
- 1, 8, and 32 configured nodes;
- concurrency at four times aggregate node capacity;
- approximate and exact scheduler selection;
- prefix preprocessing;
- remote and LRU-hit tokenization paths.

Output includes throughput, end-to-end p50/p99 latency, Linux process RSS,
prefix preprocessing time, scheduler time, and tokenization time. The mock
upstream responds immediately, so results measure gateway and queue behavior,
not model inference.

Run comparisons on the same idle host with the same Rust toolchain and release
profile. The benchmark is not part of normal CI because its large-body matrix
and latency results are host-sensitive.
