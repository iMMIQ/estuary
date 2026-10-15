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
`100` and must be positive. Without `ESTUARY_RUN_BENCHMARK=1`, the executable
exits immediately. HTTP bodies are serialized before timing, and each scenario
runs four untimed warm-up requests to populate connections and prefix state.

The benchmark runs:

- OpenAI Chat Completions, Anthropic Messages, and Codex Responses, all through
  the same Chat backend;
- 2 KiB, 128 KiB, and 1 MiB prompts;
- 1, 8, and 32 configured nodes;
- concurrency at four times aggregate node capacity;
- approximate and exact scheduler selection;
- prefix preprocessing;
- prefix directory record and lookup with 1, 128, and 1,024 trees, using short
  and 16 KiB ASCII prompts;
- uncontended response-buffer reservation and release;
- remote and LRU-hit tokenization paths.

Output includes throughput, end-to-end p50/p99 latency, Linux process RSS,
prefix preprocessing time, scheduler time, and tokenization time. The mock
upstream responds immediately, so results measure gateway and queue behavior,
not model inference.

Run comparisons on the same idle host with the same Rust toolchain and release
profile. The benchmark is not part of normal CI because its large-body matrix
and latency results are host-sensitive.

To run just the prefix-directory and response-buffer microbenchmarks:

```bash
ESTUARY_RUN_BENCHMARK=1 \
ESTUARY_BENCH_MICRO_ONLY=1 \
ESTUARY_BENCH_REQUESTS=1000 \
cargo bench --bench performance
```

These operations run `max(100 * ESTUARY_BENCH_REQUESTS, 10000)` times per
scenario. Routing text and directory population happen outside the timed loops.
The directory capacity is large enough to retain every seeded tree.

The session-log SQL comparison uses the production schema with deterministic,
interleaved sessions. It prints query plans, verifies identical results, and
compares the former optional-session predicate with the indexed equality:

```bash
python3 benches/session_list.py
```

Defaults are 200,000 requests, 1,000 sessions, 51 returned records, and 30 timed
queries after warm-up. Override them with `--rows`, `--sessions`, and
`--iterations`. This measures in-memory SQLite query time; it excludes HTTP,
metadata decoding, disk I/O, and model inference.

## Optimization comparison (2026-10-15)

The original `256bf90` source and the optimized working tree were built with
Rust 1.99.0 and the same extended benchmark harness and release profile. The
host was an AMD Ryzen 9 5900HX, with each executable pinned to CPUs 8–11 and
`TOKIO_WORKER_THREADS=4`. The following are medians of three alternating runs,
each with 100,000 operations per microbenchmark scenario:

| Operation | Before | After | Speedup |
| --- | ---: | ---: | ---: |
| Record a repeated 16 KiB prefix, one tree | 19.480 µs | 0.747 µs | 26.1× |
| Match a complete 16 KiB prefix, one tree | 24.801 µs | 0.374 µs | 66.3× |
| Record a short prefix, 1,024 trees | 1.550 µs | 0.525 µs | 3.0× |
| Reserve and release an uncontended response buffer | 0.036 µs | 0.023 µs | 1.6× |

A separate full-matrix run used 1,000 requests per scenario on the same pinned
CPUs. All 27 scenarios completed successfully. For the 32-node, 128 KiB cases
(128 concurrent requests), throughput was:

| Protocol | Before (requests/s) | After (requests/s) | Speedup |
| --- | ---: | ---: | ---: |
| OpenAI Chat Completions | 1,339.9 | 4,672.1 | 3.49× |
| Anthropic Messages | 1,281.1 | 4,004.5 | 3.13× |
| Codex Responses | 1,299.8 | 4,076.3 | 3.14× |

These HTTP results use an immediate mock upstream, measuring gateway overhead
rather than model generation. Small 100-request samples varied substantially
on this shared host, especially at high concurrency; use larger samples when
checking end-to-end regressions.

The session-list comparison (SQLite 3.46.1, the default 200,000-row dataset)
measured a median of 10.604 ms before and 0.028 ms after, over 30 queries per
variant. The query plan changed from `requests_time` to `requests_session` by
replacing the optional-session `OR` with a separate session-equality query.

Complete radix edges use byte-prefix comparison and their stored character
counts. Directory accounting updates only the changed tenant and tree instead
of summing every entry under the directory write lock. Response-buffer
reservations register waiters only when permits are unavailable.

The request adapter also transfers ownership of passthrough JSON and
deserializes compatibility requests from borrowed JSON, avoiding an intermediate
DOM clone. Single-chunk buffered responses retain the upstream `Bytes`; the
64 KiB regression fixture verifies storage identity, eliminating one allocation
and 65,536 bytes of copying for that fixture. Fragmented responses allocate on
the second nonempty chunk and use a validated content-length hint.

These are focused operation timings on a shared host, rather than a guarantee
of end-to-end speedup. Partial radix-edge matches still compare Unicode
characters, and model inference, network latency, protocol conversion, and
scheduler contention can dominate other workloads.
