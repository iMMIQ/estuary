# Native vLLM Provider

Estuary's native provider targets vLLM 0.25.0 and newer. The lower bound is enforced from the origin-root `GET /version` response and cannot be lowered in configuration. A node remains outside the routing set until this check succeeds.

## API contract

| Interface | vLLM 0.25+ contract | Estuary use |
| --- | --- | --- |
| `GET /version` | `{"version":"0.25.0"}` | Compatibility gate and reported provider version |
| `GET /metrics` | Prometheus text | Running, waiting, and KV-use load signals |
| `POST /tokenize` | Chat messages or string prompt to token IDs | Exact incoming token sequence |
| ZMQ KV publisher | Topic, 8-byte sequence, MessagePack batch | Store, eviction, and clear tracking |
| ZMQ replay router | Requested starting sequence | Repair a detected PUB sequence gap |

The stable metric inputs across vLLM 0.25 and 0.26 are:

- `vllm:num_requests_running`
- `vllm:num_requests_waiting`
- `vllm:kv_cache_usage_perc`

Estuary sums running and waiting samples across engine labels and uses the largest reported KV-use ratio. Fresh upstream load replaces neither admission nor the hard node semaphore: scheduling uses `max(local_active, upstream_running + upstream_waiting)`, while `max_concurrency` remains the gateway's local hard limit.

## KV event compatibility

`EventBatch` is a MessagePack array. Its event list contains tagged maps named `BlockStored`, `BlockRemoved`, and `AllBlocksCleared`. External block hashes can be binary SHA hashes or backward-compatible integers. Estuary ignores unknown fields and unknown event types so additive vLLM changes do not break the subscriber.

The relevant version difference is replay framing:

| Version | Replay data frames after the ZMQ delimiter |
| --- | --- |
| vLLM 0.25 | `sequence, payload` |
| vLLM 0.26+ | `topic, sequence, payload` |

Both forms are accepted. Estuary subscribes to PUB first and then actively requests available replay frames, so events emitted during replay remain queued. Replay must be contiguous from the requested sequence. Disconnects and gaps suspend exact routing until continuity is proven. Replay-buffer overflow or sequence rollback clears the affected directory; later incremental events remain non-authoritative until replay from sequence zero succeeds or `AllBlocksCleared` establishes a new baseline.

The event API is marked experimental by vLLM. Estuary therefore treats malformed or incompatible events as a provider-local loss of cache knowledge, never as a gateway-fatal error.

## vLLM launch

```bash
vllm serve MODEL \
  --kv-events-config \
  '{"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558","topic":"kv-events","buffer_steps":10000}'
```

The publisher's `endpoint` and `replay_endpoint` are bind addresses. The corresponding management API fields use concrete connect addresses:

```json
{
  "type": "vllm",
  "version_path": "/version",
  "metrics_path": "/metrics",
  "tokenize_path": "/tokenize",
  "monitor_interval_ms": 1000,
  "request_timeout_ms": 2000,
  "telemetry_stale_ms": 5000,
  "waiting_threshold": 8,
  "tokenize_cache_entries": 4096,
  "kv_events": {
    "endpoint": "tcp://vllm-0.internal:5557",
    "replay_endpoint": "tcp://vllm-0.internal:5558",
    "topic": "kv-events",
    "reconnect_ms": 1000,
    "max_blocks": 1000000,
    "max_event_bytes": 16777216
  }
}
```

Keep both ZMQ ports on a private network. The vLLM KV event transport does not define application-level authentication or encryption.

## Routing semantics

For supported requests, Estuary first checks whether the approximate match already exceeds `routing.prefix.cache_threshold` and whether an authoritative exact directory contains blocks. Only then does it obtain token IDs from `/tokenize`; pre-tokenized Completions bypass the remote-call gate. The least-loaded tokenizer's node-local cache is checked before one request is sent to that node. Failure or the single total `provider.request_timeout_ms` deadline degrades immediately to approximate affinity without trying every node. Estuary then walks the event-derived token trie for every eligible node. A hybrid model can publish multiple KV groups; the usable prefix is the minimum match across all groups learned for that node. If the longest confirmed match exceeds the threshold and the pool is not load-imbalanced, the longest matching node set is preferred.

Exact state is intentionally conservative:

- only local `GPU` events are accepted;
- LoRA-specific events are ignored;
- an event with non-null `extra_keys` is ignored, covering salts and multimodal identifiers;
- Chat Completions and single string or pre-tokenized Completions can use exact matching;
- Responses, batch Completions, documents, and failed tokenization use character affinity;
- `BlockRemoved` and `AllBlocksCleared` remove learned state rather than guessing eviction;
- sequence rollback, replay overflow, conflicts, decode failures, and memory-limit violations invalidate the node directory;
- an invalidated directory automatically attempts replay and otherwise stays on approximate affinity until a trustworthy baseline is observed.

Each schedulable Estuary node should represent one addressable vLLM cache domain. When vLLM internally distributes one HTTP endpoint over hidden data-parallel ranks, Estuary cannot force a request onto the rank whose event it observed. Expose those ranks or replicas separately when rank-level locality is required.

Nodes sharing a public model must use tokenization-equivalent model revisions, chat templates, and prompt-processing settings. Use separate public model names for incompatible pools.

## References

- [vLLM 0.25 KV events](https://github.com/vllm-project/vllm/blob/v0.25.0/vllm/distributed/kv_events.py)
- [vLLM 0.25 tokenize protocol](https://github.com/vllm-project/vllm/blob/v0.25.0/vllm/entrypoints/serve/tokenize/protocol.py)
- [vLLM 0.25 metrics](https://github.com/vllm-project/vllm/blob/v0.25.0/vllm/v1/metrics/loggers.py)
- [vLLM KV event subscriber example](https://github.com/vllm-project/vllm/blob/v0.25.0/examples/features/kv_events/kv_events_subscriber.py)
