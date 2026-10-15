# Dashboard observability

[Documentation index](README.md) | [Operations](operations.md)

The overview, upstream list, and node metrics tab share process-local operational
statistics. Collection works even when session logging is disabled. It does not
query session-log SQLite tables or store prompts and responses.

## Rates and history

The server samples once per second independently of browsers. Rates include the
raw counter delta divided by actual monotonic elapsed time, a time-aware EWMA
with a 10-second half-life, and a trailing 60-second mean. Browser refreshes do
not change smoothing. Gateway scopes use their known zero-counter baseline for
the first EWMA update, so a newly created model does not initialize at its first
burst rate. Engine rates warm up from the first valid counter delta. The UI
normally polls every five seconds and pauses in hidden tabs.

History retains up to 180 five-second points, covering 15 minutes. Charts share
a time axis and inspector. Output throughput also shows the highest raw
one-second sample within each point; load history retains sampled peaks.
In-flight peaks additionally observe request starts and finishes, preserving
requests shorter than the sampling interval. These are observed peaks, not a
continuous measurement of upstream gauges. Missing observations and sampling
gaps remain gaps. Wall-clock timestamps label history; rates and elapsed times
use the monotonic clock. A backwards wall-clock jump clears chart history.

Select 1, 5, or 15 minutes for counters, ratios, and distributions. Counters and
histograms use 10-second buckets. The oldest partial bucket is excluded once a
full window is available, so effective coverage can be up to 10 seconds shorter
than the selected window. `observed_seconds` reports this coverage, including
startup and new-node warmup. History charts always show 15 minutes; their
latency series use rolling five-minute P95s.

## Sources and definitions

| Metric | Definition |
| --- | --- |
| Gateway incoming / successful requests per second | Logical downstream request starts / final successes. |
| Gateway input / output tokens per second | Reported usage, booked when the downstream response finishes or is cancelled. It can lag live engine generation. |
| Gateway TTFT | Gateway receipt to the first observed text, reasoning, or tool output. Includes admission and routing waits; excludes the final network hop to the client. Streaming only. |
| Upstream TTFT | Upstream attempt start to its first observed generation event. Streaming only. |
| Full response lifetime | Gateway receipt until the response body is exhausted or dropped, including failures and cancellations. This is a server-side observation, not client acknowledgement. |
| Admission wait | Time waiting for gateway ingress admission, including cancellation while waiting. |
| Routing wait | Time spent selecting a usable node or waiting for its capacity, summed across attempts. |
| TPOT / per-request generation speed | Provider mean ITL when available; otherwise estimated first-to-last generation time divided by reported output tokens minus one, for a single generation. Speed is its reciprocal. SSE chunks are never counted as tokens. |
| In-flight requests | Starts minus final outcomes, including ingress waits and response delivery. |
| Occupied upstream slots | Node lease occupancy, separately from the whole downstream request lifetime. |
| Failure / cancellation / unknown / success ratios | Each final outcome divided by all final outcomes within the selected window. |
| Retries | Retry attempts, plus the fraction of completed logical requests that retried at least once. |
| Output usage coverage | Requests with reported output usage divided by completed generation requests. Embeddings do not lower this ratio. |
| Gateway cached-token ratio | Reported cached tokens divided by input tokens, using only requests reporting both. |
| Exact / approximate routing matches | Routing affinity decisions, independently of actual engine cache hits. |

Primary request statistics include POST chat completions, completions, responses,
messages, and embeddings. Health checks, model discovery, and token-count utility
requests are excluded. Embeddings contribute request and input-token statistics,
but not generation-speed samples. Failures before model attribution appear in
fleet totals without being assigned to a model.

An HTTP 200 is insufficient to establish streaming success. Recognized terminal
events establish success, explicit stream errors establish failure, downstream
body drops establish cancellation, and unrecognized stream EOF remains unknown.
Gateway/model counters count logical requests once. Node counters count attempts,
including failed attempts before retries. Request latency and usage samples are
attributed to the final node; they are not measurements of every retry attempt.

Distributions expose P50, P95, mean, sample count, and overflow count. They merge
fixed histograms rather than averaging node percentiles. Each histogram has 80
bins: zero, geometric finite boundaries starting at 0.0005 and increasing by
1.25, and an overflow bin. Units are seconds except generation speed, which is
tokens per second. Quantiles are interpolated estimates and become unavailable
if the requested rank lies in overflow. Low TTFT sample counts are labelled.

## Engine telemetry and missing data

The separate vLLM section includes **all traffic seen by the engine**, including
traffic bypassing this gateway. It shows input/output token rates, successful
request rates, running/waiting work, KV-cache usage, recent prefix-cache ratio,
preemptions per minute, reporting coverage, sample age, and observed resets.
Fleet KV-cache usage is the maximum across fresh reporting nodes.

Counter deltas are calculated per labelled engine series before node/fleet
aggregation. A counter decrease resets its baseline; the first sample and stale
gaps do not create throughput spikes. The cache ratio uses paired counter deltas
from the last minute, rather than process-lifetime totals. Legacy node rate and
cache-ratio fields use these same smoothed/recent values; cumulative counters
remain available separately.

`null` means unavailable, unsupported, stale, resetting, or not yet sampled.
Valid idle samples are zero and decay the EWMA. Missing usage is not estimated
as zero. Reporters are counted separately for input, output, and requests; fleet
rates can be partial when nodes do not report. Node panels retain telemetry age
and expose stale coverage. Failed dashboard refreshes retain the previous data
with an explicit stale indicator.

The gateway does not inject usage options into native passthrough requests.
Clients/providers must return usage for usage-dependent metrics. Existing
protocol adapters retain their existing usage behavior.

## API, metrics, and limits

`GET /admin/api/observability` uses existing admin authentication. Query options:

- `scope=fleet|node|model` (default `fleet`);
- `id=<node ID or public model name>` for node/model scope;
- `window=60|300|900` seconds (default `300`).

The response includes fleet, node, and model summaries and only the selected
scope's history. Unknown selections return `selected: null` and empty history;
invalid scopes/windows return HTTP 400. For example:

```bash
curl -H "Authorization: Bearer $ESTUARY_ADMIN_TOKEN" \
  'http://127.0.0.1:9090/admin/api/observability?scope=model&id=chat&window=300'
```

`GET /metrics` exports the same source with bounded `scope` and `name` labels:

- `estuary_gateway_{started,success,failed,cancelled,unknown,retries,...}_total`;
- `estuary_gateway_{incoming_requests,completed_requests,input_tokens,output_tokens}_per_second`
  with `stat=raw|smoothed|mean_60s`;
- lifetime `estuary_gateway_{ttft,upstream_ttft,duration,ingress,queue,tpot}_seconds`
  and `estuary_gateway_generation_rate_tokens_per_second` histograms;
- `estuary_engine_*_per_second` with the same `stat` values,
  `estuary_engine_cache_hit_ratio`, `estuary_engine_preemptions_per_minute`,
  and reporting/expected-node gauges;
- `estuary_active_requests`, `estuary_upstream_active_requests`,
  `estuary_concurrency_limit`, queue/memory gauges, and engine demand/KV gauges.

Unavailable series are omitted from exposition. Global queue and memory gauges
are emitted only for fleet scope. Engine gauges are not attributed to models.
Legacy HTTP header-creation metrics retain their existing names and semantics.

Collection retains at most 32 node scopes and 32 model scopes, plus fleet totals.
Identifiers are limited to 1,024 bytes, window storage to 90 ten-second buckets
per scope, and history to 180 points per scope. Each configured vLLM node tracks
up to 512 relevant labelled counter series and 600 recent delta samples. Larger
engine series sets report unavailable rates instead of a silently truncated sum.
The dashboard labels dimension limits; fleet totals still include every request.
Memory and scrape size therefore depend on these bounds rather than request
volume. Removed nodes retain their bounded process-local history.

Gateway restart clears these statistics and changes `instance_id`. Replacing a
node configuration changes its instance and resets its node statistics; old
in-flight completions cannot contaminate the replacement. Fleet/model results
still account for those requests. Separate gateway processes have separate
histories: use an external metrics store for persistence or cross-process views.

Regression tests cover usage, scope filtering, authentication, response lifetime,
stream errors/EOF/cancellation, retry attribution, node replacement, coverage,
histogram merging, bounds, resets, and desktop/mobile dashboard interactions.
