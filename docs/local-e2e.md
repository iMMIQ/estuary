# Local coding-agent end-to-end tests

[Documentation index](README.md) | [Native vLLM provider](vllm.md) |
[Session logging](session-logging.md)

`tests/claude_vllm_cpu_e2e.py` runs the real Claude Code CLI through Estuary
against an already running loopback inference server. It checks streamed text,
a local MCP tool roundtrip using a fresh random nonce, reconstructed session
payloads, token usage and timing, logger health, and released scheduler
reservations. CLI configuration, workspaces, and databases are isolated under
`target/`; cloud credentials are excluded from the CLI environment.

## llama.cpp without a local build

Download the Ubuntu x64 CPU archive from the upstream
[llama.cpp releases](https://github.com/ggml-org/llama.cpp/releases) and extract
it under `target/claude-llama-cpu-e2e/`. Download
[Qwen3.5-0.8B-Q4_0.gguf](https://huggingface.co/ggml-org/Qwen3.5-0.8B-GGUF/tree/main)
to the same directory. Start the extracted server:

```bash
target/claude-llama-cpu-e2e/llama-b11516/llama-server \
  -m target/claude-llama-cpu-e2e/Qwen3.5-0.8B-Q4_0.gguf \
  --alias qwen35-cpu --host 127.0.0.1 --port 18000 \
  --ctx-size 16384 --parallel 1 --threads 2 --threads-batch 2 \
  --seed 1 --temp 0 \
  --predict 256 \
  --jinja --reasoning off \
  --chat-template-file tests/fixtures/qwen35-inline-system.jinja
```

Use the extracted directory corresponding to the downloaded release. In another
terminal, build Estuary and run the test:

```bash
cargo build --locked
python3 tests/claude_vllm_cpu_e2e.py \
  --provider openai --upstream http://127.0.0.1:18000/v1 \
  --model qwen35-cpu --protocols native chat \
  --output target/claude-llama-cpu-e2e/runs
```

The stock Qwen3.5 template rejects Claude Code's inline `system` environment
messages with `System message must be at the beginning`. The test fixture is
derived from the official Qwen3.5 template and renders those text messages in
their original positions. It preserves the original tool and reasoning formats.
This is an upstream template adjustment, not a gateway-wide message rewrite.

Select provider type `openai`, with `anthropic_protocol` explicitly set to
`native` or `chat`. The script creates a separate node for each protocol. This
avoids vLLM's version, tokenization, metrics, and KV event interfaces. The test
therefore validates Estuary's generic gateway paths and real Qwen3.5 GGUF
inference; it does not validate vLLM AWQ kernels or exact KV event routing.

## Codex Responses

The separate Codex test uses the real CLI and Responses API, with isolated
settings, a small replacement instruction file, and the same random-nonce MCP
fixture. Run against the llama.cpp server above:

```bash
python3 tests/codex_cpu_e2e.py \
  --upstream http://127.0.0.1:18000/v1 --model qwen35-cpu \
  --flatten-namespaces --output target/codex-llama-cpu-e2e/runs
```

Current Codex versions send MCP tools inside Responses `namespace` tool groups.
llama.cpp `b11516` skips those groups. Set the node's
`provider.flatten_codex_namespaces` to `true`, or enable **Codex namespace tool
compatibility** in the editor for an OpenAI-compatible provider. Estuary flattens
function definitions and replayed calls, collision-checks their names, and
restores the namespace and function name in both JSON and streamed responses.
The option defaults to `false` for generic providers; vLLM already performs this
conversion automatically. It does not impose vLLM's other tool restrictions on
generic backends.

The script checks streamed text, actual MCP execution and replayed tool output,
the exact nonce in the final answer, Responses protocol attribution, captured
payloads and usage/timing, logger health, and released reservations. It disables
web search and shell tools to keep this a local protocol test. The CLI settings
preapprove only the fixed local nonce tool for unattended execution, using the
official per-tool approval setting. The provider configuration
follows the official [custom-provider configuration](https://learn.chatgpt.com/docs/config-file/config-advanced).

## Ollama

For an existing Ollama server with a tool-capable model, use its loopback
[OpenAI-compatible endpoint](https://docs.ollama.com/api/openai-compatibility)
and select Chat conversion:

```bash
python3 tests/claude_vllm_cpu_e2e.py \
  --provider openai --upstream http://127.0.0.1:11434/v1 \
  --model MODEL --protocols chat
```

The model's context window must fit Claude Code's environment and tool
definitions. The default MCP fixture is smaller than the CLI's built-in `Read`
tool; use `--tool-mode read` for additional model-capability coverage. A response
mentioning a tool is insufficient: the test requires Claude Code to execute the
tool, send the resulting nonce upstream, and return that exact nonce.

## Local validation observations

With llama.cpp `b11516` and Qwen3.5-0.8B Q4_0 on an AVX2-only CPU, both native
Messages and Chat conversion completed real streamed text and MCP roundtrips
after the template adjustment above. Logger health and scheduler reservations
also passed. Responses `output_config.effort` remains an expected explicit
gateway rejection; this test does not establish Responses generation support.

The small model can ask for a file path instead of using the supplied tool, or
generate repeated identical tool calls even when instructed to call once. Keep
the CLI transcripts when assessing model behavior. A successful gateway
roundtrip verifies delivery and logging, not coding-agent reliability.
