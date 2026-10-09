# Local coding-agent end-to-end tests

[Documentation index](README.md) | [Native vLLM provider](vllm.md) |
[Session logging](session-logging.md)

The opt-in tests run installed Claude Code, Codex, and OpenCode CLIs through
Estuary against an already running loopback inference server. They check
streamed text, actual local MCP execution, replayed tool output, reconstructed
session payloads, token usage and timing, logger health, and released scheduler
reservations. A fresh random nonce must reach the final answer; HTTP success or
a model merely mentioning a tool does not satisfy the tool check.

Build Estuary with `cargo build --locked` before running these tests. The scripts
isolate CLI settings, workspaces, and databases under `target/` and exclude cloud
credentials from the CLI environment. Keep downloaded models, server binaries,
reports, and CLI transcripts under this ignored directory as well. Use each
script's `--help` for timeout, binary, protocol, and output options.

## llama.cpp

Download a CPU archive from [llama.cpp releases](https://github.com/ggml-org/llama.cpp/releases)
and extract it under `target/local-e2e/llama.cpp/`. Place
[Qwen3.5-0.8B-Q4_0.gguf](https://huggingface.co/ggml-org/Qwen3.5-0.8B-GGUF/tree/main)
under `target/local-e2e/`. Adjust the executable path to match the extracted
archive, then start the server:

```bash
LLAMA_SERVER=target/local-e2e/llama.cpp/llama-server
"$LLAMA_SERVER" \
  -m target/local-e2e/Qwen3.5-0.8B-Q4_0.gguf \
  --alias qwen35-cpu --host 127.0.0.1 --port 18000 \
  --ctx-size 16384 --parallel 1 --threads 2 --threads-batch 2 \
  --seed 1 --temp 0 --predict 256 \
  --jinja --reasoning off \
  --chat-template-file tests/fixtures/qwen35-inline-system.jinja
```

The template fixture allows Claude Code's inline text-only `system` environment
messages to retain their original positions while preserving Qwen3.5's tool and
reasoning formats. Apply this fixture on the inference server when testing
these messages.

The generic `openai` provider validates gateway protocols with real GGUF
inference. It does not exercise vLLM's kernels, tokenization, metrics, or KV event
routing; use the `vllm` provider for those interfaces.

## Claude Code

Run native Messages and Chat conversion against the server above:

```bash
python3 tests/claude_vllm_cpu_e2e.py \
  --provider openai --upstream http://127.0.0.1:18000/v1 \
  --model qwen35-cpu --protocols native chat \
  --output target/local-e2e/claude
```

The script creates a separate node for each protocol. The default tool fixture
uses MCP; `--tool-mode read` tests the CLI's built-in `Read` tool. Adding
`responses` to `--protocols` tests the explicit rejection of Claude Code's
unsupported output effort setting, rather than successful Responses generation.

## Codex

The Codex test uses Responses with isolated settings and a small replacement
instruction file:

```bash
python3 tests/codex_cpu_e2e.py \
  --upstream http://127.0.0.1:18000/v1 --model qwen35-cpu \
  --flatten-namespaces --output target/local-e2e/codex
```

For backends that cannot consume Codex's Responses `namespace` tool groups,
enable `provider.flatten_codex_namespaces` or **Codex namespace tool
compatibility** in the node editor. Estuary flattens function definitions and
replayed calls, collision-checks names, and restores namespaces in JSON and
streamed responses. This option defaults to `false` for generic providers; the
vLLM provider applies the conversion automatically.

The script disables web search and shell tools and preapproves only the fixed
local nonce tool for unattended execution.

## OpenCode v2

Run Chat and Responses with the same server:

```bash
python3 tests/opencode_cpu_e2e.py \
  --upstream http://127.0.0.1:18000/v1 --model qwen35-cpu \
  --output target/local-e2e/opencode
```

The script starts its own loopback OpenCode server, waits for agent and MCP
readiness, and connects the CLI with `run --server`. Fresh XDG paths and removal
of inherited `PWD` isolate the workspace. Only `nonce_read` is allowed; shell,
web, and subagents are denied. The MCP server uses `codemode: false` to expose
its tool directly to the model.

Chat uses `@opencode/ai/providers/openai-compatible`; Responses uses
`@opencode/ai/providers/openai/responses`. The script also checks SQLite
integrity, payload DAG reference counts, and shared message prefixes. Use
`--protocols chat` or `--protocols responses` to isolate a protocol. See the
upstream [provider](https://opencode.ai/v2/docs/providers),
[MCP](https://opencode.ai/v2/docs/mcp-servers), and
[permission](https://opencode.ai/v2/docs/permissions) configuration.

## Ollama

Use a tool-capable model on Ollama's loopback
[OpenAI-compatible endpoint](https://docs.ollama.com/api/openai-compatibility):

```bash
python3 tests/claude_vllm_cpu_e2e.py \
  --provider openai --upstream http://127.0.0.1:11434/v1 \
  --model MODEL --protocols chat --output target/local-e2e/ollama
```

For every backend, the context window must fit the CLI's environment and tool
definitions. Small models may omit tools, produce invalid arguments, or repeat
calls. Retain failed transcripts under `target/` to distinguish model behavior
from protocol failures. These checks validate transport, tool roundtrips, and
logging; they do not assess general coding ability.
