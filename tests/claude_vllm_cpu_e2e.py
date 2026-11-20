#!/usr/bin/env python3
"""Opt-in Claude Code test against a running local CPU inference server.

Build with cargo build --locked, start vLLM with an instruction/tool model and
--enable-auto-tool-choice --tool-call-parser hermes, then run:
python3 tests/claude_vllm_cpu_e2e.py --upstream http://127.0.0.1:18000/v1 \
    --model qwen-cpu
For llama.cpp (started with --jinja), use --provider openai. For Ollama's
OpenAI-compatible endpoint, use --provider openai.
Uses isolated Claude settings/workspace and fresh gateway/log databases. The
default tool test reads a random nonce through a local stdio MCP server; use
--tool-mode read for the more demanding built-in Read tool. Does not use cloud
credentials. Reports and CLI transcripts go under ignored target/.
Add --images to test single/multiple PNG input and Read replay with a vision model.
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from e2e_helpers import (
    MCP_NONCE_SCRIPT,
    E2ERunner,
    GatewayContext,
    run_process_group,
    validate_loopback_cli,
)
from image_fixtures import IMAGES, image_blocks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--provider", choices=["vllm", "openai"], default="vllm")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/estuary"))
    parser.add_argument(
        "--output", type=Path, default=Path("target/claude-vllm-cpu-e2e/runs")
    )
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--skip-tools", action="store_true")
    parser.add_argument("--tool-mode", choices=["mcp", "read"], default="mcp")
    parser.add_argument(
        "--images",
        action="store_true",
        help="Also test single/multiple PNG input and Read replay",
    )
    args = parser.parse_args()
    validate_loopback_cli(parser, args.upstream, "claude", "Claude Code")
    runner = E2ERunner(
        args.output,
        {
            "upstream": args.upstream,
            "model": args.model,
            "provider": args.provider,
            "claude_version": subprocess.check_output(
                ["claude", "--version"], text=True
            ).strip(),
        },
    )
    image_paths = runner.write_images(IMAGES)

    with GatewayContext(args.binary, runner.output) as gateway:
        node = gateway.register_node(
            "cpu-chat",
            args.upstream,
            {"cpu-chat": args.model},
            max_concurrency=2,
            provider=args.provider,
        )
        runner.report["provider_version"] = node["runtime"]["provider_version"]
        if args.provider == "vllm":
            runner.report["vllm_version"] = runner.report["provider_version"]
        runner.save()

        def claude(tools=False, image_count=0):
            alias = "cpu-chat"
            session = alias + ("-tools" if tools else "-text")
            if image_count:
                session += f"-images-{image_count}"
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(("ANTHROPIC_", "CLAUDE_", "ESTUARY_"))
                and k != "MAX_THINKING_TOKENS"
            }
            env.update(
                {
                    "ANTHROPIC_BASE_URL": gateway.base_url,
                    "ANTHROPIC_AUTH_TOKEN": "local-test-only",
                    "ANTHROPIC_API_KEY": "local-test-only",
                    "ANTHROPIC_MODEL": alias,
                    "ANTHROPIC_DEFAULT_HAIKU_MODEL": alias,
                    "ANTHROPIC_DEFAULT_SONNET_MODEL": alias,
                    "ANTHROPIC_DEFAULT_OPUS_MODEL": alias,
                    "ANTHROPIC_CUSTOM_HEADERS": "x-estuary-session-id: " + session,
                    "CLAUDE_CONFIG_DIR": str(runner.output / "claude-config"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "CLAUDE_CODE_MAX_OUTPUT_TOKENS": "256",
                    "MAX_THINKING_TOKENS": "0",
                    "NO_PROXY": "127.0.0.1,localhost,::1",
                }
            )
            mcp = tools and args.tool_mode == "mcp" and not image_count
            tool_name = "mcp__nonce__read" if mcp else "Read"
            mcp_config = {"mcpServers": {}}
            if mcp:
                mcp_config["mcpServers"]["nonce"] = {
                    "command": sys.executable,
                    "args": [str(MCP_NONCE_SCRIPT), str(runner.fixture)],
                }
            prompt = "Reply exactly CPU_OK."
            if tools:
                prompt = (
                    "Call mcp__nonce__read exactly once with an empty JSON object {}. "
                    "It reads the local file for you and requires no file path. "
                    "After the tool returns, reply only with its exact nonce string."
                    if mcp
                    else f"Use Read to read {runner.fixture}. Return the value of its JSON nonce field. Ignore line numbers."
                )
            if tools and image_count:
                prompt = (
                    "IMAGE_READ_E2E: Read these PNG files in order: "
                    + json.dumps([str(path) for path in image_paths[:image_count]])
                    + f". After reading all, reply exactly IMAGE_OK_{image_count}."
                )
            direct_images = image_count and not tools
            command = [
                "claude",
                "-p",
                *([] if direct_images else [prompt]),
                "--model",
                alias,
                "--bare" if mcp else "--safe-mode",
                "--setting-sources",
                "",
                "--strict-mcp-config",
                "--mcp-config",
                json.dumps(mcp_config),
                "--tools",
                "Read" if tools and not mcp else "",
                "--system-prompt",
                "You are a helpful assistant. Use tools when asked. Be concise.",
                "--output-format",
                "stream-json",
                "--verbose",
                "--no-session-persistence",
                "--max-turns",
                "4",
            ]
            stdin = None
            if direct_images:
                command.extend(["--input-format", "stream-json"])
                stdin = (
                    json.dumps(
                        {
                            "type": "user",
                            "message": {
                                "role": "user",
                                "content": image_blocks(image_count),
                            },
                        }
                    )
                    + "\n"
                ).encode()
            if tools:
                command.extend(
                    ["--allowedTools", tool_name, "--permission-mode", "dontAsk"]
                )
            transcript = runner.output / (session + ".jsonl")
            diagnostics = runner.output / (session + ".stderr")
            returncode = run_process_group(
                command,
                cwd=runner.workspace,
                env=env,
                stdout_path=transcript,
                stderr_path=diagnostics,
                timeout=args.timeout,
                label="Claude",
                stdin_bytes=stdin,
            )
            stdout, stderr = transcript.read_text(), diagnostics.read_text()
            events = [
                json.loads(line) for line in stdout.splitlines() if line.startswith("{")
            ]
            result = next(
                (e for e in reversed(events) if e.get("type") == "result"), None
            )
            assert returncode == 0 and result and not result.get("is_error"), (
                returncode,
                result,
                stderr[-2000:],
            )
            expected = (
                f"IMAGE_OK_{image_count}"
                if image_count
                else runner.nonce
                if tools
                else "CPU_OK"
            )
            assert expected in result.get("result", ""), result
            rows = gateway.session_details(
                session, minimum=1 + (image_count or 1) if tools else 1
            )
            for detail in rows:
                request = detail["request"]
                assert (
                    request["http_status"] == 200 and request["outcome"] == "success"
                ), request
                assert (
                    request["protocol"] == "anthropic_messages" and request["streaming"]
                ), request
                assert request["usage"].get("output_tokens", 0) > 0, request
                assert request["delivery"] == "body_consumed", request
                assert request["timings_us"].get("first_output", 0) > 0, request
                assert request["timings_us"].get("total", 0) > 0, request
                raw = request["usage"].get("raw", {})
                created = raw.get("prompt_tokens_details", {}).get(
                    "created_cache_tokens"
                )
                if created is not None:
                    assert request["usage"]["cache_write_tokens"] == created, request
                assert len(request["attempts"]) == 1, request
                assert request["attempts"][0]["adapter"] == "chat_to_messages", request
                assert request["attempts"][0]["endpoint"] == "chat/completions", request
                assert {p["stage"] for p in detail["payloads"]} == {
                    "client_input",
                    "upstream_input",
                    "client_output",
                    "upstream_output",
                }, detail
            if tools:
                inputs = [
                    p["content"]
                    for d in rows
                    for p in d["payloads"]
                    if p["stage"] == "client_input"
                ]
                tool_results = max(
                    (
                        [
                            block
                            for message in body["messages"]
                            if isinstance(message["content"], list)
                            for block in message["content"]
                            if block.get("type") == "tool_result"
                        ]
                        for body in inputs
                    ),
                    key=len,
                )
                if image_count:
                    assert (
                        sum(
                            block.get("type") == "image"
                            for result in tool_results
                            for block in result.get("content", [])
                        )
                        == image_count
                    ), tool_results
                else:
                    assert any(
                        runner.nonce in json.dumps(result) for result in tool_results
                    ), tool_results
            return {
                "requests": len(rows),
                "result": result.get("result"),
                "usage": [r["request"]["usage"] for r in rows],
                "timings_us": [r["request"]["timings_us"] for r in rows],
            }

        runner.check("Claude Code streaming through Chat", claude)
        if not args.skip_tools:
            runner.check(
                f"Claude Code {args.tool_mode} tool roundtrip through Chat",
                lambda: claude(True),
            )
        if args.images:
            for count in [1, 2]:
                runner.check(
                    f"Claude Code {count} image input through Chat",
                    lambda count=count: claude(image_count=count),
                )
                if not args.skip_tools:
                    runner.check(
                        f"Claude Code Read {count} image replay through Chat",
                        lambda count=count: claude(True, count),
                    )
        expected_sessions = ["cpu-chat-text"]
        if not args.skip_tools:
            expected_sessions.append("cpu-chat-tools")
        if args.images:
            expected_sessions.extend(
                f"cpu-chat-{kind}-images-{count}"
                for count in [1, 2]
                for kind in (["text"] if args.skip_tools else ["text", "tools"])
            )
        runner.check(
            "logger health and session grouping",
            lambda: gateway.check_logger_health(
                expected_sessions, include_sessions=True
            ),
        )
        runner.check(
            "scheduler releases all reservations", gateway.check_scheduler_idle
        )

    with sqlite3.connect(runner.output / "logs.db") as connection:
        runner.report["storage"] = {
            "content_blobs": connection.execute(
                "SELECT count(*) FROM content_blobs"
            ).fetchone()[0],
            "sequence_nodes": connection.execute(
                "SELECT count(*) FROM sequence_nodes"
            ).fetchone()[0],
        }
    return runner.exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
