#!/usr/bin/env python3
"""Opt-in real Codex Responses test through Estuary to a local CPU server.

Start a Chat-compatible llama.cpp server, build Estuary, then run:
python3 tests/codex_cpu_e2e.py --upstream http://127.0.0.1:18000/v1 \
    --model qwen35-cpu
Uses --ignore-user-config, --ignore-rules, --ephemeral, a small replacement
instruction file, and a local MCP nonce fixture. Cloud credentials are excluded
from the process environment. Reports go under ignored target/.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from e2e_helpers import (
    MCP_NONCE_SCRIPT,
    E2ERunner,
    GatewayContext,
    run_process_group,
    sqlite_integrity,
    toml_value,
    validate_loopback_cli,
)
from image_fixtures import IMAGES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--provider", choices=["openai", "vllm"], default="openai")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/estuary"))
    parser.add_argument(
        "--output", type=Path, default=Path("target/codex-cpu-e2e/runs")
    )
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument(
        "--images",
        action="store_true",
        help="Test single/multiple PNG input and MCP image replay",
    )
    args = parser.parse_args()
    validate_loopback_cli(parser, args.upstream, "codex", "Codex CLI")
    runner = E2ERunner(
        args.output,
        {
            "upstream": args.upstream,
            "model": args.model,
            "provider": args.provider,
            "codex_version": subprocess.check_output(
                ["codex", "--version"], text=True
            ).strip(),
        },
    )
    image_paths = runner.write_images(IMAGES)
    instructions = runner.output / "instructions.txt"
    instructions.write_text(
        "You are a helpful assistant. Use tools when asked. Be concise.\n"
    )

    with GatewayContext(args.binary, runner.output) as gateway:
        gateway.register_node(
            "cpu-codex",
            args.upstream,
            {"cpu-codex": args.model},
            provider=args.provider,
        )

        def codex(tools=False, image_count=0):
            session = "codex-tools" if tools else "codex-text"
            if image_count:
                session += f"-images-{image_count}"
            settings = {
                "model_provider": "estuary_cpu_e2e",
                "model_providers.estuary_cpu_e2e.name": "Estuary local CPU E2E",
                "model_providers.estuary_cpu_e2e.base_url": gateway.v1_url,
                "model_providers.estuary_cpu_e2e.wire_api": "responses",
                "model_providers.estuary_cpu_e2e.requires_openai_auth": False,
                "model_providers.estuary_cpu_e2e.env_key": "ESTUARY_LOCAL_TEST_KEY",
                "model_providers.estuary_cpu_e2e.supports_websockets": False,
                "model_providers.estuary_cpu_e2e.http_headers": {
                    "x-estuary-session-id": session
                },
                "model_providers.estuary_cpu_e2e.request_max_retries": 0,
                "model_providers.estuary_cpu_e2e.stream_max_retries": 0,
                "model_reasoning_effort": "none",
                "model_context_window": 16384,
                "model_instructions_file": str(instructions),
                "web_search": "disabled",
                "approval_policy": "never",
                "project_doc_max_bytes": 0,
                "project_root_markers": [],
                "sqlite_home": str(runner.output / "codex-state"),
                "log_dir": str(runner.output / "codex-log"),
                "features.shell_tool": False,
                "features.multi_agent": False,
                "features.apps": False,
                "features.code_mode_host": False,
                "features.enable_request_compression": False,
                "features.shell_snapshot": False,
                "features.hooks": False,
                "features.sleep_tool": False,
                "features.goals": False,
            }
            if tools:
                tool_name = "images" if image_count else "read"
                settings.update(
                    {
                        "mcp_servers.nonce.command": sys.executable,
                        "mcp_servers.nonce.args": [
                            str(MCP_NONCE_SCRIPT),
                            str(runner.fixture),
                            *(
                                [str(path) for path in image_paths]
                                if image_count
                                else []
                            ),
                        ],
                        "mcp_servers.nonce.enabled_tools": [tool_name],
                        f"mcp_servers.nonce.tools.{tool_name}.approval_mode": "approve",
                    }
                )
            final = runner.output / (session + "-final.txt")
            command = [
                "codex",
                "exec",
                "--ignore-user-config",
                "--ignore-rules",
                "--ephemeral",
                "--skip-git-repo-check",
                "-C",
                str(runner.workspace),
                "-s",
                "read-only",
                "--json",
                "-m",
                "cpu-codex",
                "-o",
                str(final),
            ]
            for key, value in settings.items():
                command += ["-c", key + "=" + toml_value(value)]
            prompt = (
                "Call mcp__nonce__read exactly once with {}. It reads the local file for you and requires no file path. After it returns, reply only with its exact nonce string."
                if tools
                else "Reply exactly CPU_OK. Do not use any tools."
            )
            if image_count:
                if tools:
                    prompt = f"CODEX_IMAGE_TOOL_E2E count={image_count}. Call the nonce images tool exactly once with count={image_count}. After it returns, reply only with its exact nonce string."
                else:
                    prompt = f"CODEX_IMAGE_INPUT_E2E count={image_count}. Reply exactly IMAGE_OK_{image_count}."
                    for path in image_paths[:image_count]:
                        command += ["--image", str(path)]
            # --image accepts multiple paths and otherwise consumes the prompt too.
            command += ["--", prompt]
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(
                    ("OPENAI_", "ANTHROPIC_", "ESTUARY_", "CODEX_", "CLAUDE_")
                )
            }
            env.update(
                ESTUARY_LOCAL_TEST_KEY="local-test-only",
                NO_PROXY="127.0.0.1,localhost,::1",
                CODEX_HOME=str(runner.output / "codex-home"),
            )
            Path(env["CODEX_HOME"]).mkdir(exist_ok=True)
            stdout_path = runner.output / (session + ".jsonl")
            stderr_path = runner.output / (session + ".stderr")
            returncode = run_process_group(
                command,
                cwd=runner.workspace,
                env=env,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                timeout=args.timeout,
                label="Codex",
            )
            events = [
                json.loads(s)
                for s in stdout_path.read_text().splitlines()
                if s.startswith("{")
            ]
            assert returncode == 0, (events[-5:], stderr_path.read_text()[-2000:])
            completed = next(
                (e for e in reversed(events) if e.get("type") == "turn.completed"), None
            )
            assert completed, events
            result = final.read_text().strip()
            expected = (
                runner.nonce
                if tools
                else f"IMAGE_OK_{image_count}"
                if image_count
                else "CPU_OK"
            )
            assert result == expected, result
            rows = gateway.session_details(session)
            assert len(rows) >= (2 if tools else 1), rows
            for d in rows:
                r = d["request"]
                assert (
                    r["protocol"] == "openai_responses"
                    and r["endpoint"] == "/v1/responses"
                ), r
                assert (
                    r["streaming"]
                    and r["http_status"] == 200
                    and r["outcome"] == "success"
                ), r
                assert r["delivery"] == "body_consumed", r
                assert (
                    r["usage"]["output_tokens"] > 0
                    and r["timings_us"]["first_output"] > 0
                ), r
                assert {p["stage"] for p in d["payloads"]} == {
                    "client_input",
                    "upstream_input",
                    "client_output",
                    "upstream_output",
                }, d
                assert len(r["attempts"]) == 1, r
                assert all(
                    a["endpoint"] == "chat/completions"
                    and a["adapter"] == "chat_to_responses"
                    for a in r["attempts"]
                ), r
            if tools:
                calls = [
                    e["item"]
                    for e in events
                    if e.get("type") == "item.completed"
                    and e.get("item", {}).get("type") == "mcp_tool_call"
                ]
                assert calls and all(x.get("status") == "completed" for x in calls), (
                    events
                )
                bodies = [
                    p["content"]
                    for d in rows
                    for p in d["payloads"]
                    if p["stage"] == "client_input"
                ]
                assert any(
                    i.get("type") == "function_call_output"
                    and runner.nonce in json.dumps(i)
                    for b in bodies
                    for i in b.get("input", [])
                    if isinstance(i, dict)
                ), bodies
            return {
                "result": result,
                "requests": len(rows),
                "usage": [d["request"]["usage"] for d in rows],
                "timings_us": [d["request"]["timings_us"] for d in rows],
                "client_usage": completed["usage"],
            }

        runner.check("Codex streamed Responses text", codex)
        runner.check(
            "Codex MCP tool roundtrip with Responses history", lambda: codex(True)
        )
        if args.images:
            for count in [1, 2]:
                runner.check(
                    f"Codex {count} image input through Chat",
                    lambda count=count: codex(image_count=count),
                )
                runner.check(
                    f"Codex MCP {count} image replay through Chat",
                    lambda count=count: codex(True, count),
                )
        sessions = ["codex-text", "codex-tools"]
        if args.images:
            sessions.extend(
                f"codex-{kind}-images-{count}"
                for count in [1, 2]
                for kind in ["text", "tools"]
            )
        runner.check(
            "session logger health", lambda: gateway.check_logger_health(sessions)
        )
        runner.check(
            "scheduler releases all reservations", gateway.check_scheduler_idle
        )

    runner.report["storage"] = sqlite_integrity(runner.output / "logs.db")
    return runner.exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
