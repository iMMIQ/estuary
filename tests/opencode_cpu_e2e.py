#!/usr/bin/env python3
"""Opt-in real OpenCode v2 Chat and Responses tests through a local CPU server.

Start llama.cpp, build Estuary, then run:
python3 tests/opencode_cpu_e2e.py --upstream http://127.0.0.1:18000/v1 --model qwen35-cpu
Uses an owned loopback OpenCode server, isolated XDG state and a small custom agent.
Only the local MCP nonce tool is allowed. Reports go under ignored target/.
"""

import argparse
import base64
import json
import os
import sqlite3
import subprocess
import sys
import urllib.parse
from collections import Counter
from pathlib import Path

from e2e_helpers import (
    MCP_NONCE_SCRIPT,
    E2ERunner,
    GatewayContext,
    free_port,
    http_json,
    run_process_group,
    stop_process_group,
    validate_loopback_cli,
    wait_for,
)


def audit_storage(path):
    """Check persisted payload DAG integrity and real prefix sharing."""
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
        blobs = db.execute(
            "SELECT hash, refs, blob_refs, sequence_refs FROM content_blobs"
        ).fetchall()
        sequences = db.execute(
            "SELECT hash, refs, previous_hash, item_hash, item_count FROM sequence_nodes"
        ).fetchall()
        expected_blobs = Counter(
            h for (h,) in db.execute("SELECT root_hash FROM payloads")
        )
        expected_sequences = Counter()
        for _, _, blob_refs, sequence_refs in blobs:
            expected_blobs.update(json.loads(blob_refs))
            expected_sequences.update(json.loads(sequence_refs))
        for _, _, previous, item, _ in sequences:
            expected_blobs[item] += 1
            if previous:
                expected_sequences[previous] += 1
        assert all(refs == expected_blobs[h] for h, refs, _, _ in blobs)
        assert all(refs == expected_sequences[h] for h, refs, _, _, _ in sequences)
        assert set(expected_blobs) == {h for h, _, _, _ in blobs}
        assert set(expected_sequences) == {h for h, _, _, _, _ in sequences}
        # Responses can start with one user item (instructions are a scalar),
        # whereas Chat starts with system + user. Either prefix is reusable.
        shared = sum(refs > 1 and count >= 1 for _, refs, _, _, count in sequences)
        assert shared > 0, "No shared message prefix in real tool history"
        return {
            "integrity_check": "ok",
            "foreign_key_violations": 0,
            "reference_counts": "exactly match payload and DAG edges",
            "content_blobs": len(blobs),
            "sequence_nodes": len(sequences),
            "shared_prefix_nodes": shared,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--provider", choices=["openai", "vllm"], default="openai")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/estuary"))
    parser.add_argument(
        "--output", type=Path, default=Path("target/opencode-cpu-e2e/runs")
    )
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument(
        "--protocols",
        nargs="+",
        choices=["chat", "responses"],
        default=["chat", "responses"],
    )
    args = parser.parse_args()
    validate_loopback_cli(parser, args.upstream, "opencode", "OpenCode CLI")
    runner = E2ERunner(
        args.output,
        {
            "upstream": args.upstream,
            "model": args.model,
            "provider": args.provider,
            "opencode_version": subprocess.check_output(
                ["opencode", "--version"], text=True
            ).strip(),
            "protocols": args.protocols,
        },
    )

    with GatewayContext(args.binary, runner.output) as gateway:
        gateway.register_node(
            "cpu-opencode",
            args.upstream,
            {"cpu-opencode": args.model},
            provider=args.provider,
        )

        def opencode(protocol="chat", tools=False):
            session = f"opencode-{protocol}-{'tools' if tools else 'text'}"
            case = runner.output / session
            case.mkdir()
            workspace = case / "workspace"
            workspace.mkdir()
            rules = [{"action": "*", "resource": "*", "effect": "deny"}]
            if tools:
                rules.append({"action": "nonce_read", "resource": "*", "effect": "allow"})
            config = {
                "model": "estuary_cpu_e2e/cpu-opencode",
                "default_agent": "e2e",
                "update": "disable",
                "compaction": {"auto": False},
                "providers": {
                    "estuary_cpu_e2e": {
                        "name": "Estuary local CPU E2E",
                        "env": ["ESTUARY_LOCAL_TEST_KEY"],
                        "package": (
                            "@opencode/ai/providers/openai/responses"
                            if protocol == "responses"
                            else "@opencode/ai/providers/openai-compatible"
                        ),
                        "settings": {"baseURL": gateway.v1_url},
                        "headers": {"x-estuary-session-id": session},
                        "models": {
                            "cpu-opencode": {
                                "limit": {"context": 16384, "output": 256},
                                "body": {"tool_choice": "auto"} if tools else {},
                                "capabilities": {
                                    "tools": True,
                                    "input": ["text"],
                                    "output": ["text"],
                                },
                            }
                        },
                    }
                },
                "agents": {
                    "e2e": {
                        "mode": "primary",
                        "system": (
                            "You are a helpful assistant. When asked to use nonce_read, call it with {}. Never invent its result. After the tool returns, reply only with the exact text it returned."
                            if tools
                            else "You are a helpful assistant. Be concise."
                        ),
                        "permissions": rules,
                        "steps": 3,
                    }
                },
                "mcp": {"servers": {}},
            }
            if tools:
                config["mcp"]["servers"]["nonce"] = {
                    "type": "local",
                    "codemode": False,
                    "command": [
                        sys.executable,
                        str(MCP_NONCE_SCRIPT),
                        str(runner.fixture),
                    ],
                }
            (workspace / "opencode.json").write_text(json.dumps(config, indent=2))
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(
                    ("OPENAI_", "ANTHROPIC_", "ESTUARY_", "CODEX_", "CLAUDE_", "OPENCODE_")
                )
            }
            env.pop("PWD", None)
            for key in ["CONFIG", "DATA", "CACHE", "STATE"]:
                env[f"XDG_{key}_HOME"] = str(case / key.lower())
            global_config = case / "config/opencode"
            global_config.mkdir(parents=True)
            (global_config / "opencode.json").write_text(json.dumps(config, indent=2))
            env.update(
                ESTUARY_LOCAL_TEST_KEY="local-test-only",
                OPENCODE_PASSWORD="local-test-only",
                NO_PROXY="127.0.0.1,localhost,::1",
            )
            cli_port = free_port()
            cli_base = f"http://127.0.0.1:{cli_port}"
            query = urllib.parse.urlencode({"directory": str(workspace)})
            cli_headers = {
                "Authorization": "Basic "
                + base64.b64encode(b"opencode:local-test-only").decode()
            }
            stdout_path = runner.output / (session + ".jsonl")
            stderr_path = runner.output / (session + ".stderr")
            with (runner.output / (session + "-server.log")).open("w") as server_log:
                server = subprocess.Popen(
                    [
                        "opencode",
                        "serve",
                        "--hostname",
                        "127.0.0.1",
                        "--port",
                        str(cli_port),
                        "--print-logs",
                    ],
                    cwd=workspace,
                    env=env,
                    stdout=server_log,
                    stderr=server_log,
                    start_new_session=True,
                )
                try:

                    def agents_ready():
                        try:
                            data = http_json(
                                cli_base + "/api/agent?" + query, headers=cli_headers
                            )["data"]
                            return data if any(a["id"] == "e2e" for a in data) else None
                        except (OSError, AssertionError):
                            return None

                    wait_for(agents_ready)
                    if tools:

                        def mcp_ready():
                            data = http_json(
                                cli_base + "/api/mcp?" + query, headers=cli_headers
                            )["data"]
                            return (
                                data
                                if any(
                                    s["name"] == "nonce"
                                    and s["status"]["status"] == "connected"
                                    for s in data
                                )
                                else None
                            )

                        wait_for(mcp_ready)
                    command = [
                        "opencode",
                        "run",
                        "--server",
                        cli_base,
                        "--format",
                        "json",
                        "--model",
                        "estuary_cpu_e2e/cpu-opencode",
                        "--agent",
                        "e2e",
                        "--title",
                        session,
                        "Call nonce_read exactly once with {}. It reads the local file for you and requires no file path. After it returns, reply only with its exact nonce string."
                        if tools
                        else "Reply exactly CPU_OK. Do not use any tools.",
                    ]
                    returncode = run_process_group(
                        command,
                        cwd=workspace,
                        env=env,
                        stdout_path=stdout_path,
                        stderr_path=stderr_path,
                        timeout=args.timeout,
                        label="OpenCode",
                    )
                finally:
                    stop_process_group(server)
            events = [
                json.loads(s)
                for s in stdout_path.read_text().splitlines()
                if s.startswith("{")
            ]
            assert returncode == 0, (events[-5:], stderr_path.read_text()[-3000:])
            assert not any(e.get("type") == "error" for e in events), events
            result = "".join(
                e.get("part", {}).get("text", "")
                for e in events
                if e.get("type") == "text"
            ).strip()
            assert result == (runner.nonce if tools else "CPU_OK"), (
                result,
                events[-5:],
            )
            rows = gateway.session_details(session, sort_by_start=True)
            assert len(rows) >= (2 if tools else 1), rows
            for d in rows:
                r = d["request"]
                assert r["protocol"] == (
                    "openai_responses" if protocol == "responses" else "openai"
                ) and r["endpoint"] == (
                    "/v1/responses"
                    if protocol == "responses"
                    else "/v1/chat/completions"
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
                assert r["attempts"] and all(
                    a["endpoint"]
                    == ("responses" if protocol == "responses" else "chat/completions")
                    for a in r["attempts"]
                ), r
            if tools:
                calls = [e["part"] for e in events if e.get("type") == "tool_use"]
                assert len(calls) == 1 and calls[0].get("tool") == "nonce_read", calls
                assert calls[0].get("state", {}).get("status") == "completed", calls
                assert calls[0]["state"]["input"] == {}, calls
                assert calls[0]["state"]["output"] == runner.nonce, calls
                bodies = [
                    p["content"]
                    for d in rows
                    for p in d["payloads"]
                    if p["stage"] == "client_input"
                ]
                assert runner.nonce not in json.dumps(bodies[0]), (
                    "Nonce leaked into initial model prompt"
                )
                assert any(
                    runner.nonce in json.dumps(i)
                    and (
                        i.get("type") == "function_call_output"
                        or i.get("role") == "tool"
                    )
                    for b in bodies
                    for i in b.get("input", b.get("messages", []))
                    if isinstance(i, dict)
                ), bodies
            return {
                "result": result,
                "requests": len(rows),
                "actual_mcp_calls": len(calls) if tools else 0,
                "usage": [d["request"]["usage"] for d in rows],
                "timings_us": [d["request"]["timings_us"] for d in rows],
            }

        for protocol in args.protocols:
            runner.check(
                f"OpenCode {protocol} streamed text",
                lambda protocol=protocol: opencode(protocol),
            )
            runner.check(
                f"OpenCode {protocol} MCP nonce roundtrip",
                lambda protocol=protocol: opencode(protocol, True),
            )
        sessions = [
            f"opencode-{p}-{t}" for p in args.protocols for t in ["text", "tools"]
        ]
        runner.check(
            "session logger health",
            lambda: gateway.check_logger_health(sessions, count_only=True),
        )
        runner.check(
            "scheduler releases all reservations", gateway.check_scheduler_idle
        )

    runner.check(
        "database integrity and shared history prefixes",
        lambda: audit_storage(runner.output / "logs.db"),
    )
    return runner.exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
