#!/usr/bin/env python3
"""Opt-in real Codex Responses test through Estuary to a local CPU server.

Start a Responses-compatible llama.cpp server, build Estuary, then run:
python3 tests/codex_cpu_e2e.py --upstream http://127.0.0.1:18000/v1 \
    --model qwen35-cpu
Uses --ignore-user-config, --ignore-rules, --ephemeral, a small replacement
instruction file, and a local MCP nonce fixture. Cloud credentials are excluded
from the process environment. Reports go under ignored target/.
"""

import argparse
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import uuid
from pathlib import Path

from claude_vllm_cpu_e2e import wait_for
from deepseek_e2e import free_port, http


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--provider", choices=["openai", "vllm"], default="openai")
    parser.add_argument("--flatten-namespaces", action="store_true")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/estuary"))
    parser.add_argument(
        "--output", type=Path, default=Path("target/codex-cpu-e2e/runs")
    )
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    if urllib.parse.urlsplit(args.upstream).hostname not in {
        "localhost",
        "127.0.0.1",
        "::1",
    }:
        parser.error("This test only accepts a loopback inference server")
    if not shutil.which("codex"):
        parser.error("Codex CLI must be installed")
    output = args.output.resolve() / str(uuid.uuid4())
    output.mkdir(parents=True, mode=0o700)
    workspace = output / "workspace"
    workspace.mkdir()
    fixture = workspace / "nonce.json"
    nonce = "LOCAL_NONCE_" + uuid.uuid4().hex[:12]
    fixture.write_text(json.dumps({"nonce": nonce}) + "\n")
    instructions = output / "instructions.txt"
    instructions.write_text(
        "You are a helpful assistant. Use tools when asked. Be concise.\n"
    )
    public, admin = free_port(), free_port()
    while public == admin:
        admin = free_port()
    base, control = f"http://127.0.0.1:{public}/v1", f"http://127.0.0.1:{admin}"
    report = {
        "upstream": args.upstream,
        "model": args.model,
        "provider": args.provider,
        "flatten_codex_namespaces": args.flatten_namespaces,
        "codex_version": subprocess.check_output(
            ["codex", "--version"], text=True
        ).strip(),
        "checks": [],
    }

    def save():
        (output / "report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False)
        )

    def check(name, fn):
        started = time.monotonic()
        try:
            item = {"name": name, "passed": True, "details": fn()}
        except Exception as error:  # noqa: BLE001 - preserve independent test failures
            item = {"name": name, "passed": False, "error": str(error)}
        item["seconds"] = round(time.monotonic() - started, 3)
        report["checks"].append(item)
        save()
        print(json.dumps(item, ensure_ascii=False), flush=True)

    def success(url, body=None):
        status, _, text = http(url, body)
        assert 200 <= status < 300, (status, text[:2000])
        return json.loads(text)

    def details(session):
        def ended():
            query = urllib.parse.urlencode(
                {"since": 0, "session": session, "limit": 100}
            )
            rows = success(control + "/admin/api/logs/requests?" + query)["requests"]
            return rows if rows and all(r["ended_at_ms"] for r in rows) else None

        rows = wait_for(ended)
        return [
            success(control + "/admin/api/logs/requests/" + row["id"]) for row in rows
        ]

    def codex(tools=False):
        session = "codex-tools" if tools else "codex-text"
        settings = {
            "model_provider": "estuary_cpu_e2e",
            "model_providers.estuary_cpu_e2e.name": "Estuary local CPU E2E",
            "model_providers.estuary_cpu_e2e.base_url": base,
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
            "sqlite_home": str(output / "codex-state"),
            "log_dir": str(output / "codex-log"),
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
            settings.update(
                {
                    "mcp_servers.nonce.command": sys.executable,
                    "mcp_servers.nonce.args": [
                        str(Path(__file__).resolve().parent / "fixtures/mcp_nonce.py"),
                        str(fixture),
                    ],
                    "mcp_servers.nonce.enabled_tools": ["read"],
                    "mcp_servers.nonce.tools.read.approval_mode": "approve",
                }
            )

        def toml(value):
            if isinstance(value, dict):
                return (
                    "{"
                    + ", ".join(
                        json.dumps(k) + " = " + toml(v) for k, v in value.items()
                    )
                    + "}"
                )
            return json.dumps(value)

        final = output / (session + "-final.txt")
        command = [
            "codex",
            "exec",
            "--ignore-user-config",
            "--ignore-rules",
            "--ephemeral",
            "--skip-git-repo-check",
            "-C",
            str(workspace),
            "-s",
            "read-only",
            "--json",
            "-m",
            "cpu-codex",
            "-o",
            str(final),
        ]
        for key, value in settings.items():
            command += ["-c", key + "=" + toml(value)]
        command += [
            "Call mcp__nonce__read exactly once with {}. It reads the local file for you and requires no file path. After it returns, reply only with its exact nonce string."
            if tools
            else "Reply exactly CPU_OK. Do not use any tools."
        ]
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(
                ("OPENAI_", "ANTHROPIC_", "ESTUARY_", "CODEX_", "CLAUDE_")
            )
        }
        env.update(
            ESTUARY_LOCAL_TEST_KEY="local-test-only", NO_PROXY="127.0.0.1,localhost,::1"
        )
        with (
            (output / (session + ".jsonl")).open("w") as out,
            (output / (session + ".stderr")).open("w") as err,
        ):
            process = subprocess.Popen(
                command, env=env, stdout=out, stderr=err, start_new_session=True
            )
            try:
                process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                raise AssertionError(
                    f"Codex timed out after {args.timeout}s; see {session}.jsonl"
                ) from None
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
        events = [
            json.loads(s)
            for s in (output / (session + ".jsonl")).read_text().splitlines()
            if s.startswith("{")
        ]
        assert process.returncode == 0, (
            events[-5:],
            (output / (session + ".stderr")).read_text()[-2000:],
        )
        assert any(e.get("type") == "turn.completed" for e in events), events
        result = final.read_text().strip()
        assert result == (nonce if tools else "CPU_OK"), result
        rows = details(session)
        assert len(rows) >= (2 if tools else 1), rows
        for d in rows:
            r = d["request"]
            assert (
                r["protocol"] == "openai_responses" and r["endpoint"] == "/v1/responses"
            ), r
            assert (
                r["streaming"] and r["http_status"] == 200 and r["outcome"] == "success"
            ), r
            assert r["delivery"] == "body_consumed", r
            assert (
                r["usage"]["output_tokens"] > 0 and r["timings_us"]["first_output"] > 0
            ), r
            assert {p["stage"] for p in d["payloads"]} == {
                "client_input",
                "upstream_input",
                "client_output",
                "upstream_output",
            }, d
            assert all(a["endpoint"] == "responses" for a in r["attempts"]), r
        if tools:
            calls = [
                e["item"]
                for e in events
                if e.get("type") == "item.completed"
                and e.get("item", {}).get("type") == "mcp_tool_call"
            ]
            assert calls and all(x.get("status") == "completed" for x in calls), events
            bodies = [
                p["content"]
                for d in rows
                for p in d["payloads"]
                if p["stage"] == "client_input"
            ]
            assert any(
                i.get("type") == "function_call_output" and nonce in json.dumps(i)
                for b in bodies
                for i in b.get("input", [])
                if isinstance(i, dict)
            ), bodies
        return {
            "result": result,
            "requests": len(rows),
            "usage": [d["request"]["usage"] for d in rows],
            "timings_us": [d["request"]["timings_us"] for d in rows],
        }

    with (output / "gateway.log").open("w") as log:
        gateway = subprocess.Popen(
            [
                str(args.binary.resolve()),
                "--database",
                str(output / "control.db"),
                "--listen",
                f"127.0.0.1:{public}",
                "--admin-listen",
                f"127.0.0.1:{admin}",
                "--session-log-database",
                str(output / "logs.db"),
            ],
            stdout=log,
            stderr=log,
            start_new_session=True,
            env={k: v for k, v in os.environ.items() if not k.startswith("ESTUARY_")},
        )
        try:

            def ready():
                try:
                    return success(control + "/health/live")
                except OSError:
                    return None

            wait_for(ready)
            success(
                control + "/admin/api/nodes",
                {
                    "id": "cpu-codex",
                    "base_url": args.upstream,
                    "models": {"cpu-codex": args.model},
                    "max_concurrency": 1,
                    "provider": {
                        "type": args.provider,
                        "flatten_codex_namespaces": args.flatten_namespaces,
                    },
                },
            )
            check("Codex streamed Responses text", lambda: codex())
            check(
                "Codex MCP tool roundtrip with Responses history", lambda: codex(True)
            )

            def logger():
                records = [
                    d["request"]
                    for s in ["codex-text", "codex-tools"]
                    for d in details(s)
                ]
                status = success(control + "/admin/api/logs/status")
                assert (
                    status["available"]
                    and status["write_errors"] == 0
                    and status["dropped"] == 0
                ), status
                return {"status": status, "requests": records}

            check("session logger health", logger)

            def idle():
                nodes = success(control + "/admin/api/nodes")["nodes"]
                assert all(
                    n["runtime"]["active"] == 0
                    and n["runtime"]["pending_prefill_tokens"] == 0
                    and n["runtime"]["pending_decode_tokens"] == 0
                    for n in nodes
                ), nodes
                return {"nodes": len(nodes)}

            check("scheduler releases all reservations", idle)
        finally:
            os.killpg(gateway.pid, signal.SIGTERM)
            try:
                gateway.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(gateway.pid, signal.SIGKILL)
                gateway.wait()
    with sqlite3.connect(output / "logs.db") as db:
        report["storage"] = {
            "integrity_check": db.execute("PRAGMA integrity_check").fetchone()[0],
            "foreign_key_violations": len(
                db.execute("PRAGMA foreign_key_check").fetchall()
            ),
        }
    save()
    print("Report:", output / "report.json", flush=True)
    return 0 if all(c["passed"] for c in report["checks"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
