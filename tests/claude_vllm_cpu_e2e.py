#!/usr/bin/env python3
"""Opt-in Claude Code test against a running local CPU inference server.

Build with cargo build --locked, start vLLM with an instruction/tool model and
--enable-auto-tool-choice --tool-call-parser hermes, then run:
python3 tests/claude_vllm_cpu_e2e.py --upstream http://127.0.0.1:18000/v1 \
    --model qwen-cpu
For llama.cpp (started with --jinja), use --provider openai. For Ollama's
OpenAI-compatible endpoint, also select --protocols chat.
Uses isolated Claude settings/workspace and fresh gateway/log databases. The
default tool test reads a random nonce through a local stdio MCP server; use
--tool-mode read for the more demanding built-in Read tool. Does not use cloud
credentials. Reports and CLI transcripts go under ignored target/.
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

from e2e_helpers import free_port, http


def wait_for(predicate, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.1)
    raise AssertionError("condition did not become true")


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
        "--protocols",
        nargs="+",
        choices=["native", "chat", "responses"],
        default=["native", "chat"],
    )
    args = parser.parse_args()
    upstream = urllib.parse.urlsplit(args.upstream)
    if upstream.hostname not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("This test only accepts a loopback inference server")
    if not shutil.which("claude"):
        parser.error("Claude Code must be installed")
    output = args.output.resolve() / str(uuid.uuid4())
    output.mkdir(parents=True, mode=0o700)
    workspace = output / "workspace"
    workspace.mkdir()
    nonce = "LOCAL_NONCE_" + uuid.uuid4().hex[:12]
    fixture = workspace / "nonce.json"
    fixture.write_text(json.dumps({"nonce": nonce}) + "\n")
    public, admin = free_port(), free_port()
    while admin == public:
        admin = free_port()
    base = f"http://127.0.0.1:{public}"
    control = f"http://127.0.0.1:{admin}"
    report = {
        "upstream": args.upstream,
        "model": args.model,
        "provider": args.provider,
        "claude_version": subprocess.check_output(
            ["claude", "--version"], text=True
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
            detail = fn()
            item = {"name": name, "passed": True, "details": detail}
        except Exception as error:  # noqa: BLE001 - preserve independent failures
            item = {"name": name, "passed": False, "error": str(error)}
        item["seconds"] = round(time.monotonic() - started, 3)
        report["checks"].append(item)
        save()
        print(json.dumps(item, ensure_ascii=False), flush=True)
        return item["passed"]

    def success(url, body=None, headers=None):
        status, _, text = http(url, body, headers)
        assert 200 <= status < 300, (status, text[:2000])
        return json.loads(text)

    def requests(session):
        query = urllib.parse.urlencode({"since": 0, "session": session, "limit": 100})
        return success(control + "/admin/api/logs/requests?" + query)["requests"]

    def details(session, minimum=1):
        rows = wait_for(
            lambda: (
                (r if len(r) >= minimum and all(x["ended_at_ms"] for x in r) else None)
                if (r := requests(session))
                else None
            )
        )
        return [success(control + "/admin/api/logs/requests/" + r["id"]) for r in rows]

    def claude(protocol, tools=False):
        alias = "cpu-" + protocol
        session = alias + ("-tools" if tools else "-text")
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("ANTHROPIC_", "CLAUDE_", "ESTUARY_"))
            and k != "MAX_THINKING_TOKENS"
        }
        env.update(
            {
                "ANTHROPIC_BASE_URL": base,
                "ANTHROPIC_AUTH_TOKEN": "local-test-only",
                "ANTHROPIC_API_KEY": "local-test-only",
                "ANTHROPIC_MODEL": alias,
                "ANTHROPIC_DEFAULT_HAIKU_MODEL": alias,
                "ANTHROPIC_DEFAULT_SONNET_MODEL": alias,
                "ANTHROPIC_DEFAULT_OPUS_MODEL": alias,
                "ANTHROPIC_CUSTOM_HEADERS": "x-estuary-session-id: " + session,
                "CLAUDE_CONFIG_DIR": str(output / "claude-config"),
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                "CLAUDE_CODE_MAX_OUTPUT_TOKENS": "256",
                "MAX_THINKING_TOKENS": "0",
                "NO_PROXY": "127.0.0.1,localhost,::1",
            }
        )
        mcp = tools and args.tool_mode == "mcp"
        tool_name = "mcp__nonce__read" if mcp else "Read"
        mcp_config = {"mcpServers": {}}
        if mcp:
            mcp_config["mcpServers"]["nonce"] = {
                "command": sys.executable,
                "args": [
                    str(Path(__file__).resolve().parent / "fixtures" / "mcp_nonce.py"),
                    str(fixture),
                ],
            }
        prompt = "Reply exactly CPU_OK."
        if tools:
            prompt = (
                "Call mcp__nonce__read exactly once with an empty JSON object {}. "
                "It reads the local file for you and requires no file path. "
                "After the tool returns, reply only with its exact nonce string."
                if mcp
                else f"Use Read to read {fixture}. Return the value of its JSON nonce field. Ignore line numbers."
            )
        command = [
            "claude",
            "-p",
            prompt,
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
        if tools:
            command.extend(
                ["--allowedTools", tool_name, "--permission-mode", "dontAsk"]
            )
        transcript = output / (session + ".jsonl")
        diagnostics = output / (session + ".stderr")
        with transcript.open("w") as out, diagnostics.open("w") as err:
            process = subprocess.Popen(
                command,
                cwd=workspace,
                env=env,
                stdout=out,
                stderr=err,
                start_new_session=True,
            )
            try:
                process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                raise AssertionError(
                    f"Claude timed out after {args.timeout}s; see {session}.jsonl"
                ) from None
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
        stdout, stderr = transcript.read_text(), diagnostics.read_text()
        events = [
            json.loads(line) for line in stdout.splitlines() if line.startswith("{")
        ]
        result = next((e for e in reversed(events) if e.get("type") == "result"), None)
        if protocol == "responses":
            assert process.returncode == 1 and result and result.get("is_error"), result
            assert "output effort cannot be represented losslessly" in result.get(
                "result", ""
            ), result
            rows = details(session)
            assert all(
                r["request"]["http_status"] == 400
                and r["request"]["outcome"] == "error"
                for r in rows
            ), rows
            return {
                "expected_unsupported_feature": "Claude Code output effort",
                "requests": len(rows),
            }
        assert process.returncode == 0 and result and not result.get("is_error"), (
            process.returncode,
            result,
            stderr[-2000:],
        )
        expected = nonce if tools else "CPU_OK"
        assert expected in result.get("result", ""), result
        rows = details(session, minimum=2 if tools else 1)
        for detail in rows:
            request = detail["request"]
            assert request["http_status"] == 200 and request["outcome"] == "success", (
                request
            )
            assert (
                request["protocol"] == "anthropic_messages" and request["streaming"]
            ), request
            assert request["usage"].get("output_tokens", 0) > 0, request
            assert request["delivery"] == "body_consumed", request
            assert request["timings_us"].get("first_output", 0) > 0, request
            assert request["timings_us"].get("total", 0) > 0, request
            raw = request["usage"].get("raw", {})
            created = raw.get("prompt_tokens_details", {}).get("created_cache_tokens")
            if created is not None:
                assert request["usage"]["cache_write_tokens"] == created, request
            assert len(request["attempts"]) == 1, request
            assert (
                request["attempts"][0]["adapter"]
                == {
                    "native": "native_anthropic",
                    "chat": "chat_to_anthropic",
                    "responses": "responses_to_anthropic",
                }[protocol]
            ), request
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
            assert any(
                b.get("type") == "tool_result" and nonce in json.dumps(b)
                for body in inputs
                for message in body["messages"]
                if isinstance(message["content"], list)
                for b in message["content"]
            ), inputs
        return {
            "requests": len(rows),
            "result": result.get("result"),
            "usage": [r["request"]["usage"] for r in rows],
            "timings_us": [r["request"]["timings_us"] for r in rows],
        }

    log = (output / "gateway.log").open("w")
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
        for protocol in args.protocols:
            node = success(
                control + "/admin/api/nodes",
                {
                    "id": "cpu-" + protocol,
                    "base_url": args.upstream,
                    "models": {"cpu-" + protocol: args.model},
                    "max_concurrency": 2,
                    "provider": {
                        "type": args.provider,
                        "anthropic_protocol": protocol,
                    },
                },
            )
            report["provider_version"] = node["runtime"]["provider_version"]
            if args.provider == "vllm":
                report["vllm_version"] = report["provider_version"]
        save()
        for protocol in args.protocols:
            if protocol == "responses":
                check(
                    "Claude Code unsupported Responses output effort is rejected",
                    lambda: claude("responses"),
                )
                continue
            check("Claude Code streaming " + protocol, lambda p=protocol: claude(p))
            if not args.skip_tools:
                check(
                    f"Claude Code {args.tool_mode} tool roundtrip " + protocol,
                    lambda p=protocol: claude(p, True),
                )

        def logger_health():
            expected_sessions = []
            for protocol in args.protocols:
                expected_sessions.append("cpu-" + protocol + "-text")
                if protocol != "responses" and not args.skip_tools:
                    expected_sessions.append("cpu-" + protocol + "-tools")
            # Wait for final rows even when the CLI check failed before it could
            # inspect logs. Fast JSON errors can precede the writer's next flush.
            records = [
                detail["request"]
                for session in expected_sessions
                for detail in details(session)
            ]
            status = success(control + "/admin/api/logs/status")
            assert (
                status["available"]
                and status["dropped"] == 0
                and status["write_errors"] == 0
            ), status
            sessions = success(control + "/admin/api/logs/sessions?since=0")
            assert set(expected_sessions) <= {
                session["id"] for session in sessions["sessions"]
            }, sessions
            return {"status": status, "sessions": sessions, "requests": records}

        check("logger health and session grouping", logger_health)

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
        log.close()
        with sqlite3.connect(output / "logs.db") as connection:
            report["storage"] = {
                "content_blobs": connection.execute(
                    "SELECT count(*) FROM content_blobs"
                ).fetchone()[0],
                "sequence_nodes": connection.execute(
                    "SELECT count(*) FROM sequence_nodes"
                ).fetchone()[0],
            }
        save()
        print("Report:", output / "report.json", flush=True)
    return 0 if all(c["passed"] for c in report["checks"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
