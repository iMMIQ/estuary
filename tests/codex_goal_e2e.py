#!/usr/bin/env python3
"""Exercise Codex 0.170 goals through Estuary and a deterministic Chat backend.

Uses the installed Codex app-server, native goal lifecycle, autonomous next turn,
and an actual MCP nonce read. No cloud credentials or model downloads are used.
Build Estuary first with cargo build --locked. Reports remain under target/.
"""

import argparse
import json
import os
import queue
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from claude_mock_e2e import MockLlm, tool_reply
from claude_vllm_cpu_e2e import wait_for
from e2e_helpers import free_port, http


class Rpc:
    def __init__(self, process, output):
        self.process = process
        self.output = output
        self.inbox = queue.Queue()
        self.events = []
        self.serial = 0
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        with self.output.open("w") as log:
            for line in self.process.stdout:
                log.write(line)
                log.flush()
                self.inbox.put(json.loads(line))
        self.inbox.put(None)

    def send(self, value):
        self.process.stdin.write(json.dumps(value) + "\n")
        self.process.stdin.flush()

    def receive(self, timeout=30):
        value = self.inbox.get(timeout=timeout)
        assert value is not None, "Codex app-server exited unexpectedly"
        self.events.append(value)
        assert not ("method" in value and "id" in value), value
        return value

    def request(self, method, params):
        self.serial += 1
        request_id = self.serial
        self.send({"id": request_id, "method": method, "params": params})
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            value = self.receive(max(0.1, deadline - time.monotonic()))
            if value.get("id") == request_id:
                assert "error" not in value, value
                return value["result"]
        raise AssertionError(f"RPC timed out: {method}")


def mock_reply(body):
    messages = body["messages"]
    tools = [tool["function"] for tool in body.get("tools", [])]
    call_ids = set()
    for message in messages:
        if message["role"] == "assistant":
            call_ids.update(call["id"] for call in message.get("tool_calls", []))
        if message["role"] == "tool":
            assert message.get("tool_call_id") in call_ids, (
                "Chat tool results must answer a preceding tool call",
                message,
            )
    if "BUDGET_LIMIT_E2E" in json.dumps(messages):
        return {"role": "assistant", "content": "BUDGET_STEP"}, "stop", "budget_step"

    def call(name, arguments):
        matches = [tool["name"] for tool in tools if tool["name"] == name]
        if not matches and name == "nonce":
            matches = [
                tool["name"]
                for tool in tools
                if "Read and return the nonce" in tool.get("description", "")
            ]
        assert len(matches) == 1, (name, [tool["name"] for tool in tools])
        return tool_reply(body, matches[0], arguments)

    results = [message for message in messages if message["role"] == "tool"]
    assistant = [message for message in messages if message["role"] == "assistant"]
    if not any(
        call["function"]["name"] == "get_goal"
        for message in assistant
        for call in message.get("tool_calls", [])
    ):
        return call("get_goal", {})
    if not any(message.get("content") == "GOAL_STEP_READY" for message in assistant):
        return {"role": "assistant", "content": "GOAL_STEP_READY"}, "stop", "first_turn"
    nonce = re.search(r"LOCAL_NONCE_[0-9a-f]+", json.dumps(results))
    if not nonce:
        return call("nonce", {})
    if not any(
        call["function"]["name"] == "update_goal"
        for message in assistant
        for call in message.get("tool_calls", [])
    ):
        return call("update_goal", {"status": "complete"})
    return (
        {"role": "assistant", "content": "GOAL_OK " + nonce[0]},
        "stop",
        "goal_complete",
    )


def stop(process):
    if process is not None and process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/estuary"))
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument(
        "--output", type=Path, default=Path("target/codex-goal-e2e/runs")
    )
    args = parser.parse_args()
    version = subprocess.check_output([args.codex, "--version"], text=True).strip()
    assert version == "codex-cli 0.170.0", version
    output = args.output.resolve() / str(uuid.uuid4())
    output.mkdir(parents=True, mode=0o700)
    workspace = output / "workspace"
    workspace.mkdir()
    nonce = "LOCAL_NONCE_" + uuid.uuid4().hex[:12]
    fixture = workspace / "nonce.json"
    fixture.write_text(json.dumps({"nonce": nonce}) + "\n")
    instructions = output / "instructions.txt"
    instructions.write_text(
        "Be concise. Use goal tools and the nonce MCP tool as instructed.\n"
    )
    report = {"codex_version": version, "checks": [], "passed": False}
    gateway = codex = None
    server = MockLlm(output, model="mock-codex-goal", reply=mock_reply)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    public, admin = free_port(), free_port()
    while admin == public:
        admin = free_port()
    base = f"http://127.0.0.1:{public}/v1"
    control = f"http://127.0.0.1:{admin}"
    session = "codex-native-goal"

    def success(url, body=None):
        status, _, text = http(url, body)
        assert 200 <= status < 300, (status, text[:2000])
        return json.loads(text)

    def check(name, details):
        report["checks"].append({"name": name, "passed": True, "details": details})
        print(json.dumps(report["checks"][-1], ensure_ascii=False), flush=True)

    try:
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
                env={
                    k: v for k, v in os.environ.items() if not k.startswith("ESTUARY_")
                },
            )

        def ready():
            try:
                return success(control + "/health/live")
            except OSError:
                return None

        wait_for(ready)
        success(
            control + "/admin/api/nodes",
            {
                "id": "goal",
                "base_url": f"http://127.0.0.1:{server.server_port}/v1",
                "models": {"goal-test": "mock-codex-goal"},
                "max_concurrency": 1,
                "provider": {"type": "openai"},
            },
        )
        settings = {
            "model_provider": "estuary_goal_e2e",
            "model_providers.estuary_goal_e2e.name": "Estuary goal E2E",
            "model_providers.estuary_goal_e2e.base_url": base,
            "model_providers.estuary_goal_e2e.wire_api": "responses",
            "model_providers.estuary_goal_e2e.requires_openai_auth": False,
            "model_providers.estuary_goal_e2e.env_key": "ESTUARY_LOCAL_TEST_KEY",
            "model_providers.estuary_goal_e2e.supports_websockets": False,
            "model_providers.estuary_goal_e2e.http_headers": {
                "x-estuary-session-id": session
            },
            "model_providers.estuary_goal_e2e.request_max_retries": 0,
            "model_providers.estuary_goal_e2e.stream_max_retries": 0,
            "model": "goal-test",
            "model_reasoning_effort": "none",
            "model_context_window": 32768,
            "model_instructions_file": str(instructions),
            "web_search": "disabled",
            "approval_policy": "never",
            "project_doc_max_bytes": 0,
            "project_root_markers": [],
            "sqlite_home": str(output / "codex-state"),
            "log_dir": str(output / "codex-log"),
            "features.goals": True,
            "features.shell_tool": False,
            "features.multi_agent": False,
            "features.multi_agent_v2": False,
            "features.apps": False,
            "features.code_mode_host": False,
            "features.enable_request_compression": False,
            "features.shell_snapshot": False,
            "features.hooks": False,
            "mcp_servers.nonce.command": sys.executable,
            "mcp_servers.nonce.args": [
                str(Path(__file__).resolve().parent / "fixtures/mcp_nonce.py"),
                str(fixture),
            ],
            "mcp_servers.nonce.enabled_tools": ["read"],
            "mcp_servers.nonce.tools.read.approval_mode": "approve",
        }

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

        command = [args.codex, "app-server", "--stdio"]
        for key, value in settings.items():
            command += ["-c", key + "=" + toml(value)]
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(
                ("OPENAI_", "ANTHROPIC_", "ESTUARY_", "CODEX_", "CLAUDE_")
            )
        }
        env.update(
            CODEX_HOME=str(output / "codex-home"),
            ESTUARY_LOCAL_TEST_KEY="local-test-only",
            NO_PROXY="127.0.0.1,localhost,::1",
        )
        Path(env["CODEX_HOME"]).mkdir()
        with (output / "codex.stderr").open("w") as err:
            codex = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=err,
                text=True,
                bufsize=1,
                env=env,
                cwd=workspace,
                start_new_session=True,
            )
        rpc = Rpc(codex, output / "codex.jsonl")
        initialized = rpc.request(
            "initialize",
            {
                "clientInfo": {"name": "estuary_goal_e2e", "version": "0.1"},
                "capabilities": {"experimentalApi": True},
            },
        )
        rpc.send({"method": "initialized"})
        thread = rpc.request(
            "thread/start",
            {
                "cwd": str(workspace),
                "model": "goal-test",
                "modelProvider": "estuary_goal_e2e",
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "ephemeral": False,
            },
        )["thread"]["id"]
        check("Codex 0.170 app-server connected", initialized)
        assert rpc.request("thread/goal/get", {"threadId": thread}).get("goal") is None
        goal = rpc.request(
            "thread/goal/set",
            {
                "threadId": thread,
                "objective": "Inspect this goal with get_goal and end the first turn with GOAL_STEP_READY, leaving the goal active. In an autonomous continuation, read the nonce through MCP, mark the goal complete with update_goal, then report GOAL_OK followed by the exact nonce.",
                "status": "paused",
                "tokenBudget": 20000,
                "origin": "user",
            },
        )["goal"]
        assert goal["status"] == "paused", goal
        goal = rpc.request(
            "thread/goal/set",
            {"threadId": thread, "status": "active", "origin": "user"},
        )["goal"]
        assert goal["status"] == "active", goal
        check("native goal create, pause and resume", goal)
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            value = rpc.receive(max(0.1, deadline - time.monotonic()))
            if value.get("method") == "turn/completed":
                params = value["params"]
                assert params["turn"]["status"] == "completed", params
                final = [
                    e
                    for e in rpc.events
                    if e.get("method") == "item/completed"
                    and e["params"].get("item", {}).get("text") == "GOAL_OK " + nonce
                ]
                if final:
                    break
        else:
            raise AssertionError("Goal did not complete before the timeout")
        turns = [
            e["params"]["turn"]["id"]
            for e in rpc.events
            if e.get("method") == "turn/completed"
        ]
        assert len(set(turns)) >= 2, turns
        goal = rpc.request("thread/goal/get", {"threadId": thread})["goal"]
        assert goal["status"] == "complete" and goal["tokensUsed"] > 0, goal
        check(
            "autonomous continuation executes MCP and completes goal",
            {"turns": turns, "goal": goal, "final": "GOAL_OK " + nonce},
        )
        rpc.request("thread/goal/clear", {"threadId": thread, "origin": "user"})
        assert rpc.request("thread/goal/get", {"threadId": thread}).get("goal") is None
        check("completed goal can be cleared", {"thread_id": thread})
        budget_thread = rpc.request(
            "thread/start",
            {
                "cwd": str(workspace),
                "model": "goal-test",
                "modelProvider": "estuary_goal_e2e",
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "ephemeral": False,
            },
        )["thread"]["id"]
        rpc.request(
            "thread/goal/set",
            {
                "threadId": budget_thread,
                "objective": "BUDGET_LIMIT_E2E: report BUDGET_STEP while leaving the goal active.",
                "tokenBudget": 1,
                "origin": "user",
            },
        )
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            event = rpc.receive(max(0.1, deadline - time.monotonic()))
            if (
                event.get("method") == "turn/completed"
                and event["params"]["threadId"] == budget_thread
            ):
                assert event["params"]["turn"]["status"] == "completed", event
                break
        else:
            raise AssertionError("Budget-limited goal did not stop")
        limited = rpc.request("thread/goal/get", {"threadId": budget_thread})["goal"]
        assert limited["status"] == "budgetLimited" and limited["tokensUsed"] == 136, (
            limited
        )
        check(
            "goal budget stops autonomous continuation and excludes cached tokens",
            limited,
        )
        rpc.request("thread/goal/clear", {"threadId": budget_thread, "origin": "user"})
        time.sleep(0.2)
        records = success(
            control
            + "/admin/api/logs/requests?since=0&session="
            + session
            + "&limit=100"
        )["requests"]
        details = [
            success(control + "/admin/api/logs/requests/" + record["id"])
            for record in records
        ]
        (output / "request-details.json").write_text(json.dumps(details, indent=2))
        assert len(records) == 6, len(records)
        early_closes = []
        for detail in details:
            record = detail["request"]
            assert record["http_status"] == 200, record
            assert record["protocol"] == "openai_responses" and record["streaming"], (
                record
            )
            assert (
                len(record["attempts"]) == 1
                and record["attempts"][0]["outcome"] == "success"
            ), record
            completed = next(
                p["content"]
                for p in detail["payloads"]
                if p["stage"] == "client_output"
            )
            assert completed["status"] == "completed" and completed["error"] is None, (
                completed
            )
            if record["outcome"] == "cancelled":
                # Codex may close HTTP immediately after response.completed.
                # Its actual tool execution/final message above and captured
                # terminal event establish completion independently of EOF.
                assert record["delivery"] == "body_dropped", record
                early_closes.append(record["id"])
            else:
                assert (
                    record["outcome"] == "success"
                    and record["delivery"] == "body_consumed"
                ), record
        with server.lock:
            requests = list(server.requests)
        assert not [r for r in requests if "error" in r], requests
        assert all(
            r["path"] == "/v1/chat/completions"
            and not r["client_credentials_forwarded"]
            for r in requests
        )
        actions = [r["action"] for r in requests]
        assert actions.count("first_turn") == actions.count("goal_complete") == 1, (
            actions
        )
        assert actions.count("budget_step") == 1, actions
        assert all(
            r["usage"]["input_tokens"] == 1000
            and r["usage"]["cache_read_tokens"] == 880
            and r["usage"]["output_tokens"] == 16
            for r in records
        )
        token_updates = [
            event["params"]["tokenUsage"]["total"]
            for event in rpc.events
            if event.get("method") == "thread/tokenUsage/updated"
            and event["params"]["threadId"] == thread
        ]
        assert (
            token_updates[-1]["inputTokens"] == 5000
            and token_updates[-1]["cachedInputTokens"] == 4400
            and token_updates[-1]["outputTokens"] == 80
        ), token_updates
        nodes = success(control + "/admin/api/nodes")["nodes"]
        assert all(
            n["runtime"]["active"] == 0
            and n["runtime"]["pending_prefill_tokens"] == 0
            and n["runtime"]["pending_decode_tokens"] == 0
            for n in nodes
        ), nodes
        check(
            "Chat routing, tool history, usage and released reservations",
            {
                "requests": len(records),
                "actions": actions,
                "tokens_used": goal["tokensUsed"],
                "client_closed_after_completed": early_closes,
            },
        )
        report["passed"] = True
    except Exception as error:  # noqa: BLE001 - preserve failure artifacts
        report["error"] = str(error) or type(error).__name__
        print(json.dumps({"passed": False, "error": report["error"]}), flush=True)
    finally:
        stop(codex)
        stop(gateway)
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
        if (output / "logs.db").exists():
            with sqlite3.connect(output / "logs.db") as db:
                report["storage"] = {
                    "integrity_check": db.execute("PRAGMA integrity_check").fetchone()[
                        0
                    ],
                    "foreign_key_violations": len(
                        db.execute("PRAGMA foreign_key_check").fetchall()
                    ),
                }
            report["passed"] &= report["storage"] == {
                "integrity_check": "ok",
                "foreign_key_violations": 0,
            }
        (output / "report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False)
        )
        print("Goal E2E report:", output / "report.json", flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
