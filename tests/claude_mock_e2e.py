#!/usr/bin/env python3
"""Run real Claude Code against Estuary and a deterministic, Chat-only mock LLM.

Build with cargo build --locked, then run python3 tests/claude_mock_e2e.py.
Requires Claude Code on PATH. Uses no model server or cloud credentials.
Exercises text streaming plus real MCP and built-in Read tool execution.
The mock never reads the nonce file: it can repeat the nonce only after it
arrives in a replayed Chat tool result. Artifacts remain under ignored target/.
"""

import argparse
import json
import re
import subprocess
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


class MockLlm(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, output):
        super().__init__(("127.0.0.1", 0), MockHandler)
        self.output = output
        self.requests = []
        self.lock = threading.Lock()

    def record(self, record):
        with self.lock:
            self.requests.append(record)
            with (self.output / "mock-requests.jsonl").open("a") as log:
                log.write(json.dumps(record, ensure_ascii=False) + "\n")


class MockHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _format, *_args):
        pass

    def json_response(self, value, status=200):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 - HTTP handler API
        if urlsplit(self.path).path == "/v1/models":
            self.json_response(
                {"object": "list", "data": [{"id": "mock-claude", "object": "model"}]}
            )
        else:
            self.json_response({"error": {"message": "unexpected mock route"}}, 404)

    def do_POST(self):  # noqa: N802 - HTTP handler API
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        record = {
            "path": urlsplit(self.path).path,
            "query": urlsplit(self.path).query,
            "body": body,
            "client_credentials_forwarded": any(
                self.headers.get(name) for name in ["authorization", "x-api-key"]
            ),
        }
        try:
            assert record["path"] in {
                "/v1/chat/completions",
                "/v1/chat/completions/render",
            }, self.path
            assert not record["client_credentials_forwarded"]
            assert body["model"] == "mock-claude", body.get("model")
            assert "input" not in body and "system" not in body
            assert isinstance(body["messages"], list)
            if record["path"].endswith("/render"):
                record["action"] = "count_tokens"
                self.server.record(record)
                self.json_response({"token_ids": list(range(1000))})
                return
            message, finish, action = mock_reply(body)
            record["action"] = action
            self.server.record(record)
            usage = {
                "prompt_tokens": 1000,
                "prompt_tokens_details": {"cached_tokens": 880},
                "completion_tokens": 16,
            }
            if body.get("stream"):
                assert body["stream_options"]["include_usage"] is True
                self.stream_response(message, finish, usage)
            else:
                self.json_response(
                    {
                        "id": "chatcmpl_mock",
                        "object": "chat.completion",
                        "model": "mock-claude",
                        "choices": [
                            {"index": 0, "message": message, "finish_reason": finish}
                        ],
                        "usage": usage,
                    }
                )
        except (AssertionError, KeyError, ValueError) as error:
            record["error"] = str(error)
            self.server.record(record)
            self.json_response({"error": {"message": str(error)}}, 400)

    def stream_response(self, message, finish, usage):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        chunks = [{"role": "assistant"}]
        if "tool_calls" in message:
            call = message["tool_calls"][0]
            arguments = call["function"]["arguments"]
            chunks.append(
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": call["id"],
                            "type": "function",
                            "function": {
                                "name": call["function"]["name"],
                                "arguments": "",
                            },
                        }
                    ]
                }
            )
            for fragment in [arguments[:1], arguments[1:-1], arguments[-1:]]:
                if fragment:
                    chunks.append(
                        {
                            "tool_calls": [
                                {"index": 0, "function": {"arguments": fragment}}
                            ]
                        }
                    )
        else:
            text = message["content"]
            chunks.extend({"content": part} for part in [text[:3], text[3:]] if part)
        for delta in chunks:
            self.event(
                {"choices": [{"index": 0, "delta": delta, "finish_reason": None}]}
            )
        self.event({"choices": [{"index": 0, "delta": {}, "finish_reason": finish}]})
        # Usage follows finish_reason, as it does with vLLM include_usage streams.
        self.event({"choices": [], "usage": usage})
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def event(self, value):
        data = ("data: " + json.dumps(value, ensure_ascii=False) + "\n\n").encode()
        # Split in the middle of the SSE framing and JSON, exercising decoding.
        for fragment in [data[:2], data[2:17], data[17:]]:
            self.wfile.write(fragment)
            self.wfile.flush()
            time.sleep(0.001)


def mock_reply(body):
    tool_results = [m for m in body["messages"] if m["role"] == "tool"]
    if tool_results:
        match = re.search(r"LOCAL_NONCE_[0-9a-f]+", json.dumps(tool_results[-1]))
        assert match, "the real tool did not return the nonce"
        return {"role": "assistant", "content": match[0]}, "stop", "repeat_tool_nonce"
    user_text = json.dumps([m for m in body["messages"] if m["role"] == "user"])
    tools = {t["function"]["name"]: t["function"] for t in body.get("tools", [])}
    if "Call mcp__nonce__read exactly once" in user_text:
        name, arguments = "mcp__nonce__read", {}
    elif "Use Read to read " in user_text:
        match = re.search(r"Use Read to read (.+?\.json)", user_text)
        assert match, "Read prompt is missing its fixture path"
        name, arguments = "Read", {"file_path": match[1]}
    else:
        assert "Reply exactly CPU_OK." in user_text, "unexpected generation request"
        return {"role": "assistant", "content": "CPU_OK"}, "stop", "text"
    assert name in tools, f"CLI did not supply the requested tool: {name}"
    return (
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call_mock_nonce",
                    "type": "function",
                    "function": {"name": name, "arguments": json.dumps(arguments)},
                }
            ],
        },
        "tool_calls",
        "invoke_" + name,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/estuary"))
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument(
        "--output", type=Path, default=Path("target/claude-mock-e2e/runs")
    )
    args = parser.parse_args()
    output = args.output.resolve() / str(uuid.uuid4())
    output.mkdir(parents=True)
    server = MockLlm(output)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    report = {"mock_url": f"http://127.0.0.1:{server.server_port}/v1", "runs": []}
    try:
        for mode in ["mcp", "read"]:
            command = [
                sys.executable,
                str(Path(__file__).with_name("claude_vllm_cpu_e2e.py")),
                "--upstream",
                report["mock_url"],
                "--model",
                "mock-claude",
                "--provider",
                "openai",
                "--binary",
                str(args.binary.resolve()),
                "--tool-mode",
                mode,
                "--timeout",
                str(args.timeout),
                "--output",
                str(output / mode),
            ]
            completed = subprocess.run(
                command, capture_output=True, text=True, check=False
            )
            (output / f"{mode}.stdout").write_text(completed.stdout)
            (output / f"{mode}.stderr").write_text(completed.stderr)
            child_path = next((output / mode).glob("*/report.json"), None)
            child = json.loads(child_path.read_text()) if child_path else {}
            usages = [
                usage
                for check in child.get("checks", [])
                if check["passed"] and "usage" in check.get("details", {})
                for usage in check["details"]["usage"]
            ]
            client_usages = []
            for transcript in (output / mode).glob("*/cpu-chat-*.jsonl"):
                events = [
                    json.loads(line)
                    for line in transcript.read_text().splitlines()
                    if line.startswith("{")
                ]
                result = next(
                    (
                        event
                        for event in reversed(events)
                        if event.get("type") == "result"
                    ),
                    None,
                )
                if result and not result.get("is_error"):
                    client_usages.append(result["usage"])
            client_usage_verified = len(client_usages) == 2 and all(
                usage.get("cache_read_input_tokens", 0) * 100
                == 88
                * (
                    usage.get("input_tokens", 0)
                    + usage.get("cache_read_input_tokens", 0)
                    + usage.get("cache_creation_input_tokens", 0)
                )
                and usage.get("output_tokens", 0) > 0
                for usage in client_usages
            )
            run = {
                "tool_mode": mode,
                "exit_code": completed.returncode,
                "claude_usage_verified": client_usage_verified,
                "claude_usage": client_usages,
                "report": str(child_path) if child_path else None,
                "checks": [
                    {"name": check["name"], "passed": check["passed"]}
                    for check in child.get("checks", [])
                ],
                "results": [
                    check["details"]["result"]
                    for check in child.get("checks", [])
                    if check["passed"] and "result" in check.get("details", {})
                ],
                "usage_verified": bool(usages)
                and all(
                    usage.get("input_tokens") == 1000
                    and usage.get("cache_read_tokens") == 880
                    and usage.get("output_tokens") == 16
                    for usage in usages
                ),
            }
            if completed.returncode and completed.stderr:
                run["error"] = completed.stderr[-4000:]
                print(
                    json.dumps(
                        {"tool_mode": mode, "error": run["error"]}, ensure_ascii=False
                    ),
                    flush=True,
                )
            report["runs"].append(run)
            report["claude_version"] = child.get("claude_version")
            for line in completed.stdout.splitlines():
                if line.startswith("{"):
                    check = json.loads(line)
                    print(
                        json.dumps(
                            {
                                "tool_mode": mode,
                                "name": check["name"],
                                "passed": check["passed"],
                                "error": check.get("error", "")[:1200],
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )
                elif line.startswith("Report:"):
                    print(line, flush=True)
        with server.lock:
            records = list(server.requests)
        actions = [record.get("action") for record in records]
        report["mock"] = {
            "requests": len(records),
            "actions": actions,
            "errors": [r["error"] for r in records if "error" in r],
            "only_chat_endpoints": all(
                r["path"].startswith("/v1/chat/completions") for r in records
            ),
            "credentials_stripped": all(
                not r["client_credentials_forwarded"] for r in records
            ),
        }
        passed = (
            all(
                run["exit_code"] == 0
                and run["usage_verified"]
                and run["claude_usage_verified"]
                for run in report["runs"]
            )
            and not report["mock"]["errors"]
            and report["mock"]["only_chat_endpoints"]
            and report["mock"]["credentials_stripped"]
            and all(
                action in actions
                for action in ["text", "invoke_mcp__nonce__read", "invoke_Read"]
            )
            and actions.count("repeat_tool_nonce") == 2
        )
        report["passed"] = passed
        return 0 if passed else 1
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
        (output / "report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False)
        )
        print("Mock E2E report:", output / "report.json", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
