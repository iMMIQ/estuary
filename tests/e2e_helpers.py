"""Shared helpers for opt-in coding-agent end-to-end tests."""

import base64
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

LOOPBACK_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
MCP_NONCE_SCRIPT = Path(__file__).resolve().parent / "fixtures" / "mcp_nonce.py"


def http(url, body=None, headers=None):
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        response = LOOPBACK_HTTP.open(request, timeout=120)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, dict(response.headers), response.read().decode()


def http_json(url, body=None, headers=None):
    status, _, text = http(url, body, headers=headers)
    assert 200 <= status < 300, (status, text[:2000])
    return json.loads(text)


def free_port():
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def free_port_pair():
    first, second = free_port(), free_port()
    while second == first:
        second = free_port()
    return first, second


def wait_for(predicate, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.1)
    raise AssertionError("condition did not become true")


def validate_loopback_cli(parser, upstream, cli_name, label):
    if urllib.parse.urlsplit(upstream).hostname not in LOOPBACK_HOSTS:
        parser.error("This test only accepts a loopback inference server")
    if not shutil.which(cli_name):
        parser.error(f"{label} must be installed")


def stop_process_group(process, timeout=15):
    if process is None or process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def run_process_group(
    command,
    *,
    cwd,
    env,
    stdout_path,
    stderr_path,
    timeout,
    label,
    stdin_bytes=None,
):
    with stdout_path.open("w") as out, stderr_path.open("w") as err:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=out,
            stderr=err,
            stdin=subprocess.PIPE if stdin_bytes is not None else subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            if stdin_bytes is not None:
                process.communicate(input=stdin_bytes, timeout=timeout)
            else:
                process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise AssertionError(
                f"{label} timed out after {timeout}s; see {stdout_path.name}"
            ) from None
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    return process.returncode


def toml_value(value):
    if isinstance(value, dict):
        return (
            "{"
            + ", ".join(json.dumps(k) + " = " + toml_value(v) for k, v in value.items())
            + "}"
        )
    return json.dumps(value)


def sqlite_integrity(path):
    with sqlite3.connect(path) as db:
        return {
            "integrity_check": db.execute("PRAGMA integrity_check").fetchone()[0],
            "foreign_key_violations": len(
                db.execute("PRAGMA foreign_key_check").fetchall()
            ),
        }


class E2ERunner:
    """Shared workspace, nonce fixture, and JSON check report helper."""

    def __init__(self, output_root, report):
        self.output = output_root.resolve() / str(uuid.uuid4())
        self.output.mkdir(parents=True, mode=0o700)
        self.workspace = self.output / "workspace"
        self.workspace.mkdir()
        self.nonce = "LOCAL_NONCE_" + uuid.uuid4().hex[:12]
        self.fixture = self.workspace / "nonce.json"
        self.fixture.write_text(json.dumps({"nonce": self.nonce}) + "\n")
        self.report = {**report, "checks": []}

    def write_images(self, images):
        paths = [self.workspace / f"image-{index}.png" for index in range(len(images))]
        for path, data in zip(paths, images, strict=True):
            path.write_bytes(data)
        return paths

    def save(self):
        (self.output / "report.json").write_text(
            json.dumps(self.report, indent=2, ensure_ascii=False)
        )

    def check(self, name, fn):
        started = time.monotonic()
        try:
            item = {"name": name, "passed": True, "details": fn()}
        except Exception as error:  # noqa: BLE001 - preserve independent failures
            item = {"name": name, "passed": False, "error": str(error)}
        item["seconds"] = round(time.monotonic() - started, 3)
        self.report["checks"].append(item)
        self.save()
        print(json.dumps(item, ensure_ascii=False), flush=True)
        return item["passed"]

    def exit_code(self):
        self.save()
        print("Report:", self.output / "report.json", flush=True)
        return 0 if all(c["passed"] for c in self.report["checks"]) else 1


class GatewayContext:
    """Managed local Estuary process with control-plane query helpers."""

    def __init__(self, binary, output):
        self.binary = binary.resolve()
        self.output = output
        self.public, self.admin = free_port_pair()
        self.base_url = f"http://127.0.0.1:{self.public}"
        self.v1_url = f"{self.base_url}/v1"
        self.control_url = f"http://127.0.0.1:{self.admin}"
        self._log = None
        self._process = None

    def __enter__(self):
        self._log = (self.output / "gateway.log").open("w")
        self._process = subprocess.Popen(
            [
                str(self.binary),
                "--database",
                str(self.output / "control.db"),
                "--listen",
                f"127.0.0.1:{self.public}",
                "--admin-listen",
                f"127.0.0.1:{self.admin}",
                "--session-log-database",
                str(self.output / "logs.db"),
            ],
            stdout=self._log,
            stderr=self._log,
            start_new_session=True,
            env={k: v for k, v in os.environ.items() if not k.startswith("ESTUARY_")},
        )

        def ready():
            try:
                return http_json(self.control_url + "/health/live")
            except OSError:
                return None

        wait_for(ready)
        return self

    def __exit__(self, *_exc):
        stop_process_group(self._process)
        if self._log is not None:
            self._log.close()

    def register_node(
        self, node_id, upstream, models, *, max_concurrency=1, provider="openai"
    ):
        return http_json(
            self.control_url + "/admin/api/nodes",
            {
                "id": node_id,
                "base_url": upstream,
                "models": models,
                "max_concurrency": max_concurrency,
                "provider": {"type": provider},
            },
        )

    def session_details(self, session, minimum=1, sort_by_start=False):
        def ended():
            query = urllib.parse.urlencode(
                {"since": 0, "session": session, "limit": 100}
            )
            rows = http_json(self.control_url + "/admin/api/logs/requests?" + query)[
                "requests"
            ]
            return (
                rows
                if len(rows) >= minimum and all(r["ended_at_ms"] for r in rows)
                else None
            )

        rows = wait_for(ended)
        if sort_by_start:
            rows = sorted(rows, key=lambda r: (r["started_at_ms"], r["id"]))
        return [
            http_json(self.control_url + "/admin/api/logs/requests/" + row["id"])
            for row in rows
        ]

    def check_logger_health(
        self, expected_sessions, *, include_sessions=False, count_only=False
    ):
        records = [
            detail["request"]
            for session in expected_sessions
            for detail in self.session_details(session)
        ]
        status = http_json(self.control_url + "/admin/api/logs/status")
        assert (
            status["available"]
            and status["dropped"] == 0
            and status["write_errors"] == 0
        ), status
        result = {
            "status": status,
            "requests": len(records) if count_only else records,
        }
        if include_sessions:
            sessions = http_json(self.control_url + "/admin/api/logs/sessions?since=0")
            assert set(expected_sessions) <= {
                session["id"] for session in sessions["sessions"]
            }, sessions
            result["sessions"] = sessions
        return result

    def check_scheduler_idle(self):
        nodes = http_json(self.control_url + "/admin/api/nodes")["nodes"]
        assert all(
            n["runtime"]["active"] == 0
            and n["runtime"]["pending_prefill_tokens"] == 0
            and n["runtime"]["pending_decode_tokens"] == 0
            for n in nodes
        ), nodes
        return {"nodes": len(nodes)}


def image_summary(body):
    images = []
    for message in body["messages"]:
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if block.get("type") == "image_url":
                url = block["image_url"]["url"]
                assert url.startswith("data:image/png;base64,"), (
                    "image media type changed"
                )
                data = base64.b64decode(url.split(",", 1)[1], validate=True)
                images.append(
                    {
                        "role": message["role"],
                        "sha256": hashlib.sha256(data).hexdigest(),
                    }
                )
    return images


def tool_reply(body, name, arguments, call_id="call_mock_nonce"):
    tools = {t["function"]["name"] for t in body.get("tools", [])}
    assert name in tools, f"CLI did not supply the requested tool: {name}"
    return (
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": json.dumps(arguments)},
                }
            ],
        },
        "tool_calls",
        "invoke_" + name,
    )


class MockLlm(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, output, *, model, reply):
        super().__init__(("127.0.0.1", 0), MockHandler)
        self.output = output
        self.requests = []
        self.lock = threading.Lock()
        self.model = model
        self.reply = reply
        self._worker = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server_port}/v1"

    def __enter__(self):
        self._worker = threading.Thread(target=self.serve_forever, daemon=True)
        self._worker.start()
        return self

    def __exit__(self, *_exc):
        self.shutdown()
        self.server_close()
        if self._worker is not None:
            self._worker.join(timeout=5)

    def record(self, record):
        with self.lock:
            self.requests.append(record)
            with (self.output / "mock-requests.jsonl").open("a") as log:
                log.write(json.dumps(record, ensure_ascii=False) + "\n")

    def summary(self):
        with self.lock:
            records = list(self.requests)
        actions = [record.get("action") for record in records]
        return (
            records,
            actions,
            {
                "requests": len(records),
                "actions": actions,
                "errors": [r["error"] for r in records if "error" in r],
                "only_chat_endpoints": bool(records)
                and all(r["path"].startswith("/v1/chat/completions") for r in records),
                "credentials_stripped": all(
                    not r["client_credentials_forwarded"] for r in records
                ),
                "image_checks": [
                    {"action": r["action"], "images": r["images"]}
                    for r in records
                    if r.get("action", "").startswith("images_")
                ],
            },
        )


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
                {
                    "object": "list",
                    "data": [{"id": self.server.model, "object": "model"}],
                }
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
            assert body["model"] == self.server.model, body.get("model")
            assert "input" not in body and "system" not in body
            assert isinstance(body["messages"], list)
            if record["path"].endswith("/render"):
                record["action"] = "count_tokens"
                self.server.record(record)
                self.json_response({"token_ids": list(range(1000))})
                return
            message, finish, action = self.server.reply(body)
            record["action"] = action
            record["images"] = image_summary(body)
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
                        "model": self.server.model,
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
