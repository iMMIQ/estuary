#!/usr/bin/env python3
"""Opt-in live DeepSeek and Codex test; credentials stay out of arguments/files.

Build with cargo build --locked, then run:
python3 tests/deepseek_e2e.py --api-key-file .cache/deepseek-e2e/api-key
Uses the real, billable API. Reports and traces go under ignored target/.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request


def http(url, body=None, headers=None):
    request = urllib.request.Request(
        url, data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", **(headers or {})})
    try:
        response = urllib.request.urlopen(request, timeout=120)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, dict(response.headers), response.read().decode()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def events(text):
    result = []
    for block in text.replace("\r\n", "\n").split("\n\n"):
        name, data = None, []
        for line in block.splitlines():
            if line.startswith("event:"):
                name = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].strip())
        if data and "\n".join(data) != "[DONE]":
            result.append((name, json.loads("\n".join(data))))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-key-file", type=Path, required=True)
    parser.add_argument("--model", help="Defaults to the smallest available chat model")
    parser.add_argument("--output", type=Path, default=Path("target/deepseek-e2e"))
    parser.add_argument("--binary", type=Path, default=Path("target/debug/estuary"))
    parser.add_argument("--skip-codex", action="store_true")
    args = parser.parse_args()
    key = args.api_key_file.read_text().strip()
    if not key or any(char.isspace() for char in key):
        parser.error("Key file must contain only the API key")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    os.chmod(output, 0o700)
    report = {"checks": [], "upstream": "https://api.deepseek.com/v1"}
    alias = "deepseek-e2e"
    public, admin = free_port(), free_port()
    while public == admin:
        admin = free_port()
    base = f"http://127.0.0.1:{public}/v1"
    control = f"http://127.0.0.1:{admin}"
    gateway = None
    log = None

    def safe(text):
        return str(text).replace(key, "[REDACTED]")

    def save():
        (output / "report.json").write_text(safe(json.dumps(report, indent=2)))

    def check(name, fn):
        started = time.monotonic()
        try:
            detail = fn()
            item = {"name": name, "passed": True, "details": detail}
        except Exception as error:
            item = {"name": name, "passed": False, "error": safe(error)}
        item["seconds"] = round(time.monotonic() - started, 3)
        report["checks"].append(item)
        save()
        print(safe(json.dumps(item)), flush=True)
        return item["passed"]

    def success(url, body=None, headers=None):
        status, response_headers, text = http(url, body, headers)
        assert 200 <= status < 300, (status, text[:2000])
        return text, response_headers

    def discover():
        text, _ = success(report["upstream"] + "/models", headers={"Authorization": f"Bearer {key}"})
        models = [model["id"] for model in json.loads(text)["data"]]
        preferred = ["deepseek-v4-flash", "deepseek-flash", "deepseek-chat"]
        report["model"] = args.model or next((m for m in preferred if m in models), models[0])
        assert report["model"] in models, models
        return {"available_models": models, "selected": report["model"]}

    def direct():
        text, _ = success(report["upstream"] + "/chat/completions", {
            "model": report["model"], "messages": [{"role": "user", "content": "Reply exactly E2E_OK."}],
            "max_tokens": 32, "thinking": {"type": "disabled"}},
            {"Authorization": f"Bearer {key}"})
        result = json.loads(text)
        assert "E2E_OK" in result["choices"][0]["message"]["content"], result
        return {"usage": result.get("usage")}

    def probe(endpoint, streaming, suffix="", extra=None):
        if endpoint == "responses":
            body = {"model": alias, "input": "Reply exactly E2E_OK.", "max_output_tokens": 64}
            # Deliberately contradictory headers: the URL owns the protocol.
            headers = {"User-Agent": "claude-code/1.0", "anthropic-version": "2023-06-01"}
        else:
            body = {"model": alias, "messages": [{"role": "user", "content": "Reply exactly E2E_OK."}],
                    "max_tokens": 64}
            headers = {"User-Agent": "codex_cli_rs/1.0"}
        body.update(stream=streaming)
        body.update(extra or {})
        text, response_headers = success(base + "/" + endpoint, body, headers)
        (output / f"{endpoint}-{'stream' if streaming else 'json'}{suffix}.txt").write_text(text)
        if streaming:
            parsed = events(text)
            assert parsed, text
            if endpoint == "responses":
                assert all(name and name.startswith("response.") and event["type"] == name
                           for name, event in parsed), parsed
                if extra and extra.get("reasoning", {}).get("effort") == "low":
                    assert any(name == "response.reasoning_text.delta" and event.get("delta")
                               for name, event in parsed), parsed
                terminal = [event["response"] for name, event in parsed if name == "response.completed"]
                assert len(terminal) == 1, parsed
                result = terminal[0]
            else:
                assert all(name and not name.startswith("response.") and event["type"] == name
                           for name, event in parsed), parsed
                assert any(name == "message_start" for name, _ in parsed), parsed
                assert any(name == "message_stop" for name, _ in parsed), parsed
                generated = "".join(event.get("delta", {}).get("text", "") for _, event in parsed)
                assert "E2E_OK" in generated, text
                return {"event_types": sorted({name for name, _ in parsed}), "node": response_headers.get("x-gateway-node")}
        else:
            result = json.loads(text)
        if endpoint == "responses":
            assert result["object"] == "response" and "content" not in result and result.get("type") != "message", result
            assert result["status"] == "completed", result
            assert "E2E_OK" in json.dumps(result["output"]), result
        else:
            assert result["type"] == "message" and "output" not in result, result
            assert "E2E_OK" in json.dumps(result["content"]), result
        return {"usage": result.get("usage"), "node": response_headers.get("x-gateway-node")}

    def tool_roundtrip():
        prompt = "Call files.echo once with value 7."
        body = {"model": alias, "input": prompt, "max_output_tokens": 128,
                "tools": [{"type": "namespace", "name": "files", "tools": [{"type": "function",
                    "name": "echo", "description": "Echo an integer", "parameters": {"type": "object",
                    "properties": {"value": {"type": "integer"}}, "required": ["value"]}}]}], "tool_choice": "required"}
        text, _ = success(base + "/responses", body, {"User-Agent": "codex_cli_rs/1.0"})
        result = json.loads(text)
        (output / "tool-call.json").write_text(text)
        calls = [item for item in result["output"] if item["type"] == "function_call"]
        assert len(calls) == 1, result
        call = calls[0]
        assert call["name"] == "echo" and call["namespace"] == "files", call
        assert json.loads(call["arguments"]) == {"value": 7}, call
        body["input"] = [{"role": "user", "content": prompt}, *result["output"],
                         {"type": "function_call_output", "call_id": call["call_id"], "output": "7"},
                         {"role": "user", "content": "Now reply exactly E2E_OK. Do not call any tools."}]
        body["tool_choice"] = "none"
        text, _ = success(base + "/responses", body)
        followup = json.loads(text)
        assert "E2E_OK" in json.dumps(followup["output"]), followup
        (output / "tool-followup.json").write_text(text)
        return {"namespace": call["namespace"], "name": call["name"], "call_id_preserved": True}

    def custom_patch():
        text, _ = success(base + "/responses", {"model": alias, "input":
            "Use apply_patch to add hello.txt containing hello. Produce a valid patch starting *** Begin Patch.",
            "max_output_tokens": 256, "tools": [{"type": "custom", "name": "apply_patch",
                "description": "Apply a patch. Input is the patch text."}], "tool_choice": "required"})
        result = json.loads(text)
        (output / "custom-tool.json").write_text(text)
        calls = [item for item in result["output"] if item["type"] == "custom_tool_call"]
        assert len(calls) == 1 and calls[0]["name"] == "apply_patch", result
        assert "*** Begin Patch" in calls[0]["input"], calls
        return {"type": calls[0]["type"], "name": calls[0]["name"]}

    def mixed_protocols():
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            jobs = [pool.submit(probe, endpoint, stream, "-concurrent")
                    for endpoint in ["responses", "messages"] for stream in [False, True]]
            return [job.result() for job in jobs]

    def cli(use_tool=False):
        codex = shutil.which("codex")
        assert codex, "codex executable not found; use --skip-codex only for HTTP-only runs"
        scratch = output / "codex-scratch"
        scratch.mkdir(exist_ok=True)
        settings = {"model_provider": '"estuary_deepseek_e2e"',
            "model_providers.estuary_deepseek_e2e.name": '"Estuary DeepSeek E2E"',
            "model_providers.estuary_deepseek_e2e.base_url": json.dumps(base),
            "model_providers.estuary_deepseek_e2e.wire_api": '"responses"',
            "model_providers.estuary_deepseek_e2e.requires_openai_auth": "false",
            "model_providers.estuary_deepseek_e2e.supports_websockets": "false",
            "web_search": '"disabled"', "model_reasoning_effort": '"none"'}
        label = "codex-tool" if use_tool else "codex"
        expected = "E2E_TOOL_OK" if use_tool else "E2E_OK"
        final_file = output / f"{label}-final.txt"
        final_file.unlink(missing_ok=True)
        command = [codex, "exec", "--ignore-user-config", "--ignore-rules", "--ephemeral",
                   "--skip-git-repo-check", "-C", str(scratch), "-s", "read-only", "--json",
                   "-m", alias, "-o", str(final_file)]
        for name, value in settings.items():
            command += ["-c", f"{name}={value}"]
        command += [("Use exec_command exactly once to run printf E2E_TOOL_OK. "
                     "After receiving the command output, reply exactly E2E_TOOL_OK. "
                     "Do not inspect files or run any other commands.") if use_tool else
                    "Reply exactly E2E_OK. Do not use any tools or inspect any files."]
        # No upstream credential is inherited by Codex; only the gateway gets it.
        env = dict(os.environ)
        env.pop("ESTUARY_DEEPSEEK_E2E_KEY", None)
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=150)
        (output / f"{label}-events.jsonl").write_text(safe(result.stdout))
        (output / f"{label}-stderr.txt").write_text(safe(result.stderr))
        assert result.returncode == 0, safe(result.stderr[-3000:] + result.stdout[-3000:])
        assert final_file.read_text().strip() == expected
        emitted = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
        assert any(item.get("type") == "turn.completed" for item in emitted), emitted
        if use_tool:
            executions = [item["item"] for item in emitted if item.get("type") == "item.completed"
                          and item.get("item", {}).get("type") == "command_execution"]
            assert len(executions) == 1 and executions[0]["exit_code"] == 0, emitted
            assert "E2E_TOOL_OK" in executions[0]["aggregated_output"], executions
        return {"version": subprocess.check_output([codex, "--version"], text=True).strip(), "final": expected}

    try:
        if not check("authenticated_model_discovery", discover) or not check("direct_chat_control", direct):
            return 1
        database = output / f"gateway-{time.time_ns()}.db"
        env = {**os.environ, "ESTUARY_DEEPSEEK_E2E_KEY": key, "RUST_LOG": "info,estuary::proxy=debug"}
        log = (output / "gateway.log").open("w")
        gateway = subprocess.Popen([str(args.binary.resolve()), "--database", str(database),
            "--listen", f"127.0.0.1:{public}", "--admin-listen", f"127.0.0.1:{admin}",
            "--upstream-header-timeout-ms", "120000", "--stream-idle-timeout-ms", "120000",
            "--upstream-body-timeout-ms", "120000"], env=env, stdout=log, stderr=log)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            assert gateway.poll() is None, "Gateway exited; inspect gateway.log"
            try:
                http(control + "/admin/api/nodes")
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise RuntimeError("Gateway did not start")
        success(control + "/admin/api/nodes", {"id": "deepseek-live", "base_url": report["upstream"],
            "api_key_env": "ESTUARY_DEEPSEEK_E2E_KEY", "models": {alias: report["model"]},
            "max_concurrency": 8, "model_capabilities": {alias: {"family": "deepseek"}},
            "provider": {"type": "openai", "anthropic_protocol": "native"}})
        for endpoint in ["responses", "messages"]:
            for stream in [False, True]:
                check(f"{endpoint}_{'stream' if stream else 'buffered'}", lambda e=endpoint, s=stream: probe(e, s))
        check("concurrent_mixed_protocols", mixed_protocols)
        check("namespaced_tool_roundtrip", tool_roundtrip)
        check("codex_custom_apply_patch", custom_patch)
        check("responses_reasoning_stream", lambda: probe("responses", True, "-reasoning",
            {"reasoning": {"effort": "low"}, "max_output_tokens": 1024}))
        if not args.skip_codex:
            check("actual_codex_cli", cli)
            check("actual_codex_tool_roundtrip", lambda: cli(use_tool=True))
        report["passed"] = all(item["passed"] for item in report["checks"])
        return 0 if report["passed"] else 1
    finally:
        if gateway:
            gateway.send_signal(signal.SIGTERM)
            try:
                gateway.wait(timeout=15)
            except subprocess.TimeoutExpired:
                gateway.kill()
                gateway.wait()
        if log:
            log.close()
        if gateway:
            diagnostic = (output / "gateway.log").read_text()
            report["adapter_log"] = {name: diagnostic.count(f"adapter=\"{name}\"")
                                     for name in ["deepseek_responses", "deepseek_messages"]}
            report["credential_absent_from_database_and_log"] = (
                key.encode() not in database.read_bytes() and key not in diagnostic)
            assert report["credential_absent_from_database_and_log"]
        report["gateway_stopped"] = gateway is None or gateway.poll() is not None
        save()


if __name__ == "__main__":
    raise SystemExit(main())
