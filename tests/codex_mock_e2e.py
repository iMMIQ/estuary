#!/usr/bin/env python3
"""Run real Codex Responses -> Estuary -> Chat-only mock, including PNGs.

Build with cargo build --locked, then run python3 tests/codex_mock_e2e.py.
No model downloads or cloud credentials. Tests text, a real MCP nonce read,
single/multiple image attachments, and MCP single/multiple image output replay.
Artifacts remain under ignored target/codex-mock-e2e/runs/.
"""

import argparse
import hashlib
import json
import re
import subprocess
import sys
import threading
import uuid
from pathlib import Path

from claude_mock_e2e import MockLlm, image_summary, tool_reply
from image_fixtures import IMAGES


def mock_reply(body):
    text = json.dumps([m for m in body["messages"] if m["role"] == "user"])
    tool_results = [m for m in body["messages"] if m["role"] == "tool"]
    image_input = "CODEX_IMAGE_INPUT_E2E" in text
    image_tool = "CODEX_IMAGE_TOOL_E2E" in text
    if image_input or image_tool:
        count = int(re.search(r"count=([12])", text)[1])
        images = image_summary(body)
        if image_input or tool_results:
            assert [i["sha256"] for i in images] == [
                hashlib.sha256(data).hexdigest() for data in IMAGES[:count]
            ], "image count/bytes/order changed"
            assert all(
                i["role"] == ("tool" if image_tool else "user") for i in images
            ), "image role changed"
            if image_input:
                return (
                    {"role": "assistant", "content": f"IMAGE_OK_{count}"},
                    "stop",
                    f"images_input_{count}",
                )
        else:
            assert not images, "MCP images appeared before the real tool ran"
            return call_mcp(body, "Read local PNG fixtures", {"count": count})
    if tool_results:
        match = re.search(r"LOCAL_NONCE_[0-9a-f]+", json.dumps(tool_results))
        assert match, "real MCP execution did not supply the nonce"
        return (
            {"role": "assistant", "content": match[0]},
            "stop",
            f"images_tool_{count}" if image_tool else "repeat_tool_nonce",
        )
    if "Call mcp__nonce__read exactly once" in text:
        return call_mcp(body, "Read and return the nonce", {})
    assert "Reply exactly CPU_OK." in text, "unexpected generation request"
    return {"role": "assistant", "content": "CPU_OK"}, "stop", "text"


def call_mcp(body, description, arguments):
    tools = [tool["function"] for tool in body.get("tools", [])]
    matches = [
        tool["name"] for tool in tools if description in tool.get("description", "")
    ]
    assert len(matches) == 1, (description, tools)
    return tool_reply(body, matches[0], arguments)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/estuary"))
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument(
        "--output", type=Path, default=Path("target/codex-mock-e2e/runs")
    )
    args = parser.parse_args()
    output = args.output.resolve() / str(uuid.uuid4())
    output.mkdir(parents=True)
    server = MockLlm(output, model="mock-codex", reply=mock_reply)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    report = {"mock_url": f"http://127.0.0.1:{server.server_port}/v1"}
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("codex_cpu_e2e.py")),
                "--upstream",
                report["mock_url"],
                "--model",
                "mock-codex",
                "--binary",
                str(args.binary.resolve()),
                "--timeout",
                str(args.timeout),
                "--images",
                "--output",
                str(output / "codex"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        (output / "codex.stdout").write_text(completed.stdout)
        (output / "codex.stderr").write_text(completed.stderr)
        child_path = next((output / "codex").glob("*/report.json"), None)
        child = json.loads(child_path.read_text()) if child_path else {}
        report.update(
            exit_code=completed.returncode,
            report=str(child_path),
            codex_version=child.get("codex_version"),
        )
        report["checks"] = [
            {k: check[k] for k in ["name", "passed"]}
            | ({"error": check["error"]} if "error" in check else {})
            for check in child.get("checks", [])
        ]
        details = [
            check["details"]
            for check in child.get("checks", [])
            if check["passed"] and "usage" in check.get("details", {})
        ]
        report["usage_verified"] = len(details) == 6 and all(
            usage.get("input_tokens") == 1000
            and usage.get("cache_read_tokens") == 880
            and usage.get("output_tokens") == 16
            for detail in details
            for usage in detail["usage"]
        )
        report["client_usage"] = [detail["client_usage"] for detail in details]
        report["client_usage_verified"] = len(details) == 6 and all(
            detail["client_usage"]["input_tokens"] == 1000 * detail["requests"]
            and detail["client_usage"]["cached_input_tokens"]
            == 880 * detail["requests"]
            and detail["client_usage"]["output_tokens"] == 16 * detail["requests"]
            for detail in details
        )
        with server.lock:
            records = list(server.requests)
        actions = [record.get("action") for record in records]
        report["mock"] = {
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
        }
        report["passed"] = (
            completed.returncode == 0
            and len(report["checks"]) == 8
            and all(check["passed"] for check in report["checks"])
            and child.get("storage")
            == {"integrity_check": "ok", "foreign_key_violations": 0}
            and report["usage_verified"]
            and report["client_usage_verified"]
            and not report["mock"]["errors"]
            and report["mock"]["only_chat_endpoints"]
            and report["mock"]["credentials_stripped"]
            and actions.count("text") == 1
            and actions.count("repeat_tool_nonce") == 1
            and all(
                actions.count(f"images_{kind}_{count}") == 1
                for kind in ["input", "tool"]
                for count in [1, 2]
            )
        )
        for check in report["checks"]:
            print(json.dumps(check, ensure_ascii=False), flush=True)
        if completed.returncode and completed.stderr:
            print(completed.stderr[-4000:], file=sys.stderr)
        print(
            json.dumps(
                {
                    "passed": report["passed"],
                    "usage_verified": report["usage_verified"],
                    "client_usage_verified": report["client_usage_verified"],
                    "mock": report["mock"],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 0 if report["passed"] else 1
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
