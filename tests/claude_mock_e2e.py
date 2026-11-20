#!/usr/bin/env python3
"""Run real Claude Code against Estuary and a deterministic, Chat-only mock LLM.

Build with cargo build --locked, then run python3 tests/claude_mock_e2e.py.
Requires Claude Code on PATH. Uses no model server or cloud credentials.
Exercises text/image streaming plus real MCP and built-in Read tool execution.
The mock never reads the nonce file: it can repeat the nonce only after it
arrives in a replayed Chat tool result. Artifacts remain under ignored target/.
"""

import argparse
import hashlib
import json
import re
import subprocess
import sys
import uuid
from pathlib import Path

from e2e_helpers import MockLlm, image_summary, tool_reply
from image_fixtures import IMAGES


def mock_reply(body):
    user_blocks = [
        block
        for message in body["messages"]
        if message["role"] == "user"
        for block in (
            message["content"]
            if isinstance(message["content"], list)
            else [{"type": "text", "text": message["content"]}]
        )
    ]
    text = "\n".join(block["text"] for block in user_blocks if block["type"] == "text")
    if "IMAGE_E2E_BEGIN" in text or "IMAGE_READ_E2E:" in text:
        count = int(re.search(r"IMAGE_OK_([12])", text)[1])
        images = image_summary(body)
        expected = [hashlib.sha256(data).hexdigest() for data in IMAGES[:count]]
        assert [image["sha256"] for image in images] == expected[: len(images)], (
            "image bytes/order changed"
        )
        read = "IMAGE_READ_E2E:" in text
        assert all(image["role"] == ("tool" if read else "user") for image in images), (
            "image role changed"
        )
        if read:
            paths = json.loads(
                re.search(r"Read these PNG files in order: (\[.*?\])", text)[1]
            )
            assert len(paths) == count
            if len(images) < count:
                return tool_reply(
                    body,
                    "Read",
                    {"file_path": paths[len(images)]},
                    f"call_image_{len(images)}",
                )
        else:
            # Check text/image interleaving, not just the image array's ordering.
            sequence = []
            for block in user_blocks:
                if block["type"] == "image_url":
                    sequence.append("image")
                elif block["type"] == "text":
                    sequence.extend(
                        re.findall(r"IMAGE_E2E_(?:BEGIN|BETWEEN|END)", block["text"])
                    )
            assert sequence == (
                ["IMAGE_E2E_BEGIN", "image", "IMAGE_E2E_END"]
                if count == 1
                else [
                    "IMAGE_E2E_BEGIN",
                    "image",
                    "IMAGE_E2E_BETWEEN",
                    "image",
                    "IMAGE_E2E_END",
                ]
            ), sequence
        assert len(images) == count, "image missing or duplicated"
        return (
            {"role": "assistant", "content": f"IMAGE_OK_{count}"},
            "stop",
            f"images_{'read' if read else 'input'}_{count}",
        )
    tool_results = [m for m in body["messages"] if m["role"] == "tool"]
    if tool_results:
        match = re.search(r"LOCAL_NONCE_[0-9a-f]+", json.dumps(tool_results[-1]))
        assert match, "the real tool did not return the nonce"
        return {"role": "assistant", "content": match[0]}, "stop", "repeat_tool_nonce"
    user_text = json.dumps([m for m in body["messages"] if m["role"] == "user"])
    if "Call mcp__nonce__read exactly once" in user_text:
        name, arguments = "mcp__nonce__read", {}
    elif "Use Read to read " in user_text:
        match = re.search(r"Use Read to read (.+?\.json)", user_text)
        assert match, "Read prompt is missing its fixture path"
        name, arguments = "Read", {"file_path": match[1]}
    else:
        assert "Reply exactly CPU_OK." in user_text, "unexpected generation request"
        return {"role": "assistant", "content": "CPU_OK"}, "stop", "text"
    return tool_reply(body, name, arguments)


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
    report = {"runs": []}
    try:
        with MockLlm(output, model="mock-claude", reply=mock_reply) as server:
            report["mock_url"] = server.url
            for mode in ["mcp", "read"]:
                command = [
                    sys.executable,
                    str(Path(__file__).with_name("claude_vllm_cpu_e2e.py")),
                    "--upstream",
                    server.url,
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
                if mode == "read":
                    command.append("--images")
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
                client_usage_verified = len(client_usages) == (
                    6 if mode == "read" else 2
                ) and all(
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
                            {"tool_mode": mode, "error": run["error"]},
                            ensure_ascii=False,
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
            _, actions, report["mock"] = server.summary()
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
            and all(
                actions.count(f"images_{kind}_{count}") == 1
                for kind in ["input", "read"]
                for count in [1, 2]
            )
        )
        report["passed"] = passed
        return 0 if passed else 1
    finally:
        (output / "report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False)
        )
        print("Mock E2E report:", output / "report.json", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
