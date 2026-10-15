"""Local stdio MCP fixture for nonce reads and optional PNG image results."""

import base64
import json
import sys
from pathlib import Path


def main():
    fixture = Path(sys.argv[1])
    images = [Path(path) for path in sys.argv[2:]]
    for line in sys.stdin:
        request = json.loads(line)
        if "id" not in request:
            continue
        method = request.get("method")
        if method == "initialize":
            result = {
                "protocolVersion": request["params"]["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "local-nonce", "version": "1.0"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "read",
                        "description": "Read and return the nonce string from a local file. No arguments.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": False,
                        },
                    }
                ]
            }
            if images:
                result["tools"].append(
                    {
                        "name": "images",
                        "description": "Read local PNG fixtures in order and return their image contents.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "count": {"type": "integer", "enum": [1, 2]}
                            },
                            "required": ["count"],
                            "additionalProperties": False,
                        },
                    }
                )
        elif method == "tools/call" and request.get("params", {}).get("name") == "read":
            result = {
                "content": [
                    {"type": "text", "text": json.loads(fixture.read_text())["nonce"]}
                ]
            }
        elif (
            method == "tools/call"
            and request.get("params", {}).get("name") == "images"
            and images
        ):
            count = request["params"]["arguments"]["count"]
            assert type(count) is int and 1 <= count <= len(images), count
            result = {
                "content": [
                    {"type": "text", "text": json.loads(fixture.read_text())["nonce"]},
                    *[
                        {
                            "type": "image",
                            "mimeType": "image/png",
                            "data": base64.b64encode(path.read_bytes()).decode(),
                        }
                        for path in images[:count]
                    ],
                ]
            }
        elif method == "ping":
            result = {}
        else:
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": {"code": -32601, "message": "Method not found"},
                    }
                ),
                flush=True,
            )
            continue
        print(
            json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}),
            flush=True,
        )


if __name__ == "__main__":
    main()
