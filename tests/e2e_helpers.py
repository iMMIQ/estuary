"""Shared loopback HTTP helpers for opt-in coding-agent end-to-end tests."""

import json
import socket
import urllib.error
import urllib.request


def http(url, body=None, headers=None):
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
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
