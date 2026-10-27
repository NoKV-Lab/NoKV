#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the workspace RPC TLS gate's certificates, client options, and
the read-only object endpoint its Agent processes use."""

from __future__ import annotations

import http.client
import http.server
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path

from cli_contract_gate import free_port
from rpc_tls_gate import SERVER_NAME, Pki, ReadOnlyObjectProxy, TlsStack, with_object_endpoint


def openssl(*arguments: str) -> str:
    return subprocess.run(["openssl", *arguments], capture_output=True, text=True, check=True).stdout


class PkiTest(unittest.TestCase):
    def test_signs_the_owner_for_its_name_and_self_signs_clients(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pki = Pki(Path(directory) / "pki")
            owner = str(pki.directory / "owner.pem")
            self.assertIn("OK", openssl("verify", "-CAfile", str(pki.directory / "ca.pem"), owner))
            self.assertIn(f"DNS:{SERVER_NAME}", openssl("x509", "-in", owner, "-noout", "-ext", "subjectAltName"))

            certificate, key = pki.client("agent")
            self.assertTrue(key.exists())
            names = openssl("x509", "-in", str(certificate), "-noout", "-subject", "-issuer").splitlines()
            self.assertEqual(names[0].split("=", 1)[1], names[1].split("=", 1)[1])


class ReadOnlyObjectProxyTest(unittest.TestCase):
    def test_forwards_reads_and_refuses_writes_before_the_provider(self) -> None:
        seen: list[str] = []

        class Provider(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_arguments: object) -> None:
                pass

            def answer(self, body: bytes) -> None:
                seen.append(self.command)
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("ETag", '"abc"')
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

            def do_GET(self) -> None:
                self.answer(b"payload")

            def do_HEAD(self) -> None:
                self.answer(b"payload")

            def do_PUT(self) -> None:
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self.answer(b"")

        provider = http.server.ThreadingHTTPServer(("127.0.0.1", free_port()), Provider)
        threading.Thread(target=provider.serve_forever, daemon=True).start()
        proxy = ReadOnlyObjectProxy(free_port(), f"http://127.0.0.1:{provider.server_port}", 5)
        try:
            def call(method: str, body: bytes | None = None) -> http.client.HTTPResponse:
                connection = http.client.HTTPConnection("127.0.0.1", proxy.port, timeout=5)
                connection.request(method, "/bucket/nokv/artifacts/k", body=body)
                response = connection.getresponse()
                response.read()
                return response

            read = call("GET")
            self.assertEqual((read.status, read.getheader("ETag")), (200, '"abc"'))
            head = call("HEAD")
            self.assertEqual((head.status, head.getheader("Content-Length")), (200, "7"))
            self.assertEqual(call("PUT", b"x").status, 403)
            self.assertEqual(call("DELETE").status, 403)
            self.assertEqual(seen, ["GET", "HEAD"])
            self.assertEqual(
                [(r["method"], r["forwarded"], r["status"]) for r in proxy.snapshot()],
                [("GET", True, 200), ("HEAD", True, 200), ("PUT", False, 403), ("DELETE", False, 403)],
            )
        finally:
            proxy.close()
            provider.shutdown()
            provider.server_close()


class ObjectOptionsTest(unittest.TestCase):
    def test_agents_read_through_the_proxy_and_provision_keeps_the_provider(self) -> None:
        options = ["--object-bucket", "b", "--object-endpoint", "http://127.0.0.1:9000", "--object-root", "r"]
        self.assertEqual(
            with_object_endpoint(options, "http://127.0.0.1:1"),
            ["--object-bucket", "b", "--object-endpoint", "http://127.0.0.1:1", "--object-root", "r"],
        )
        stack = TlsStack.__new__(TlsStack)
        stack.owner_objects = options
        stack.agent_objects = with_object_endpoint(options, "http://127.0.0.1:1")
        self.assertEqual(stack.object_options("provision"), options)
        self.assertEqual(stack.object_options("workspace-path"), stack.agent_objects)


class TlsOptionsTest(unittest.TestCase):
    def test_carry_the_client_identity_and_the_owner_name(self) -> None:
        stack = TlsStack.__new__(TlsStack)
        stack.pki = Pki.__new__(Pki)
        stack.pki.directory = Path("/pki")
        stack.clients = {"agent": (Path("/pki/agent.pem"), Path("/pki/agent.key"))}
        self.assertEqual(
            stack.tls_options("agent", "other.test"),
            [
                "--rpc-tls-ca", "/pki/ca.pem",
                "--rpc-tls-cert", "/pki/agent.pem",
                "--rpc-tls-key", "/pki/agent.key",
                "--rpc-tls-server-name", "other.test",
            ],
        )
        self.assertEqual(stack.tls_options(None, SERVER_NAME), [])


if __name__ == "__main__":
    unittest.main()
