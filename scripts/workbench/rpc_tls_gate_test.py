#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the workspace RPC TLS gate's certificates and client options."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from rpc_tls_gate import SERVER_NAME, Pki, TlsStack


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
