#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the control-plane gate's certificates, its scenario inventory, and
the agreement between its role grants and docs/control-plane-credentials.md."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from cli_contract_gate import REPOSITORY
from control_plane_gate import (
    AGENT_ROOT_KEYS,
    AGENT_SHARD_KEYS,
    IDENTITIES,
    OWNER_READ_PREFIXES,
    OWNER_SHARD_KEYS,
    SCENARIOS,
    ControlPki,
)


def openssl(*arguments: str) -> str:
    return subprocess.run(["openssl", *arguments], capture_output=True, text=True, check=True).stdout


class ControlPkiTest(unittest.TestCase):
    def test_one_ca_signs_etcd_the_owner_and_every_named_client(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pki = ControlPki(Path(directory) / "pki")
            ca = str(pki.directory / "ca.pem")
            etcd = str(pki.directory / "etcd.pem")
            self.assertIn("OK", openssl("verify", "-CAfile", ca, etcd))
            self.assertIn("IP Address:127.0.0.1", openssl("x509", "-in", etcd, "-noout", "-ext", "subjectAltName"))
            for name in IDENTITIES:
                certificate, key = pki.pair(name)
                self.assertTrue(key.exists())
                self.assertIn("OK", openssl("verify", "-CAfile", ca, str(certificate)))
                # RFC 2253 spells the subject the same on every OpenSSL build.
                subject = openssl("x509", "-in", str(certificate), "-noout", "-subject", "-nameopt", "RFC2253")
                self.assertEqual(subject.strip().removeprefix("subject="), f"CN={name}")
            outsider, _ = pki.pair("outsider")
            verify = subprocess.run(["openssl", "verify", "-CAfile", ca, str(outsider)], capture_output=True, text=True)
            self.assertNotEqual(verify.returncode, 0)


class InventoryTest(unittest.TestCase):
    def test_scenarios_cover_the_control_plane_matrix(self) -> None:
        self.assertEqual(
            sorted(SCENARIOS),
            [
                "c0_partial_tls_options_are_invalid_input",
                "c1_agent_round_trip",
                "c2_agent_identity_cannot_administer",
                "c3_another_tenants_root_is_denied",
                "c4_role_matrix_holds",
                "c5_unaccepted_identities_are_refused",
                "c6_deleted_user_is_refused_at_once",
            ],
        )

    def test_role_grants_match_the_credentials_document(self) -> None:
        document = (REPOSITORY / "docs" / "control-plane-credentials.md").read_text()
        for family in (*AGENT_ROOT_KEYS, *AGENT_SHARD_KEYS, *OWNER_SHARD_KEYS, *OWNER_READ_PREFIXES, "tenants"):
            self.assertIn(f"`{family}/", document, family)
        # The Agent never reads the owner's recovery frontier or session key.
        self.assertFalse(set(AGENT_SHARD_KEYS) & {"logical-shard-recovery", "sessions"})

    def test_gate_sends_no_private_key_through_argv_of_etcdctl_probes(self) -> None:
        source = (REPOSITORY / "scripts" / "workbench" / "control_plane_gate.py").read_text()
        self.assertIn('for key in self.pki.directory.glob("*.key"):', source)


if __name__ == "__main__":
    unittest.main()
