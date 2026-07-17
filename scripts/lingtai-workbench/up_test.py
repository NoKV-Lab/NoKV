#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path


UP_SCRIPT = Path(__file__).with_name("up.sh").resolve()


class UpScriptTest(unittest.TestCase):
    def test_term_exits_and_releases_update_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            command = r"""
source "$1"
STATE_DIR="$2"
UP_LOCK_DIR="${STATE_DIR}/up.lock"
acquire_up_lock
kill -TERM $$
printf 'continued-after-term\n'
"""
            completed = subprocess.run(
                ["bash", "-c", command, "up-test", str(UP_SCRIPT), str(state_dir)],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )

            self.assertEqual(completed.returncode, 143, completed.stderr)
            self.assertNotIn("continued-after-term", completed.stdout)
            self.assertFalse((state_dir / "up.lock").exists())

    def test_custom_credentials_fail_closed(self) -> None:
        command = r"""
source "$1"
LINGTAI_WORKBENCH_S3_ACCESS_KEY_ID=custom
LINGTAI_WORKBENCH_S3_SECRET_ACCESS_KEY=custom
validate_guarded_credentials
"""
        completed = subprocess.run(
            ["bash", "-c", command, "up-test", str(UP_SCRIPT)],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

        self.assertEqual(completed.returncode, 1)
        self.assertIn("dedicated local RustFS credentials only", completed.stderr)


if __name__ == "__main__":
    unittest.main()
