#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Generate deterministic NoKV identity metadata for Brew/Release artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from nokv_runtime import source_identity, write_build_info


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate nokv.build_info.v1 from a locked NoKV checkout."
    )
    parser.add_argument("--source-root", default=".")
    parser.add_argument(
        "--revision",
        required=True,
        help="Full git commit represented by the artifact.",
    )
    parser.add_argument(
        "--nokv-bin",
        required=True,
        help="Exact binary packaged with this build-info file.",
    )
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Generate an explicitly dirty identity for local testing only.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        identity = source_identity(Path(args.source_root), args.revision)
        if identity.source_dirty and not args.allow_dirty:
            raise ValueError(
                "source checkout is dirty; release build-info must describe a clean commit"
            )
        changed = write_build_info(Path(args.output), identity, Path(args.nokv_bin))
    except Exception as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    print(f"output: {Path(args.output).expanduser().resolve()}")
    print(f"changed: {str(changed).lower()}")
    print(f"nokv_revision: {identity.nokv_git_commit}")
    print(f"holt_revision: {identity.holt_git_commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
