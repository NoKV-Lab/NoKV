<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Append identity and response-loss recovery

The supported append contract is specified in
[Durable append product contract](append-product-spec.md). A caller persists one
logical operation ID and its complete intent. NoKV can use successive immutable
publication attempts only after durably proving the previous attempt is cleaned;
parent advancement and child admission are atomic. Successful publication and
the logical receipt are also atomic.

A completed operation returns its original receipt after process restart,
response loss, or later changes to the live file. An unfinished operation exposes
`pending`, `ready_to_retry`, or `quarantined`, with a prescribed recovery action.
Status queries need neither the original payload nor an available object store.
The native CLI is the primary interface, followed by the Python and Rust SDKs.

Start with the [durable append guide](../append.md) for current submission and
operator commands. Public inspection reads the current child's retained ledger;
public recovery asks the owner to retry quarantined cleanup using a saved
logical state token. It neither supplies the missing delta nor creates the
next publication attempt. Failed child keys are permanently sealed, not
deleted, before the same logical action can admit a successor.

The earlier single-attempt implementation prevented duplicate appends after a
lost success response but could not finish an action after an abandoned upload
was cleaned. Its executable
[identity recovery gate](../../scripts/workbench/append_identity_recovery_gate.py)
is retained for reproducible historical evidence and shared harness fixtures.
Its safety-only pending cases
do not establish completion qualification for the current product contract.
Use the [product acceptance gate](../../scripts/workbench/append_product_acceptance_gate.py),
the [public operations gate](../../scripts/workbench/append_operations_acceptance_gate.py),
and the normative contract's acceptance matrix for the current implementation.
The [qualification record](append-qualification.md) states which versions and
fault boundaries were executed; the presence of a harness is not a passing run.
