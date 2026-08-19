<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Change Governance

NoKV treats merge authority as a closed contract. A passing test suite is not
permission to merge an unreviewed metadata, recovery, object-lifetime, wire,
SDK, or Workbench rewrite.

## Enforced Policy

The `main` branch must enforce all of the following through GitHub branch
protection:

- an ordinary pull request with 5,000 or fewer changed lines does not require
  review;
- a pull request with more than 5,000 changed lines, or one that changes a
  protected CI trust root, requires one core maintainer approval on its exact
  current head;
- the current-head pusher cannot supply that approval;
- unresolved review conversations block merge;
- administrators are subject to the same restrictions;
- the branch must be current with `main` and every required status must pass.

GitHub-reported additions plus deletions are the review-size measure. The
author, current-head pusher, bots, non-core reviewers, duplicate reviews,
dismissed reviews, requested changes, and approvals against an older head do
not count. Generated files, fixtures, documentation, and deletions are not
exempt.

Protected CI trust roots are the paths owned by both core maintainers in
[`.github/CODEOWNERS`](../../.github/CODEOWNERS): workflow and reusable-action
definitions, governance and qualification scripts, CODEOWNERS itself, and the
protected release test. Their review requirement is independent of change
size because even a one-line edit can weaken the checks that authorize a
merge or release.

The `change-governance/large-change-review` status enforces the conditional
review rule for both large changes and protected CI trust-root changes. Its
legacy name remains stable because branch protection requires that exact
context. It identifies the actor who introduced the current head from the
earliest GitHub Actions `pull_request` run for that head. Missing, malformed,
paginated-beyond-bound, or unavailable PR, review, workflow-run, or pusher data
fails closed for every governed change.

## Trust Boundary

[`change-governance.yml`](../../.github/workflows/change-governance.yml) uses
`pull_request_target` and `pull_request_review` so GitHub loads the workflow
from protected `main`. It never checks out or executes pull-request code. The
runner fetches the policy and tests from the pull request's exact base SHA and
publishes a dedicated commit status on the untrusted head SHA.

This distinction is required: a normal `pull_request` workflow is part of the
proposed diff and can otherwise weaken the check that evaluates itself.

The required status set is:

- `nokv-workspace`;
- `object-namespace-recovery`;
- `workbench-contract`;
- `signoff`;
- `change-governance/large-change-review`.

Every validation job that runs for each pull-request head is required. The
Docker `image` check is the only CI exception: it continues in the background
and reports failures, but its runtime does not delay a merge. `sync-project` is
project-board automation, does not run on `synchronize`, and is not a merge
validation or required check.

During initial rollout, the custom governance context is added to branch
protection before the universal review rule is removed. Verified open PRs at
or below the threshold receive a one-time status for their exact head. Unknown
or later heads remain blocked until the protected workflow on `main` evaluates
them. This ordering avoids a fail-open migration window.

## Review Expectations

For a governed change, one non-pusher core maintainer approval is a minimum
gate, not evidence that a broad rewrite is reviewable. Split a change when it
crosses logical package or lifecycle boundaries, hides behavior changes among
mechanical churn, or cannot be reproduced and reviewed within one focused diff.
For storage changes, reviewers must apply the
[PR Review Checklist](./pr_review_checklist.md) and retain exact recovery,
failure, retry, retention, and downstream Workbench evidence.

## Administrative Boundary

Repository rules can constrain administrators while the rules exist. An
organization owner who can edit repository governance can still delete or
replace those rules. Preventing that action requires an organization-level
ruleset with no bypass actors, independent ownership of ruleset administration,
and organization audit-log monitoring. Repository CI alone cannot provide that
stronger guarantee.
