<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Listing the NoKV image on Artifact Hub

Artifact Hub lists NoKV as a **container images** repository that points at
`oci://ghcr.io/nokv-lab/nokv`. It reads each tracked tag's metadata from the
image index, and renders [`docs/container-image.md`](../container-image.md)
as the package page.

## What CI guarantees

- `scripts/ci/image_metadata.py emit` is the one source of the image's OCI
  labels and annotations: title, description, creation time, version,
  revision, source, license, and Artifact Hub's readme URL (the raw
  `docs/container-image.md` at the built ref), logo, category, keywords and
  prerelease flag. Annotations go on the image index and on every platform
  manifest.
- The **Docker Image** workflow builds both platforms on every pull request
  and on `main`, exports them as an OCI layout, and fails unless
  `image_metadata.py check` finds every key Artifact Hub requires on the index
  (`io.artifacthub.package.readme-url`, `org.opencontainers.image.created`,
  `org.opencontainers.image.description`) with the values `emit` gives.
- The **Release Images** workflow publishes `ghcr.io/nokv-lab/nokv` (and the
  Docker Hub image when its secrets are set) for a release tag with the same
  metadata, under the tags `vX.Y.Z`, `X.Y.Z`, `X.Y`, `X` and `latest`.

## Launch checklist

These steps need an owner of the `NoKV-Lab` organization and an Artifact Hub
account.

1. Run **Release Images** from `main` for a release tag that contains
   `docs/container-image.md` (the first release after this change).
2. In the organization's packages, open `nokv` and make it public. The image
   carries `org.opencontainers.image.source`, so GitHub links the package to
   this repository.
3. Check the published index: `crane manifest ghcr.io/nokv-lab/nokv:latest |
   jq .annotations` (or `oras manifest fetch`) shows the keys above.
4. On artifacthub.io, open the control panel and add a repository of kind
   **Container images**, named `nokv`, with URL `oci://ghcr.io/nokv-lab/nokv`.
   Track `latest` as mutable and each release tag as immutable (at most ten
   tags).
5. Copy the repository ID Artifact Hub shows, store it as the repository
   variable `ARTIFACTHUB_REPOSITORY_ID`, and run **Release Images** again for
   the same tag: it pushes `artifacthub-repo.yml` as the `artifacthub.io` tag,
   which marks the listing as a verified publisher.
6. Once the package page renders, add its Artifact Hub badge to the README.

A release whose tag predates `docs/container-image.md` gets a readme URL that
does not resolve; list only releases made after this change.
