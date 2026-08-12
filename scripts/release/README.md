# NoKV source-only Homebrew release

NoKV is distributed through the public `NoKV-Lab/homebrew-tap` as a Homebrew
Formula. The Formula downloads one release-owned source archive and compiles the
`nokv` CLI locally with Homebrew's locked standard Cargo install arguments. It
is deliberately not a Cask, does not install a precompiled executable, and does
not require Apple signing or notarization.

## Consumer flow

Anyone can install the fully qualified Formula without GitHub repository
credentials. Homebrew adds the public tap and trusts only that Formula:

```shell
brew install NoKV-Lab/tap/nokv
nokv version --json
nokv schema
```

The current release gate covers Apple Silicon and Intel macOS. Linuxbrew is not
yet a qualified release target.

The equivalent explicit tap flow is:

```shell
brew tap NoKV-Lab/tap
brew trust --formula NoKV-Lab/tap/nokv
brew install nokv
```

After a tap update is merged, the same installation is updated with:

```shell
brew update
brew upgrade nokv
```

The Formula declares Homebrew `rust` and `protobuf` as build-only dependencies.
The Rust dependency graph is resolved exclusively from the release
`Cargo.lock`; the installed binary reports its NoKV version, exact source
commit, `Cargo.lock` SHA-256, and exact Holt version/source/checksum.

The Formula version follows the stable NoKV tag and the `crates/nokv` package
version. It does not follow Holt or either Homebrew build dependency. A Holt
change is published only inside a new NoKV release after its exact pin and lock
entry pass the release gates and the generated Formula is merged into the tap.

Installing the executable does not create a NoKV deployment. A LingTai MCP
configuration must still supply the selected NoKV control-plane endpoint,
object-store configuration, root identity, and stable Agent presentation root.
Its command is the Homebrew-installed `nokv` executable and its final argument
is `mcp`.

## Release invariants

`.github/workflows/release-homebrew.yml` accepts only a canonical stable tag of
the form `vMAJOR.MINOR.PATCH`. Before any user-selected ref is checked out, the
workflow validates that syntax and resolves the tag to one commit. A release is
rejected unless all of these identities agree:

- the tag and `crates/nokv` package version;
- the workspace `nokv` dependency and `Cargo.lock` package version;
- the tag commit and the commit embedded in the installed CLI;
- the archive checksum and Formula checksum;
- the workspace Holt exact pin and the unique checksummed Holt lock entry;
- the frozen 18-tool Workbench schema and the installed CLI schema.

The generated archive comes from `git archive` at the validated commit and is
gzip-compressed with a fixed timestamp. GitHub-generated tag archives and
`latest` URLs are rejected.

## Operator flow

1. Merge the release PR, including the `crates/nokv` version update.
2. Create the matching stable tag on that exact commit.
3. Dispatch `Release Homebrew Source Formula` with that tag.
4. The workflow builds one deterministic source release candidate.
5. Apple Silicon and Intel macOS runners each install, test, and inspect the
   same candidate through Homebrew.
6. Only after both gates pass does the workflow publish the source archive,
   manifest, and checksums to the GitHub release.
7. A repository-scoped GitHub App opens a PR against the public tap. A human
   merges that PR to make the version available to consumers.

The tap repository must have public visibility and contain `.nokv-tap.json`
equal to `scripts/release/public_tap_marker.json`. Configure
the GitHub App client ID as `HOMEBREW_TAP_APP_CLIENT_ID` and its private key as
`HOMEBREW_TAP_APP_PRIVATE_KEY`. The App must be installed only where needed and
grant `contents: write` plus `pull requests: write` on `homebrew-tap`.

The workflow fails closed if the repository API does not report the tap as
public. It never accepts a broad personal access token and never pushes
directly to the tap's default branch.

## Local release checks

```shell
python3 scripts/release/test_homebrew_source_release.py
python3 scripts/workbench/workbench_contract_test.py
actionlint .github/workflows/release-homebrew.yml
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

For a local Homebrew installation test, commit the candidate in an isolated
worktree, create the matching local tag in a disposable clone, run `prepare`,
render a Formula with the archive's absolute `file://` URL, and tap that local
Git repository. Do not create or move a release tag in the primary checkout.
