# Releasing

`magic_sync_derive`, `magic_sync` and `magic_sync_dashboard` are released
together, at one version. A tag publishes them: `.github/workflows/release.yml`
runs CI on the tag, and only once it is green, and the tag is the version
everywhere, publishes the derive, then the engine, then the dashboard, then
the book with each side's API on GitHub Pages. Nothing is published from a
workstation.

Nothing that compiles holds a credential. CI's `package` job builds and
verifies the tarball of each crate on the tagged commit; the release packs the
same commit again and publishes it without building it (`cargo publish
--no-verify`, in `scripts/publish-crate.sh`), so no build script or proc macro
runs next to a token. The dashboard is packed by a job with no credentials, and the tarball is
what a second job publishes. Every action is pinned to a commit
(`.github/dependabot.yml` proposes the new ones, and those of the lockfiles,
cargo's and npm's), CI's `workflows` job runs
[zizmor](https://docs.zizmor.sh) over the workflows, and a scheduled workflow
(`advisories.yml`) looks every day for an advisory published against what a
lockfile holds.

Once, in the repository's settings:

- an environment `release`, with the maintainers who approve a release as its
  reviewers; Pages served from GitHub Actions;
- on crates.io, a Trusted Publisher for each of `magic_sync_derive` and
  `magic_sync` (this repository, workflow `release.yml`, environment
  `release`): the release asks crates.io for a token that lives for the job.
  A trusted publisher is set up on a crate that exists, so the first
  release of each crate has no one yet to trust it: it publishes with the
  environment's secret `CARGO_REGISTRY_TOKEN` (a token scoped to publishing new
  crates), which is deleted once the publishers are set;
- in the environment, the secret `NPM_TOKEN` (npm, allowed to publish
  `magic_sync_dashboard`).

## 1. The version

On a branch, set the version in:

- the root `Cargo.toml` (`[workspace.package]`);
- the `=` pin of `magic_sync_derive` in `magic_sync/Cargo.toml`;
- `dashboard/package.json`;
- the `magic_sync` requirement of the example's manifests
  (`examples/shop/*/Cargo.toml`).

Then bring the lockfiles along — the workspace's, the example programs' and
the book's snippets' — with `cargo update --workspace` in each
(`--manifest-path …/Cargo.toml` for the last ones). `scripts/check-versions.sh`
says whether every place agrees; CI runs it.

## 2. The changelog

Move the *unreleased* entry of `CHANGELOG.md` to `## <version> — <date>`. It
says, for a host upgrading:

- whether the stored schema changed (`SERVER_SCHEMA_VERSION`, `REPLICA_SCHEMA_VERSION`: each side's own): the step from the
  version before, for a host with migrations of its own, and that the server's
  processes of the earlier build are stopped before the upgrade (the book's
  *Upgrading*);
- whether the protocol changed (`PROTOCOL`), and which side to upgrade first;
- whether the dashboard's contract changed (`status::CONTRACT`);
- what in the API changed: the difference in `magic_sync/public-api/`.

## 3. The tag

Merge the branch. On main, tag the merge and push the tag:

```sh
git tag -a v<version> -m v<version>
git push origin v<version>
```

The release workflow refuses a tag that is not on main, or that is not the
version of every place in step 1, or whose changelog entry is missing. A
release that fails half-way — the derive published, the engine refused —
is finished by fixing the cause and running the failed jobs again (or all of
them: a job whose version the registry already has says so and succeeds
without publishing it again).

## What CI checks before a release can happen

Every pull request packs both crates and verifies each tarball builds on each
side (`cargo package --workspace --features magic_sync/postgres`, then
`magic_sync/sqlite`), packs the dashboard and lints the tarball, checks the
versions, builds the book with both sides' API, checks its links, and audits
the workflows. The
crates carry their licenses (`LICENSE-MIT`, `LICENSE-APACHE`, linked from the
root) and the README; the dashboard's `prepack` copies the licenses in.

docs.rs builds the server's side (`[package.metadata.docs.rs]`); the book
publishes both sides' API with each release.
