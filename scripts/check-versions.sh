#!/usr/bin/env bash
# The crate, the derive and the dashboard are released together, at one
# version: the root Cargo.toml's. Fails when another place says otherwise —
# the engine's `=` pin of the derive, the dashboard's package.json, the
# examples' requirement — or, given a version (a release's tag, without its
# `v`), when that is not the version, or the changelog has no entry for it.
set -euo pipefail
cd "$(dirname "$0")/.."

version=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"$/\1/p' Cargo.toml)
[ -n "$version" ] || { echo "no version in the root Cargo.toml's [workspace.package]" >&2; exit 1; }

wrong=0
expect() { # what, found
  if [ "$2" != "$version" ]; then
    echo "$1 says ${2:-nothing}, and the root Cargo.toml $version" >&2
    wrong=1
  fi
}

expect "magic_sync's pin of magic_sync_derive" \
  "$(sed -n 's/^magic_sync_derive = { version = "=\([^"]*\)".*/\1/p' magic_sync/Cargo.toml)"
expect "dashboard/package.json" \
  "$(node -p 'require("./dashboard/package.json").version')"
for manifest in examples/shop/*/Cargo.toml; do
  expect "$manifest" "$(sed -n 's/^magic_sync = { version = "\([^"]*\)".*/\1/p' "$manifest")"
done

if [ $# -gt 0 ]; then
  [ "$1" = "$version" ] || { echo "the release is $1, and the root Cargo.toml says $version" >&2; wrong=1; }
  grep -q "^## $version — [0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}$" CHANGELOG.md \
    || { echo "CHANGELOG.md has no entry \"## $version — <date>\"" >&2; wrong=1; }
fi

[ "$wrong" = 0 ] && echo "every version is $version"
exit "$wrong"
