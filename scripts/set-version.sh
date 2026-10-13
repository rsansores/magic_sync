#!/usr/bin/env bash
# Sets the version of a release everywhere it is written — the root
# Cargo.toml, the engine's `=` pin of the derive, the dashboard's
# package.json, the example's requirements — brings every lockfile along,
# and dates the changelog's unreleased entry. In the working tree only: the
# release commits it once every registry took the release.
#
#   scripts/set-version.sh <version> <date>
set -euo pipefail
cd "$(dirname "$0")/.."

version=${1:?the version to release}
date=${2:?the date of the release, YYYY-MM-DD}

sed -i "/^\[workspace.package\]/,/^\[/s/^version = \".*\"$/version = \"$version\"/" Cargo.toml
sed -i "s/^magic_sync_derive = { version = \"=[^\"]*\"/magic_sync_derive = { version = \"=$version\"/" \
  magic_sync/Cargo.toml
for manifest in examples/shop/*/Cargo.toml; do
  sed -i "s/^magic_sync = { version = \"[^\"]*\"/magic_sync = { version = \"$version\"/" "$manifest"
done
node -e '
  const fs = require("fs");
  const file = "dashboard/package.json";
  const json = JSON.parse(fs.readFileSync(file, "utf8"));
  json.version = process.argv[1];
  fs.writeFileSync(file, JSON.stringify(json, null, 2) + "\n");
' "$version"

for manifest in Cargo.toml examples/shop/server/Cargo.toml examples/shop/register/Cargo.toml \
  book/snippets/Cargo.toml; do
  cargo update --workspace --manifest-path "$manifest"
done

# The entry being released: one already dated for this version, or the
# unreleased one, which this dates.
if ! grep -q "^## $version — [0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}$" CHANGELOG.md; then
  grep -q -i "^## .*unreleased$" CHANGELOG.md \
    || { echo "CHANGELOG.md has no unreleased entry to release" >&2; exit 1; }
  sed -i "0,/^## .*[Uu]nreleased$/s//## $version — $date/" CHANGELOG.md
fi

scripts/check-versions.sh "$version"
