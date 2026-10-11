#!/usr/bin/env bash
# Publishes one crate of the workspace at the version of the release, unless
# crates.io already has that version — so a release that failed half-way is
# finished by running its jobs again. The tarball was built and verified by
# CI's `package` job on this very commit, so nothing is compiled here: the
# token never reaches a build script or a proc macro.
#
#   scripts/publish-crate.sh <crate> <version>
#
# CARGO_REGISTRY_TOKEN: a token for crates.io.
set -euo pipefail
cd "$(dirname "$0")/.."

crate=${1:?the crate to publish}
version=${2:?the version to publish}

status=$(curl --silent --output /dev/null --write-out '%{http_code}' \
  --user-agent "magic_sync release (https://github.com/rsansores/magic_sync)" \
  "https://crates.io/api/v1/crates/$crate/$version")
case "$status" in
  200) echo "$crate $version is on crates.io already"; exit 0 ;;
  404) ;;
  *) echo "crates.io answered $status for $crate $version" >&2; exit 1 ;;
esac

cargo publish --locked --no-verify -p "$crate"
