#!/usr/bin/env bash
# Builds what GitHub Pages serves, into target/site (or $1): the book, and
# under api/ the API reference of each side — a build of magic_sync is one
# side, and docs.rs builds only the server's. The book links into both.
set -euo pipefail
cd "$(dirname "$0")/.."
out=$(realpath -m "${1:-target/site}")

rm -rf "$out"
mdbook build book --dest-dir "$out"
for side in server:postgres replica:sqlite; do
  name=${side%%:*}
  feature=${side#*:}
  rm -rf target/doc
  RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps -p magic_sync --features "$feature"
  mkdir -p "$out/api/$name"
  cp -r target/doc/. "$out/api/$name/"
done
echo "site: $out"
