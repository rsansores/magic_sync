#!/usr/bin/env bash
# Checks every link the repository's documents make into the repository
# itself — the README, the book, the protocol, the rustdoc comments — against
# the files: the site scripts/site.sh built (target/site, or $1) for the
# published pages, the tree for GitHub's. A link to a page, a file or an
# anchor that is not there fails. Links elsewhere are not followed, so the
# check does not depend on the network. (The 404 page's links are relative to
# where Pages serves the site, which no file says.)
set -euo pipefail
cd "$(dirname "$0")/.."
root=$PWD
site=$(realpath -m "${1:-target/site}")
[ -f "$site/index.html" ] || { echo "no site at $site: run scripts/site.sh first" >&2; exit 1; }

repo=https://github.com/rsansores/magic_sync
lychee --offline --include-fragments --no-progress \
  --remap "^https://rsansores\.github\.io/magic_sync/(.*)$ file://$site/\$1" \
  --remap "^$repo/(blob|tree|edit)/main/(.*)$ file://$root/\$2" \
  --remap "^https://raw\.githubusercontent\.com/rsansores/magic_sync/main/(.*)$ file://$root/\$1" \
  --remap "^$repo/?$ file://$root/" \
  --exclude '^https?://' \
  --exclude-path "$site/404.html" \
  README.md CHANGELOG.md CONTRIBUTING.md RELEASING.md \
  magic_sync/PROTOCOL.md dashboard/README.md examples/shop/README.md \
  'magic_sync/src/**/*.rs' 'magic_sync_derive/src/**/*.rs' \
  "$site/*.html" "$site/guide/*.html"
