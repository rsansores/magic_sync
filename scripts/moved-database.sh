#!/usr/bin/env bash
# The server's database moved between clusters mid-walk — pg_dump into
# pg_restore — keeps sending what changes: the test
# `a_database_moved_to_another_cluster_keeps_sending_what_changes`, from a
# cluster whose transaction ids are far ahead of the one it moves to, as a
# long-lived server's are of a new one's. Needs PostgreSQL's server programs:
# on PATH, or in PG_BIN.
set -euo pipefail
if [ -n "${PG_BIN:-}" ]; then
  PATH="$PG_BIN:$PATH"
fi
work=$(mktemp -d)
stop() {
  for cluster in from to; do
    pg_ctl -D "$work/$cluster" -m immediate stop >/dev/null 2>&1 || true
  done
  rm -rf "$work"
}
trap stop EXIT
mkdir "$work/sock"
for cluster in from to; do
  initdb -D "$work/$cluster" -U postgres --auth=trust >/dev/null
done
# The next transaction id far ahead, with the commit log segment it lands in.
next=$((0x40000000))
dd if=/dev/zero of="$work/from/pg_xact/$(printf '%04X' $((next / 1048576)))" bs=8192 count=32 status=none
pg_resetwal -x "$next" -D "$work/from" >/dev/null
port=5601
for cluster in from to; do
  pg_ctl -D "$work/$cluster" -l "$work/$cluster.log" -w \
    -o "-p $port -k $work/sock -c listen_addresses= -c autovacuum=off -c fsync=off" start >/dev/null
  port=$((port + 1))
done
socket=$(printf '%s' "$work/sock" | sed 's|/|%2F|g')
DATABASE_URL="postgres://postgres@$socket:5601/postgres" \
MAGIC_SYNC_MOVED_URL="postgres://postgres@$socket:5602/postgres" \
  cargo test --locked -p magic_sync --features postgres \
    a_database_moved_to_another_cluster_keeps_sending_what_changes -- --ignored
