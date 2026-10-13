# Settings and wishes

A **setting** is a value a replica holds and acts on: a module's
configuration, a screen's brightness, the footer a register prints. Two
parties want to write it. The replica's user, or its hardware, says what the
setting *is*; the head office says what it *wants* it to be. Neither is a
merge of the other, and the engine never merges: the replica owns the row,
and the server **wishes** its desired columns other values, which the replica
carries out — or refuses, or finds it changed first.

Once the system settles — every message applied, every write filed — the
server and the replica show the same value of every setting, and the same
answer to every wish, whichever side's write ruled.

## A digital twin

So the server holds two halves of every setting: what the replica **reports**
it is — the row, as the replica last wrote it — and what the server
**desires** it to be — the wish, with where it stands. That pair is what IoT
platforms call a *device twin* or *device shadow*: desired state on one side,
reported state on the other, and the device reconciling the two. Here the
twin is rows in your own PostgreSQL, travelling the way every other row
does: offline-first, in order, confirmed, and set straight after a restore of
either side. There is no other service to run.

What it unlocks:

- **Remote configuration.** A screen's brightness, a printer's footer, a
  scale's tare, a module's parameters — set at the head office, carried out
  on the replica when it can be: now, after it comes back online, or once its
  hardware answers.
- **A configuration page that tells the truth.** Beside each value, what the
  replica reports and what was wished, with where the wish stands: *on its
  way*, *waiting* and why (the module is off), *applied*, *refused* and why
  (out of range), or *superseded* (changed on the replica first). The shop's
  head office shows its registers' footers this way.
- **Fleet-wide changes you can watch.** Wish the same value on many replicas,
  and `Engine::wishes` and the dashboard count them where they stand: what is
  done, what waits, what was refused.
- **Hardware in the loop.** The replica carries a wish out through your
  handler, which talks to the device. It answers what the device answered,
  and a device that is away has the wish wait and be offered again.
- **Local changes that are not lost.** With `if_unchanged`, a technician's
  change on site wins over a head office edit based on what was there before.
- **Who asked.** A wish carries a note of yours (who, or why), and its answer
  carries when it was given.

## The row

> **You write**, the row the register owns:

```rust,ignore
{{#include ../../snippets/src/rows.rs:setting}}
```

A setting goes up as a record does, with three things more:

| | |
|---|---|
| `origin` | required: the replica the row belongs to, which is where a wish of it goes |
| `#[row(version)]` | required, a field of its own: what a wish is based on, and what the engine moves on when it writes one |
| `#[row(desired)]` | each column a wish may set, at least one; the others are the replica's alone |
| `apply` | which wish the replica carries out: `always` (the default) or `if_unchanged` |

The tables are yours, as for any row; the wishes live in a table of the
engine's, `sync_wishes`, which its schema's second step creates on each side
and its triggers cover — print them again (`Engine::triggers`,
`replica::triggers`) once you add a setting. A name beginning `sync_` is the
engine's on either side: no table of yours may take one.

## Wishing, on the server

> **You write**, where an operator sets a value:

```rust,ignore
{{#include ../../snippets/src/server.rs:desire}}
```

`desire` takes the row as the operator saw it, with the values they want: a
wish is its desired columns, and with `if_unchanged` its version is the one
the wish is based on. It writes the wish in your transaction, beside whatever
else you write; the next pass sends it to the register. Wishing the row again
replaces its wish, and is carried out again; `withdraw` takes it back.

> **What happens:** the wish is a row of `sync_wishes`, written on both sides:
> the server owns what is wished, the replica its answer. So it travels as
> every row does — recorded before it is sent, confirmed, provisioned again
> with the rest — and a register that runs a build without wishes refuses it,
> which shows on the dashboard as a row set aside for it.

## Carrying it out, on a replica

> **You write**, one handler per setting table, in the replica's settings:

```rust,ignore
{{#include ../../snippets/src/replica.rs:carry_out}}
```

The engine offers the handler each wish once the message that brought it is
committed, with no transaction open, in a task of its own: it may take its
time — up to `wishes_timeout` — and talk to the hardware. It is handed only a
wish of a row the replica holds (one of a row it does not hold yet waits for
the row), and, under `if_unchanged`, only while the row is at the version the
wish is based on. It answers:

- `Applied` — the engine writes the wished columns onto the row, its version
  moved on, and records the answer in the same transaction; both go up;
- `Refused(why)` — final for this wish; the row stays as it is;
- `Later(why)` — the wish waits, and is offered again: when you call
  `Replica::offer_wishes` (the hardware came back), after the next message
  that changes a wish, at start, and every `wishes_every` while one waits.

A handler that panics, or does not answer within `wishes_timeout`, has the
wish wait too; one wish's trouble never holds back another's. A write the
database refuses (a check of yours on the setting) answers the wish
`refused`, with the database's reason.

The shop's registers carry out the head office's wishes of their receipt
footer through their printer (`examples/shop/register/src/main.rs`):

```rust,ignore
{{#include ../../../examples/shop/register/src/main.rs:printer}}
```

A setting that lives only in the database needs no code: `Direct` answers
`Applied`, and the engine writes it. `Replica::new` refuses a setting with no
handler.

> **Mind:** a replica that stops after the handler ran and before the answer
> was recorded offers the same wish again; so does one restored from an older
> copy. Carrying the same wish out twice must leave the setting as once does.

## Strategies: who wins

With `always`, every wish is carried out: the last one wins over a change the
register's user made before it arrived, and a change made after it wins until
the next wish. With `if_unchanged`, a wish is carried out only while the
register still holds the version the operator saw; otherwise it is
**superseded** — not carried out, the register's value stands, and the
operator sees why. Either way the replica decides, once, and both sides then
show what it decided.

The server files a setting as it does a record, the newer version winning.
A register that is provisioned again sends its settings and its answers again,
and those the server takes whatever their version — they are what the register
holds now. A register's digests — hourly by default
(`Settings::digests_every`) — fingerprint its settings and its answers too, so
one the server finds apart from what it records — the server restored from an
older copy, say — is provisioned again. The snapshot names the settings the
server records it holding, and the register deletes, as its own write, any it
no longer holds: so the server lets go of a setting row only as the register's
delete of it is filed. Either side restored from an older copy, both still end
showing the same value, once a digest finds them apart.

## Where a wish stands

```rust,ignore
{{#include ../../snippets/src/server.rs:wishes}}
```

`pending` until the register answers this wish; then `applied`, `refused`,
`superseded`, or `waiting` with the handler's reason — the module that does
not answer, say, which the head office could not see otherwise.

The dashboard counts each replica's wishes by where they stand, and
`Engine::wishes` lists a scope's, those that need attention first, for
`WishList` to show ([The dashboard](dashboard.md)).
