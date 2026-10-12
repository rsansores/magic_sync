/// What the engine can fail with. [`Error::is_transient`] says whether the
/// same work may succeed as it is once the infrastructure is back.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// The database failed, or refused a statement.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// A declaration the schema cannot honour, or a call the engine cannot
    /// carry out as made: a programming error, never the data's.
    #[error("{0}")]
    Misuse(String),
    /// Data that arrived and is not what its table declares: a row or a key
    /// that does not decode, a message that breaks the protocol. Set aside
    /// rather than retried: the same bytes fail the same way.
    #[error("`{table}`: {reason}")]
    Malformed {
        /// The table it was of; empty for a message rather than a row.
        table: String,
        /// What is wrong with it.
        reason: String,
    },
    /// The transport refused or could not take a message: what a host's
    /// transport or uplink answers.
    #[error("{reason}")]
    Transport {
        /// Why, as the transport says it.
        reason: String,
        /// Whether a retry may get it through (the network, the broker away)
        /// rather than never (a message the transport will never take).
        transient: bool,
    },
    /// The host failed at something the engine asked of it: who a replica is,
    /// a transaction to file in.
    #[error("{reason}")]
    Host {
        /// Why, as the host says it.
        reason: String,
        /// Whether the same question may be answered once the host is back.
        transient: bool,
    },
    /// A message of a later protocol than this build speaks. It is not acted
    /// on and not acknowledged: it waits for this side to be updated.
    #[error("a message of protocol {theirs}, and this build speaks {ours}")]
    Protocol {
        /// The protocol the message speaks.
        theirs: u32,
        /// The protocol this build speaks ([`PROTOCOL`](crate::envelope::PROTOCOL)).
        ours: u32,
    },
}

impl Error {
    /// Whether the infrastructure failed rather than the data or the code:
    /// the same work may succeed as it is once the database or the transport
    /// is back. Everything else is permanent — retrying it unchanged fails
    /// the same way — and the engine sets it aside instead of stopping on it.
    ///
    /// The transient side is the listed one, because it is the short, closed
    /// set: an error nobody listed is set aside and shows, rather than
    /// retried for ever.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Error::Database(e) => is_transient_sqlx(e),
            Error::Misuse(_) | Error::Malformed { .. } => false,
            Error::Transport { transient, .. } | Error::Host { transient, .. } => *transient,
            // The same message goes through once this side is updated.
            Error::Protocol { .. } => true,
        }
    }
}

impl Error {
    /// Whether the database refused what a statement wrote, rather than
    /// failed: the same rows are refused the same way, others may pass, and
    /// the engine leaves the refused ones out rather than stopping on them.
    /// On the server, anything but a transient failure.
    #[cfg(feature = "postgres")]
    pub(crate) fn is_refusal(&self) -> bool {
        !self.is_transient()
    }

    /// Whether the database refused what a statement wrote, rather than
    /// failed: the same rows are refused the same way, others may pass, and
    /// the engine leaves the refused ones out rather than stopping on them.
    /// On a replica, anything the database answers but a failure of its
    /// storage: a constraint, a trigger's `RAISE` or its own error, a value
    /// of the wrong type or too large — what a message carries never stops
    /// the replica, and a statement the engine got wrong is refused and
    /// reported, to be sent again once a fix is deployed.
    #[cfg(feature = "sqlite")]
    pub(crate) fn is_refusal(&self) -> bool {
        matches!(self, Error::Database(sqlx::Error::Database(_))) && !self.is_transient()
    }
}

fn is_transient_sqlx(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Io(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(db) => db.code().as_deref().is_some_and(is_transient_code),
        _ => false,
    }
}

/// PostgreSQL: serialization failure, deadlock, a statement whose completion
/// is unknown, a lock or statement timeout, shutdown, the server out of disk,
/// memory or connections, a system or I/O error, and the connection
/// exceptions — each the machine's, not the rows'.
#[cfg(feature = "postgres")]
fn is_transient_code(code: &str) -> bool {
    matches!(
        code,
        "40001"
            | "40P01"
            | "40003"
            | "55P03"
            | "57014"
            | "57P01"
            | "57P02"
            | "57P03"
            | "53100"
            | "53200"
            | "53300"
            | "53400"
            | "58000"
            | "58030"
            | "08000"
            | "08001"
            | "08003"
            | "08004"
            | "08006"
            | "08007"
            | "08P01"
    )
}

/// SQLite: the database busy or locked by another connection, out of
/// memory, interrupted, an I/O error, a full disk, a file it cannot open, a
/// file damaged or no database at all, or read-only for a while —
/// recovering, or locked by another process — each the machine's, not the
/// rows': a message must not be taken for applied, nor its rows refused,
/// while the machine is what fails. A database read-only for good (a file,
/// a directory, a database moved) is not: retrying meets it again. The
/// primary result code is the extended one's low byte.
#[cfg(feature = "sqlite")]
fn is_transient_code(code: &str) -> bool {
    const BUSY: i32 = 5;
    const LOCKED: i32 = 6;
    const NOMEM: i32 = 7;
    const READONLY: i32 = 8;
    const INTERRUPT: i32 = 9;
    const IOERR: i32 = 10;
    const CORRUPT: i32 = 11;
    const FULL: i32 = 13;
    const CANTOPEN: i32 = 14;
    const NOTADB: i32 = 26;
    const READONLY_RECOVERY: i32 = READONLY | (1 << 8);
    const READONLY_CANTLOCK: i32 = READONLY | (2 << 8);
    const READONLY_ROLLBACK: i32 = READONLY | (3 << 8);
    code.parse::<i32>().is_ok_and(|c| {
        matches!(
            c & 0xff,
            BUSY | LOCKED | NOMEM | INTERRUPT | IOERR | CORRUPT | FULL | CANTOPEN | NOTADB
        ) || matches!(c, READONLY_RECOVERY | READONLY_CANTLOCK | READONLY_ROLLBACK)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_infrastructure_is_worth_retrying_as_it_is() {
        assert!(Error::Database(sqlx::Error::PoolTimedOut).is_transient());
        assert!(Error::Transport {
            reason: "no responders".into(),
            transient: true
        }
        .is_transient());
        assert!(!Error::Misuse("a key that does not parse".into()).is_transient());
        assert!(!Error::Database(sqlx::Error::RowNotFound).is_transient());
        // The server's own failures: an I/O error, a statement whose fate
        // is unknown, a broken connection protocol.
        #[cfg(feature = "postgres")]
        for code in ["58030", "58000", "40003", "08P01", "08007"] {
            assert!(is_transient_code(code), "{code}");
        }
        #[cfg(feature = "postgres")]
        assert!(!is_transient_code("23505"), "a duplicate is the rows'");
        // A busy or locked database is the machine's; a refused row is not.
        #[cfg(feature = "sqlite")]
        {
            assert!(is_transient_code("5"));
            assert!(is_transient_code("517"));
            assert!(is_transient_code("6"));
            assert!(!is_transient_code("1811"));
            assert!(!is_transient_code("2067"));
            // Read-only while recovering, or locked by another process, for
            // a while; a read-only file, or one moved, for good.
            assert!(is_transient_code("264"));
            assert!(is_transient_code("520"));
            assert!(!is_transient_code("8"));
            assert!(!is_transient_code("1032"));
            // A damaged file, or none, is the machine's: no row is refused
            // for it, no message dropped.
            assert!(is_transient_code("11"));
            assert!(is_transient_code("267"));
            assert!(is_transient_code("26"));
        }
        assert!(!Error::Transport {
            reason: "message too large".into(),
            transient: false
        }
        .is_transient());
    }
}
