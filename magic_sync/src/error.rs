/// What the engine can fail with.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
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
    Malformed { table: String, reason: String },
    /// The transport refused or could not take a message.
    #[error("{reason}")]
    Transport { reason: String, transient: bool },
    /// The host failed at something the engine asked of it: who a replica is,
    /// a transaction to file in.
    #[error("{reason}")]
    Host { reason: String, transient: bool },
    /// A message of a later protocol than this build speaks. It is not acted
    /// on and not acknowledged: it waits for this side to be updated.
    #[error("a message of protocol {theirs}, and this build speaks {ours}")]
    Protocol { theirs: u32, ours: u32 },
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
/// memory, read-only, interrupted, an I/O error, a full disk, a file it
/// cannot open — each the machine's, not the rows'. The primary result code
/// is the extended one's low byte.
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
fn is_transient_code(code: &str) -> bool {
    const BUSY: i32 = 5;
    const LOCKED: i32 = 6;
    const NOMEM: i32 = 7;
    const READONLY: i32 = 8;
    const INTERRUPT: i32 = 9;
    const IOERR: i32 = 10;
    const FULL: i32 = 13;
    const CANTOPEN: i32 = 14;
    code.parse::<i32>().is_ok_and(|c| {
        matches!(
            c & 0xff,
            BUSY | LOCKED | NOMEM | READONLY | INTERRUPT | IOERR | FULL | CANTOPEN
        )
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
        #[cfg(all(feature = "postgres", not(feature = "sqlite")))]
        for code in ["58030", "58000", "40003", "08P01", "08007"] {
            assert!(is_transient_code(code), "{code}");
        }
        #[cfg(all(feature = "postgres", not(feature = "sqlite")))]
        assert!(!is_transient_code("23505"), "a duplicate is the rows'");
        // A busy or locked database is the machine's; a refused row is not.
        #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
        {
            assert!(is_transient_code("5"));
            assert!(is_transient_code("517"));
            assert!(is_transient_code("6"));
            assert!(!is_transient_code("1811"));
            assert!(!is_transient_code("2067"));
        }
        assert!(!Error::Transport {
            reason: "message too large".into(),
            transient: false
        }
        .is_transient());
    }
}
