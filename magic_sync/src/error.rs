/// What the engine can fail with.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Database(#[from] sqlx::Error),
    /// A declaration the schema cannot honour, or data that does not fit what
    /// a table declares: a programming error, never a storage failure.
    #[error("{0}")]
    Misuse(String),
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
            Error::Misuse(_) => false,
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

/// PostgreSQL: serialization failure, deadlock, shutdown, too many
/// connections, and the connection exceptions.
#[cfg(feature = "postgres")]
fn is_transient_code(code: &str) -> bool {
    matches!(
        code,
        "40001" | "40P01" | "57P01" | "57P02" | "57P03" | "53300" | "08000" | "08003" | "08006"
    )
}

/// SQLite: the database busy or locked by another connection, an I/O error,
/// a full disk — each the machine's, not the rows'. The primary result code
/// is the extended one's low byte.
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
fn is_transient_code(code: &str) -> bool {
    const BUSY: i32 = 5;
    const LOCKED: i32 = 6;
    const IOERR: i32 = 10;
    const FULL: i32 = 13;
    code.parse::<i32>()
        .is_ok_and(|c| matches!(c & 0xff, BUSY | LOCKED | IOERR | FULL))
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
