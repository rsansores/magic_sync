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
        // Serialization failure, deadlock, shutdown, too many connections,
        // and the connection exceptions.
        sqlx::Error::Database(db) => matches!(
            db.code().as_deref(),
            Some(
                "40001"
                    | "40P01"
                    | "57P01"
                    | "57P02"
                    | "57P03"
                    | "53300"
                    | "08000"
                    | "08003"
                    | "08006"
            )
        ),
        _ => false,
    }
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
        assert!(!Error::Transport {
            reason: "message too large".into(),
            transient: false
        }
        .is_transient());
    }
}
