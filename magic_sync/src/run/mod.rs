//! The engine, run for the host: the loops every host would otherwise write
//! the same way, and get wrong the same ways. Each is built on the step
//! functions of `relay` and `filing` (the server's) and
//! [`replica`](crate::replica), which stay public for a host that needs to
//! drive them itself.

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub mod replica;
#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub mod server;
