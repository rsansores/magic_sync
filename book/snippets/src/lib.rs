//! The code the book and the README show that is not the shop's
//! (`examples/shop`): compiled for each side, so that what they show is what
//! the engine takes. The book quotes each piece by its anchor; a test holds
//! the README to `readme.rs`.

pub mod codecs;
pub mod readme;
#[cfg(feature = "replica")]
pub mod replica;
pub mod rows;
#[cfg(feature = "server")]
pub mod server;
