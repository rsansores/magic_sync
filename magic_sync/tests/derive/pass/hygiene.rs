//! The generated code names nothing the host's scope can shadow.

use ::std::primitive::u8 as bool;
use ::std::primitive::u8 as str;

struct Option;
struct Some;
struct None;
struct Vec;
struct String;
struct Result;
struct Ok;
struct Err;
mod core {}
mod std {}

macro_rules! vec {
    () => {
        "the host's vec!"
    };
}
macro_rules! assert {
    () => {
        "the host's assert!"
    };
}
macro_rules! panic {
    () => {
        "the host's panic!"
    };
}
macro_rules! matches {
    () => {
        "the host's matches!"
    };
}

#[derive(::sqlx::FromRow, Clone, ::magic_sync::Columns)]
pub struct Part {
    #[row(created)]
    pub created_at: ::chrono::DateTime<::chrono::Utc>,
}

#[derive(::sqlx::FromRow, Clone, ::magic_sync::Row)]
#[row(table = "things", mirror = "record", key = "replica_id, id", origin = "replica_id")]
pub struct Thing {
    pub replica_id: ::uuid::Uuid,
    pub id: i64,
    pub label: ::std::option::Option<::std::string::String>,
    #[row(version)]
    pub at: i64,
    #[sqlx(flatten)]
    pub part: Part,
}

fn main() {
    let _: (bool, str) = (0, 0);
    let _ = (Option, Some, None, Vec, String, Result, Ok, Err);
    let _ = (vec!(), assert!(), panic!(), matches!());
}
