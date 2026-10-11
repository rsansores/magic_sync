//! The derive, compiled as a host compiles it: what it accepts builds, and
//! every misuse is refused with an error pointing at what says it. Each
//! refusal's message is checked in beside its case; `TRYBUILD=overwrite`
//! writes them anew.

#[test]
fn the_derive_builds_what_it_accepts_and_refuses_every_misuse_where_it_is_written() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/derive/pass/*.rs");
    cases.compile_fail("tests/derive/fail/*.rs");
}
