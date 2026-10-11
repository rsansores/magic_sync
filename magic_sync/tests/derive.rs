//! The derive, compiled as a host compiles it: what it accepts builds, and
//! every misuse is refused with an error pointing at what says it. Each
//! refusal's message is checked in beside its case; `TRYBUILD=overwrite`
//! writes them anew.
//!
//! The messages are the compiler's, and rustc rewords its diagnostics from
//! one release to the next, so the refusals are held to [`PINNED`] only: on
//! another toolchain what is accepted is built and the refusals are not run,
//! unless `MAGIC_SYNC_PINNED_DIAGNOSTICS` is set (CI's job on the pinned
//! toolchain sets it, and then another toolchain is an error).

/// The rustc release the `.stderr` files were written with. Moving it means
/// rewriting them (`TRYBUILD=overwrite`) and CI's `diagnostics` job.
const PINNED: &str = "1.97.1";

fn rustc_release() -> String {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let out = std::process::Command::new(rustc)
        .arg("--version")
        .output()
        .expect("rustc runs");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn the_derive_builds_what_it_accepts_and_refuses_every_misuse_where_it_is_written() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/derive/pass/*.rs");
    let release = rustc_release();
    if release.split_whitespace().nth(1) == Some(PINNED) {
        cases.compile_fail("tests/derive/fail/*.rs");
        return;
    }
    assert!(
        std::env::var_os("MAGIC_SYNC_PINNED_DIAGNOSTICS").is_none(),
        "the diagnostics are held to rustc {PINNED}, and this is {release}"
    );
    eprintln!("the refusals' messages are held to rustc {PINNED}: not compared on {release}");
}
