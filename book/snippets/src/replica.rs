//! A replica's side.

use magic_sync::replica::Lane;
use magic_sync::run::replica::Settings;
use shop_rows::codecs;

// ANCHOR: triggers
pub fn print_triggers() {
    println!("{}", magic_sync::replica::triggers(&codecs()));
}
// ANCHOR_END: triggers

// ANCHOR: lanes
pub fn settings() -> Settings {
    let mut settings = Settings::default();
    settings.lanes = vec![
        Lane::new("main", &[]), // everything else, and what the engine says
        Lane::new("telemetry", &["positions"]),
    ];
    settings
}
// ANCHOR_END: lanes
