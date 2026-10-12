//! A replica's side.

use crate::rows::RegisterSetting;
use magic_sync::replica::Lane;
use magic_sync::run::replica::Settings;
use magic_sync::wish::{CarryOut, Outcome, Wish, Wishes};
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

// ANCHOR: carry_out
/// The screen's brightness is a value the hardware has to take: a wish of
/// it is carried out there, and the engine records what the setting then is.
pub struct Screen;

impl CarryOut<RegisterSetting> for Screen {
    async fn carry_out(&self, wish: Wish<RegisterSetting>) -> Outcome {
        if wish.wanted.name != "brightness" {
            // Every other setting lives only in the database.
            return Outcome::Applied;
        }
        let Ok(level) = wish.wanted.value.parse::<u8>() else {
            return Outcome::Refused(format!("`{}` is no brightness", wish.wanted.value));
        };
        match set_backlight(level) {
            Ok(()) => Outcome::Applied,
            Err(why) => Outcome::Later(why),
        }
    }
}

pub fn wishes(mut settings: Settings) -> Settings {
    settings.wishes = Wishes::new().on::<RegisterSetting>(Screen);
    settings
}
// ANCHOR_END: carry_out

fn set_backlight(_level: u8) -> Result<(), String> {
    Ok(())
}
