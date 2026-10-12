#[derive(Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub enum Thing {
    One,
}

fn main() {}
