//! Several companies on one server.

// ANCHOR: partitioned
use magic_sync::plan::Replicas;

pub const REGISTERS: Replicas =
    Replicas::new("shop.registers", "registers", "active").partitioned_by("company_id");
// ANCHOR_END: partitioned
