#[derive(sqlx::FromRow, Clone, serde::Serialize, serde::Deserialize, magic_sync::Row)]
#[row(table = "audits", mirror = "server")]
pub struct Audit {
    pub id: uuid::Uuid,
}

fn main() {
    let _ = magic_sync::wire::Codec::<()>::json::<Audit>(None);
}
