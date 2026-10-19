//! A row read from a view of its own, beside the table's own row.

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "districts", schema = "geo", mirror = "catalog", sync = "broadcast")]
pub struct District {
    pub id: uuid::Uuid,
    pub list_id: Option<uuid::Uuid>,
    pub name: String,
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "districts",
    schema = "geo",
    mirror = "catalog",
    sync = "broadcast",
    from = "erp.districts"
)]
pub struct ErpDistrict {
    pub id: uuid::Uuid,
    pub name: String,
    pub zone: String,
}

fn main() {
    use magic_sync::Row as _;
    assert_eq!(District::TABLE, ErpDistrict::TABLE);
}
