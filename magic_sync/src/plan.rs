//! The planner: which rows each replica holds, and what to send it so it holds
//! exactly those.
//!
//! Every table the server sends down declares how its rows find replicas
//! ([`Sync`]); nothing here names a table. From the declarations and the
//! foreign keys between the mirrored tables the engine builds, once, the query
//! that answers "who should hold which rows" of each table. Everything it does
//! is then one operation — compare what should be held with `sync.held`, what
//! is — narrowed to what a change can have moved:
//!
//! - a row changed: that row, sent to whoever should hold it, taken back from
//!   whoever holds it and no longer should; the rows that follow it, re-routed.
//! - a group changed (a member joined or left, a group was given or taken): the
//!   replicas it reaches, every row of the grouped table.
//! - an replica is (re)provisioned: that replica, every table, from scratch.
//!
//! A row travels with the rows it points at: a parent an replica lacks goes
//! with it, and one is taken back only once nothing the replica still holds
//! points at it. So an replica never holds a row whose parent it lacks.

use crate::envelope;
use crate::row::{Column, Key, KeyPart, Mirror, Sync, Table};
use crate::wire::Codec;
use crate::Error;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

/// The rows that are replicas, and which of them are live (have somewhere to
/// send to).
pub struct Replicas {
    /// The relation listing them, with an `id` column (and the partition's).
    pub relation: &'static str,
    /// The table behind it, as a foreign key names it.
    pub table: &'static str,
    /// Which of them receive: a condition on the relation's columns.
    pub live: &'static str,
    /// The column that splits rows and replicas into partitions no row
    /// crosses: a broadcast row goes to every live replica of its partition.
    /// `None`: one partition, every replica.
    pub partition: Option<&'static str>,
}

/// A foreign key from one mirrored table to another, as the child's columns
/// that hold the parent's key.
#[derive(Clone, Debug)]
struct Reference {
    parent: usize,
    /// The child's columns, in the parent's key order.
    columns: Vec<String>,
}

/// How a grouped table reaches replicas.
#[derive(Clone, Debug)]
struct Group {
    members: String,
    /// The members' column naming the grouped row.
    member_row: String,
    /// The column naming the group, in both tables.
    group: String,
    assigned: String,
    /// The assigned table's column naming the replica.
    assigned_replica: String,
}

pub(crate) struct Plan {
    pub(crate) table: &'static str,
    schema: &'static str,
    /// What the rows replicas should hold are read from.
    active: String,
    pub(crate) key: Vec<Column>,
    sync: Sync,
    references: Vec<Reference>,
    group: Option<Group>,
}

/// The rows to send to, and take back from, each replica.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct Delta {
    pub upserts: BTreeMap<Uuid, BTreeMap<usize, BTreeSet<String>>>,
    pub deletes: BTreeMap<Uuid, BTreeMap<usize, BTreeSet<String>>>,
    /// Replicas being provisioned: what they held is forgotten and what they
    /// should hold sent whole, then everything else swept on their side.
    pub snapshots: BTreeSet<Uuid>,
    /// The xmin of the snapshot the plan read: every transaction before it had
    /// finished, so what it noted is in the rows sent.
    pub horizon: i64,
}

impl Delta {
    fn upsert(&mut self, replica: Uuid, table: usize, key: String) {
        if let Some(keys) = self
            .deletes
            .get_mut(&replica)
            .and_then(|t| t.get_mut(&table))
        {
            keys.remove(&key);
        }
        self.upserts
            .entry(replica)
            .or_default()
            .entry(table)
            .or_default()
            .insert(key);
    }

    /// The keys of `table` being sent, per replica.
    fn sent(&self, table: usize) -> Vec<(Uuid, Vec<String>)> {
        self.upserts
            .iter()
            .filter_map(|(e, t)| t.get(&table).map(|k| (*e, k.iter().cloned().collect())))
            .collect()
    }

    /// The keys of `table` being taken back, per replica.
    fn taken(&self, table: usize) -> Vec<(Uuid, Vec<String>)> {
        self.deletes
            .iter()
            .filter_map(|(e, t)| t.get(&table).map(|k| (*e, k.iter().cloned().collect())))
            .collect()
    }

    fn unsend(&mut self, replica: Uuid, table: usize, key: &str) {
        if let Some(keys) = self
            .upserts
            .get_mut(&replica)
            .and_then(|t| t.get_mut(&table))
        {
            keys.remove(key);
        }
    }

    fn delete(&mut self, replica: Uuid, table: usize, key: String) {
        self.deletes
            .entry(replica)
            .or_default()
            .entry(table)
            .or_default()
            .insert(key);
    }

    fn has_upsert(&self, replica: Uuid, table: usize, key: &str) -> bool {
        self.upserts
            .get(&replica)
            .and_then(|t| t.get(&table))
            .is_some_and(|k| k.contains(key))
    }

    fn has_delete(&self, replica: Uuid, table: usize, key: &str) -> bool {
        self.deletes
            .get(&replica)
            .and_then(|t| t.get(&table))
            .is_some_and(|k| k.contains(key))
    }

    /// Only what `replica` is owed: what a note planned for that replica alone
    /// asks.
    pub fn keep_only(&mut self, replica: Uuid) {
        self.upserts.retain(|r, _| *r == replica);
        self.deletes.retain(|r, _| *r == replica);
        self.snapshots.retain(|r| *r == replica);
    }

    /// Everything `replica` is owed, left out: it was not sent, so it is not
    /// recorded.
    pub fn forget(&mut self, replica: Uuid) {
        self.upserts.remove(&replica);
        self.deletes.remove(&replica);
        self.snapshots.remove(&replica);
    }

    /// Every row this delta sends `replica`, as its table and key.
    pub fn upserted(&self, replica: Uuid) -> Vec<(usize, String)> {
        self.upserts
            .get(&replica)
            .into_iter()
            .flat_map(|tables| {
                tables
                    .iter()
                    .flat_map(|(i, keys)| keys.iter().map(move |k| (*i, k.clone())))
            })
            .collect()
    }

    /// What is held, less what the snapshot replicas held: they start from
    /// nothing, so it is not there to count on.
    fn fresh(&self, held: BTreeSet<(Uuid, String)>) -> BTreeSet<(Uuid, String)> {
        if self.snapshots.is_empty() {
            return held;
        }
        held.into_iter()
            .filter(|(e, _)| !self.snapshots.contains(e))
            .collect()
    }
}

/// A pending note: what changed, and the one replica it is still owed to
/// when a send to that replica failed.
#[derive(Clone, Debug)]
pub struct Note {
    pub id: i64,
    pub change: Change,
    pub scope: Option<Uuid>,
}

/// What changed, as the outbox noted it.
#[derive(Clone, Debug)]
pub enum Change {
    /// A row of a mirrored table, by its key, and the row as it was before an
    /// update or a delete.
    Row {
        table: String,
        key: String,
        previous: Option<serde_json::Value>,
        /// The transaction that noted it.
        xid: i64,
    },
    /// A member joined or left a group of a grouped table's members table: the
    /// member row, as it was or is.
    Members {
        table: String,
        member: serde_json::Value,
    },
    /// A group was given to or taken from an replica.
    Assigned { table: String, replica: Uuid },
    /// An replica is provisioned again, from scratch.
    Resync { replica: Uuid },
}

/// Every row a plan sends, encoded once: by table index and key text.
#[derive(Default)]
pub struct Loaded {
    rows: HashMap<(usize, String), Vec<u8>>,
    /// Rows that did not encode, per replica they were to go to: left out of
    /// the delta, neither sent nor recorded as held.
    pub failed: Vec<Unsent>,
}

/// A row the engine could not send a replica, and why.
#[derive(Debug)]
pub struct Unsent {
    pub replica: Uuid,
    pub table: &'static str,
    pub key: String,
    pub reason: String,
}

/// A foreign key as the catalogue reads: the child's schema and table, the
/// parent's, then the child's and the parent's columns, in order.
type ForeignKey = (String, String, String, String, Vec<String>, Vec<String>);

/// Rows `record` writes per statement.
const RECORD_CHUNK: usize = 50_000;

/// The engine, built once from the codecs and the schema.
pub struct Engine {
    pub(crate) plans: Vec<Plan>,
    by_name: HashMap<&'static str, usize>,
    /// Parents before children.
    pub(crate) order: Vec<usize>,
    pub(crate) replicas: Replicas,
}

type Conn = crate::row::Conn;

fn key_sql(alias: &str, columns: &[impl AsRef<str>]) -> String {
    let parts: Vec<String> = columns
        .iter()
        .map(|c| format!("{alias}.{}::text", c.as_ref()))
        .collect();
    format!("concat_ws('|', {})", parts.join(", "))
}

/// `columns` (named as in the table aliased `alias`, typed as `types`) among
/// the key texts of the array parameter `param`. Each part of the text is cast
/// back to its column's type and compared as a row value, so the table's key
/// index serves a composite key as well as a single one.
fn key_in(alias: &str, columns: &[impl AsRef<str>], types: &[Column], param: &str) -> String {
    if let ([column], [typed]) = (columns, types) {
        return format!(
            "{alias}.{} IN (SELECT unnest({param}::text[])::{})",
            column.as_ref(),
            KeyPart::sql_type(typed)
        );
    }
    let row: Vec<String> = columns
        .iter()
        .map(|c| format!("{alias}.{}", c.as_ref()))
        .collect();
    let parts: Vec<String> = types
        .iter()
        .enumerate()
        .map(|(i, typed)| {
            format!(
                "split_part(k, '|', {})::{}",
                i + 1,
                KeyPart::sql_type(typed)
            )
        })
        .collect();
    format!(
        "({}) IN (SELECT {} FROM unnest({param}::text[]) AS k)",
        row.join(", "),
        parts.join(", ")
    )
}

/// Parses a key back into its parts, as the table's key columns type them.
pub(crate) fn parse_key(columns: &[Column], text: &str) -> Result<Key, Error> {
    columns
        .iter()
        .zip(text.split('|'))
        .map(|(column, part)| KeyPart::parse(column, part))
        .collect()
}

impl Engine {
    /// Reads the foreign keys between the mirrored tables and each grouped
    /// table's group tables from the catalog. Fails on a declaration the
    /// schema cannot honour — a `follows` column that is not a key, a group
    /// table that does not have the columns a group needs, a cycle.
    pub async fn new(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<Self, Error> {
        Self::build(conn, tables, replicas, false).await
    }

    /// [`Engine::new`], having first installed what the schema lacks: the
    /// engine's own tables when the database has none, and the triggers of
    /// every mirrored table and group table that notes nothing yet. A table
    /// mirrored for the first time that already has rows is sent whole: every
    /// live replica is provisioned again. Idempotent.
    ///
    /// For a host that has no migrations of its own. Run it before anything
    /// writes a mirrored table — a write before its trigger is never noted —
    /// on a connection that may create triggers. A host with migrations runs
    /// [`POSTGRES_SCHEMA`](crate::POSTGRES_SCHEMA) in one and declares each
    /// table's triggers in the migration that creates it, as it does its
    /// indexes; [`Engine::new`] refuses to start while one is missing.
    pub async fn install(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<Self, Error> {
        let installed: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sync')")
                .fetch_one(&mut *conn)
                .await?;
        if !installed {
            sqlx::raw_sql(crate::POSTGRES_SCHEMA)
                .execute(&mut *conn)
                .await?;
        }
        Self::build(conn, tables, replicas, true).await
    }

    async fn build(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
        install: bool,
    ) -> Result<Self, Error> {
        let versioned: bool = sqlx::query_scalar("SELECT to_regclass('sync.version') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
        let version: Option<i32> = if versioned {
            sqlx::query_scalar("SELECT version FROM sync.version")
                .fetch_optional(&mut *conn)
                .await?
        } else {
            None
        };
        if version != Some(crate::SCHEMA_VERSION) {
            return Err(Error::Misuse(format!(
                "the engine's schema is version {}, and this build needs {}: install it",
                version.map_or("none".to_owned(), |v| v.to_string()),
                crate::SCHEMA_VERSION
            )));
        }
        let tables: Vec<&Table> = tables
            .iter()
            .copied()
            .filter(|t| matches!(t.mirror, Mirror::Catalog | Mirror::Both))
            .collect();
        let by_name: HashMap<&'static str, usize> = tables
            .iter()
            .enumerate()
            .map(|(i, t)| (t.name, i))
            .collect();

        let foreign_keys: Vec<ForeignKey> = sqlx::query_as(
            "SELECT child_ns.nspname::text, child.relname::text,
                    parent_ns.nspname::text, parent.relname::text,
                    ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY AS k(n, o)
                          JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n
                          ORDER BY k.o),
                    ARRAY(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY AS k(n, o)
                          JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n
                          ORDER BY k.o)
               FROM pg_constraint c
               JOIN pg_class child ON child.oid = c.conrelid
               JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
               JOIN pg_class parent ON parent.oid = c.confrelid
               JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
              WHERE c.contype = 'f'",
        )
        .fetch_all(&mut *conn)
        .await?;

        let mut plans: Vec<Plan> = tables
            .iter()
            .map(|t| Plan {
                table: t.name,
                schema: t.schema,
                active: t
                    .active
                    .map_or_else(|| format!("{}.{}", t.schema, t.name), str::to_owned),
                key: t
                    .key
                    .iter()
                    .map(|k| {
                        *t.columns
                            .iter()
                            .find(|c| c.name == *k)
                            .expect("a key column is a column")
                    })
                    .collect(),
                sync: t.sync,
                references: Vec::new(),
                group: None,
            })
            .collect();

        let mirrored = |schema: &str, name: &str| {
            by_name
                .get(name)
                .copied()
                .filter(|&i| plans[i].schema == schema)
        };
        let mut references: Vec<(usize, Reference)> = Vec::new();
        for (child_schema, child, parent_schema, parent, child_columns, parent_columns) in
            &foreign_keys
        {
            let (Some(c), Some(p)) = (
                mirrored(child_schema, child),
                mirrored(parent_schema, parent),
            ) else {
                continue;
            };
            if c == p {
                return Err(Error::Misuse(format!(
                    "`{child}` points at itself: the engine cannot order a table's rows among themselves"
                )));
            }
            // The child's columns that hold the parent's key, in its order.
            let parent_key: Vec<&str> = plans[p].key.iter().map(|k| k.name).collect();
            let columns: Option<Vec<String>> = parent_key
                .iter()
                .map(|k| {
                    parent_columns
                        .iter()
                        .position(|pc| pc == k)
                        .map(|i| child_columns[i].clone())
                })
                .collect();
            if let Some(columns) = columns {
                references.push((c, Reference { parent: p, columns }));
            }
        }
        for (c, reference) in references {
            plans[c].references.push(reference);
        }

        let unqualified: Vec<(String, String, Vec<String>, Vec<String>)> = foreign_keys
            .iter()
            .map(|(_, c, _, p, cc, pc)| (c.clone(), p.clone(), cc.clone(), pc.clone()))
            .collect();
        for plan in &mut plans {
            match plan.sync {
                Sync::Follows(column) => {
                    if !plan
                        .references
                        .iter()
                        .any(|r| r.columns == [column.to_owned()])
                    {
                        return Err(Error::Misuse(format!(
                            "`{}` follows `{column}`, which is not a key into a mirrored table",
                            plan.table
                        )));
                    }
                }
                Sync::Grouped { members, assigned } => {
                    plan.group = Some(group_of(
                        &unqualified,
                        plan.table,
                        members,
                        assigned,
                        replicas.table,
                        replicas.partition,
                    )?);
                }
                _ => {}
            }
        }

        let columns: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT table_schema::text, table_name::text, column_name::text \
               FROM information_schema.columns",
        )
        .fetch_all(&mut *conn)
        .await?;
        for plan in &plans {
            let has = |column: &str| {
                columns
                    .iter()
                    .any(|(s, t, c)| s == plan.schema && t == plan.table && c == column)
            };
            let missing = match plan.sync {
                Sync::Broadcast => replicas.partition.filter(|p| !has(p)),
                Sync::Assigned(column) if !has(column) => Some(column),
                _ => None,
            };
            if let Some(column) = missing {
                return Err(Error::Misuse(format!(
                    "`{}` is sent by `{column}`, which it does not have",
                    plan.table
                )));
            }
        }
        let noted: Vec<(String, String)> = sqlx::query_as(
            "SELECT n.nspname::text, c.relname::text FROM pg_trigger t \
               JOIN pg_class c ON c.oid = t.tgrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
              WHERE t.tgname IN ('sync_note_written', 'sync_note_group')",
        )
        .fetch_all(&mut *conn)
        .await?;
        let is_noted =
            |schema: &str, table: &str| noted.iter().any(|(s, t)| s == schema && t == table);
        let partition = replicas.partition.unwrap_or_default();
        let mut first_mirrored = Vec::new();
        for (plan, table) in plans.iter().zip(&tables) {
            if !is_noted(plan.schema, plan.table) {
                if !install {
                    return Err(Error::Misuse(format!(
                        "`{}.{}` is mirrored but notes nothing: call sync.mirror_table() after it",
                        plan.schema, plan.table
                    )));
                }
                let owned: Vec<&str> = table
                    .columns
                    .iter()
                    .filter(|c| c.replica_owned)
                    .map(|c| c.name)
                    .collect();
                sqlx::query("SELECT sync.mirror_table($1, $2, $3, $4)")
                    .bind(plan.schema)
                    .bind(plan.table)
                    .bind(&owned)
                    .bind(partition)
                    .execute(&mut *conn)
                    .await?;
                first_mirrored.push(format!("{}.{}", plan.schema, plan.table));
            }
            if let Some(g) = &plan.group {
                for (table, column, role) in [
                    (&g.members, &g.group, "members"),
                    (&g.assigned, &g.assigned_replica, "assigned"),
                ] {
                    if noted.iter().any(|(_, t)| t == table) {
                        continue;
                    }
                    if !install {
                        return Err(Error::Misuse(format!(
                            "`{table}` groups `{}` but notes nothing: call sync.mirror_group() after it",
                            plan.table
                        )));
                    }
                    sqlx::query(
                        "SELECT sync.mirror_group(n.nspname, c.relname, $2, $3, $4) \
                           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                          WHERE c.relname = $1 AND c.relkind IN ('r', 'p') LIMIT 1",
                    )
                    .bind(table)
                    .bind(column)
                    .bind(role)
                    .bind(partition)
                    .execute(&mut *conn)
                    .await?;
                }
            }
        }
        for table in &first_mirrored {
            let has_rows: bool =
                sqlx::query_scalar(&format!("SELECT EXISTS (SELECT 1 FROM {table})"))
                    .fetch_one(&mut *conn)
                    .await?;
            if has_rows {
                // Rows written before it was noted reached no replica: every
                // replica is provisioned again, and gets them.
                sqlx::query(&format!(
                    "INSERT INTO sync.notes (kind, replica_id, partition_id) \
                     SELECT 'resync', id, {} FROM {} WHERE {}",
                    replicas.partition.unwrap_or("NULL::uuid"),
                    replicas.relation,
                    replicas.live
                ))
                .execute(&mut *conn)
                .await?;
                break;
            }
        }

        // A noted table no row declares: its changes are noted, and nothing
        // would ever send them.
        if let Some((schema, table)) = noted.iter().find(|(schema, table)| {
            !plans.iter().any(|p| p.schema == schema && p.table == table)
                && !plans.iter().any(|p| {
                    p.group
                        .as_ref()
                        .is_some_and(|g| g.members == *table || g.assigned == *table)
                })
        }) {
            return Err(Error::Misuse(format!(
                "`{schema}.{table}` is noted, but no row declares it: list its row with the others"
            )));
        }
        let order = topological(&plans)?;
        Ok(Self {
            plans,
            by_name,
            order,
            replicas,
        })
    }

    pub(crate) fn index(&self, table: &str) -> Option<usize> {
        self.by_name.get(table).copied()
    }

    pub fn table(&self, index: usize) -> &'static str {
        self.plans[index].table
    }

    /// The live replicas, as a subquery `s(id, partition)`.
    pub(crate) fn replicas_sql(&self) -> String {
        let partition = self.replicas.partition.unwrap_or("NULL::uuid");
        format!(
            "(SELECT id, {partition} AS partition FROM {} WHERE {}) AS s",
            self.replicas.relation, self.replicas.live
        )
    }

    /// How a broadcast row meets the replicas of its partition.
    fn same_partition(&self) -> String {
        match self.replicas.partition {
            Some(column) => format!("s.partition = t.{column}"),
            None => "true".to_owned(),
        }
    }

    /// `SELECT replica_id, key` for every row of a table its strategy sends
    /// to a live replica, narrowed by `filter` on the table aliased `t`;
    /// `None` for a table that travels only when pointed at, or with its
    /// parent.
    pub(crate) fn wanted_sql(&self, i: usize, filter: &str) -> Option<String> {
        let plan = &self.plans[i];
        let key = key_sql("t", &plan.key.iter().map(|c| c.name).collect::<Vec<_>>());
        let replicas = self.replicas_sql();
        let same_partition = self.same_partition();
        let view = &plan.active;
        Some(match plan.sync {
            Sync::Referenced => return None,
            Sync::Broadcast => format!(
                "SELECT s.id AS replica_id, {key} AS key FROM {view} t \
                 JOIN {replicas} ON {same_partition} WHERE {filter}"
            ),
            Sync::Assigned(column) => format!(
                "SELECT s.id AS replica_id, {key} AS key FROM {view} t \
                 JOIN {replicas} ON s.id = t.{column} WHERE {filter}"
            ),
            Sync::Grouped { .. } => {
                let g = plan
                    .group
                    .as_ref()
                    .expect("a grouped table's group is read in new()");
                format!(
                    "SELECT s.id AS replica_id, {key} AS key FROM {view} t \
                     JOIN {members} m ON m.{member_row} = t.{row_key} \
                     JOIN {assigned} a ON a.{group} = m.{group} \
                     JOIN {replicas} ON s.id = a.{replica} WHERE {filter}",
                    members = g.members,
                    member_row = g.member_row,
                    row_key = plan.key[0].name,
                    assigned = g.assigned,
                    group = g.group,
                    replica = g.assigned_replica,
                )
            }
            Sync::Follows(_) => return None,
        })
    }

    /// Who should hold which of `keys` (`None`: every key) and, where
    /// `replicas` is given, only among those.
    pub(crate) async fn wanted(
        &self,
        conn: &mut Conn,
        i: usize,
        keys: Option<&[String]>,
        replicas: Option<&[Uuid]>,
    ) -> Result<BTreeSet<(Uuid, String)>, Error> {
        let plan = &self.plans[i];
        let names: Vec<&str> = plan.key.iter().map(|c| c.name).collect();
        let filter = match keys {
            Some(_) => key_in("t", &names, &plan.key, "$1"),
            None => "$1::text[] IS NULL".to_owned(),
        };
        let Some(sql) = self.wanted_sql(i, &filter) else {
            return Ok(BTreeSet::new());
        };
        let rows: Vec<(Uuid, String)> = sqlx::query_as(&format!(
            "SELECT replica_id, key FROM ({sql}) w \
             WHERE $2::uuid[] IS NULL OR w.replica_id = ANY($2)"
        ))
        .bind(keys)
        .bind(replicas)
        .fetch_all(&mut *conn)
        .await?;
        Ok(rows.into_iter().collect())
    }

    /// Who holds which of `keys` (`None`: every key), among `replicas`
    /// when given.
    async fn held(
        &self,
        conn: &mut Conn,
        i: usize,
        keys: Option<&[String]>,
        replicas: Option<&[Uuid]>,
    ) -> Result<BTreeSet<(Uuid, String)>, Error> {
        let rows: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT replica_id, row_key FROM sync.held \
             WHERE relation = $1 \
               AND ($2::text[] IS NULL OR row_key IN (SELECT unnest($2::text[]))) \
               AND ($3::uuid[] IS NULL OR replica_id = ANY($3))",
        )
        .bind(self.plans[i].table)
        .bind(keys)
        .bind(replicas)
        .fetch_all(&mut *conn)
        .await?;
        Ok(rows.into_iter().collect())
    }

    /// A change to rows of `i`: routed again.
    async fn reconcile_keys(
        &self,
        conn: &mut Conn,
        delta: &mut Delta,
        i: usize,
        keys: &[String],
        notes: Option<&BTreeMap<String, i64>>,
    ) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let horizons = self.horizons(conn, i, keys).await?;
        let held: BTreeSet<(Uuid, String)> = horizons.keys().cloned().collect();
        let wanted = if matches!(self.plans[i].sync, Sync::Referenced) {
            // Travels only when pointed at: whoever holds it keeps it for now,
            // unless it is gone; the pass over references decides.
            let existing = self.existing(conn, i, keys).await?;
            held.iter()
                .filter(|(_, k)| existing.contains(k))
                .cloned()
                .collect()
        } else {
            self.wanted(conn, i, Some(keys), None).await?
        };
        for holder in &wanted {
            if Self::owed(&horizons, notes, holder) {
                delta.upsert(holder.0, i, holder.1.clone());
            }
        }
        for (replica, key) in held.difference(&wanted) {
            delta.delete(*replica, i, key.clone());
        }
        Ok(())
    }

    /// Whether `holder` must be sent its row: it lacks it, or holds a version
    /// sent from before the latest change `notes` has for it — a change whose
    /// transaction the send's horizon is past is already in what it holds.
    fn owed(
        horizons: &BTreeMap<(Uuid, String), i64>,
        notes: Option<&BTreeMap<String, i64>>,
        holder: &(Uuid, String),
    ) -> bool {
        match horizons.get(holder) {
            None => true,
            Some(horizon) => notes
                .and_then(|n| n.get(&holder.1))
                .is_some_and(|xid| xid >= horizon),
        }
    }

    /// Who holds which of `keys`, and the horizon each was last sent from.
    async fn horizons(
        &self,
        conn: &mut Conn,
        i: usize,
        keys: &[String],
    ) -> Result<BTreeMap<(Uuid, String), i64>, Error> {
        let rows: Vec<(Uuid, String, i64)> = sqlx::query_as(
            "SELECT replica_id, row_key, horizon::text::bigint FROM sync.held \
             WHERE relation = $1 AND row_key = ANY($2)",
        )
        .bind(self.plans[i].table)
        .bind(keys)
        .fetch_all(&mut *conn)
        .await?;
        Ok(rows.into_iter().map(|(e, k, h)| ((e, k), h)).collect())
    }

    /// Every row of `i`, for `replicas`: what only who-holds-what can have moved.
    async fn reconcile_replicas(
        &self,
        conn: &mut Conn,
        delta: &mut Delta,
        i: usize,
        replicas: &[Uuid],
    ) -> Result<(), Error> {
        if replicas.is_empty() {
            return Ok(());
        }
        let held = self.held(conn, i, None, Some(replicas)).await?;
        let wanted = self.wanted(conn, i, None, Some(replicas)).await?;
        for (replica, key) in wanted.difference(&held) {
            delta.upsert(*replica, i, key.clone());
        }
        for (replica, key) in held.difference(&wanted) {
            delta.delete(*replica, i, key.clone());
        }
        Ok(())
    }

    /// What a batch of changes asks of every replica.
    pub async fn plan(&self, conn: &mut Conn, changes: &[Change]) -> Result<Delta, Error> {
        let horizon: i64 =
            sqlx::query_scalar("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint")
                .fetch_one(&mut *conn)
                .await?;
        let mut delta = Delta {
            horizon,
            ..Delta::default()
        };
        // Per table, each changed key and the latest transaction that noted it.
        let mut rows: BTreeMap<usize, BTreeMap<String, i64>> = BTreeMap::new();
        let mut released = Released::default();
        let mut regrouped: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
        let mut reassigned: BTreeMap<usize, BTreeSet<Uuid>> = BTreeMap::new();
        for change in changes {
            match change {
                Change::Row {
                    table,
                    key,
                    previous,
                    xid,
                } => {
                    if let Some(i) = self.index(table) {
                        // A key its table cannot have fails here, the same way
                        // every time, rather than wherever a query casts it.
                        parse_key(&self.plans[i].key, key)?;
                        let latest = rows.entry(i).or_default().entry(key.clone()).or_default();
                        *latest = (*latest).max(*xid);
                        if let Some(previous) = previous {
                            released
                                .previous
                                .entry((i, key.clone()))
                                .or_default()
                                .push(previous.clone());
                        }
                    }
                }
                Change::Members { table, member } => {
                    for (i, g) in self.groups_through(table, |g| &g.members) {
                        if let Some(key) = member.get(&g.member_row).and_then(|v| v.as_str()) {
                            regrouped.entry(i).or_default().insert(key.to_owned());
                        }
                    }
                }
                Change::Assigned { table, replica } => {
                    for (i, _) in self.groups_through(table, |g| &g.assigned) {
                        reassigned.entry(i).or_default().insert(*replica);
                    }
                }
                Change::Resync { replica } => {
                    delta.snapshots.insert(*replica);
                }
            }
        }
        // A member that moved is routed again by its key; a group given or
        // taken, once per table and batch for its replicas.
        for (i, keys) in regrouped {
            let keys: Vec<String> = keys.into_iter().collect();
            self.reconcile_keys(conn, &mut delta, i, &keys, None)
                .await?;
        }
        for (i, replicas) in reassigned {
            let replicas: Vec<Uuid> = replicas.into_iter().collect();
            self.reconcile_replicas(conn, &mut delta, i, &replicas)
                .await?;
        }
        let mut touched: BTreeMap<usize, BTreeMap<String, i64>> = BTreeMap::new();
        for (i, notes) in rows {
            if matches!(self.plans[i].sync, Sync::Follows(_)) {
                touched.insert(i, notes);
                continue;
            }
            let keys: Vec<String> = notes.keys().cloned().collect();
            self.reconcile_keys(conn, &mut delta, i, &keys, Some(&notes))
                .await?;
        }
        let snapshots: Vec<Uuid> = delta.snapshots.iter().copied().collect();
        if !snapshots.is_empty() {
            for &i in &self.order {
                let wanted = self.wanted(conn, i, None, Some(&snapshots)).await?;
                for (replica, key) in wanted {
                    delta.upsert(replica, i, key);
                }
            }
            // A snapshot's replica starts from nothing: its side sweeps
            // whatever the snapshot does not name.
            for replica in &snapshots {
                delta.deletes.remove(replica);
            }
        }
        // Each pass can give the others work — a parent a follower needs, a
        // parent a deleted follower released — so they run until none does.
        for _ in 0..16 {
            let before = (delta.upserts.clone(), delta.deletes.clone());
            self.close_references(conn, &mut delta).await?;
            self.release_references(conn, &mut delta, &released).await?;
            self.follow_parents(conn, &mut delta, &touched).await?;
            if before == (delta.upserts.clone(), delta.deletes.clone()) {
                return Ok(delta);
            }
        }
        Err(Error::Misuse(
            "the sync plan did not settle in 16 passes".to_owned(),
        ))
    }

    fn groups_through(&self, table: &str, pick: impl Fn(&Group) -> &String) -> Vec<(usize, Group)> {
        self.plans
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                p.group
                    .as_ref()
                    .filter(|g| pick(g) == table)
                    .map(|g| (i, g.clone()))
            })
            .collect()
    }

    /// The parents a row points at, per row: `(child key, parent key)` for one
    /// reference of `i`.
    async fn parents_of(
        &self,
        conn: &mut Conn,
        i: usize,
        reference: &Reference,
        keys: &[String],
        active: bool,
    ) -> Result<Vec<(String, String)>, Error> {
        let plan = &self.plans[i];
        let from = if active {
            plan.active.clone()
        } else {
            qualified(plan)
        };
        let names: Vec<&str> = plan.key.iter().map(|c| c.name).collect();
        let key = key_sql("t", &names);
        let parent_key = key_sql("t", &reference.columns);
        let not_null: Vec<String> = reference
            .columns
            .iter()
            .map(|c| format!("t.{c} IS NOT NULL"))
            .collect();
        Ok(sqlx::query_as(&format!(
            "SELECT {key}, {parent_key} FROM {from} t WHERE {} AND {}",
            key_in("t", &names, &plan.key, "$1"),
            not_null.join(" AND ")
        ))
        .bind(keys)
        .fetch_all(&mut *conn)
        .await?)
    }

    /// Which of `keys` still exist in the table, retired or not.
    async fn existing(
        &self,
        conn: &mut Conn,
        i: usize,
        keys: &[String],
    ) -> Result<BTreeSet<String>, Error> {
        let plan = &self.plans[i];
        let names: Vec<&str> = plan.key.iter().map(|c| c.name).collect();
        let rows: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT {} FROM {} t WHERE {}",
            key_sql("t", &names),
            qualified(plan),
            key_in("t", &names, &plan.key, "$1")
        ))
        .bind(keys)
        .fetch_all(&mut *conn)
        .await?;
        Ok(rows.into_iter().collect())
    }

    /// Sends along every parent a sent row points at that its replica lacks,
    /// children first so a parent's own parents follow.
    async fn close_references(&self, conn: &mut Conn, delta: &mut Delta) -> Result<(), Error> {
        for &i in self.order.iter().rev() {
            let sent = delta.sent(i);
            if sent.is_empty() {
                continue;
            }
            let keys: Vec<String> = sent
                .iter()
                .flat_map(|(_, keys)| keys.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            for reference in self.plans[i].references.clone() {
                let parent_of: BTreeMap<String, String> = self
                    .parents_of(conn, i, &reference, &keys, false)
                    .await?
                    .into_iter()
                    .collect();
                let parent_keys: Vec<String> = parent_of
                    .values()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                // A snapshot starts its replica from nothing: what it held
                // before is not there to count on.
                let replicas: Vec<Uuid> = sent
                    .iter()
                    .map(|(e, _)| *e)
                    .filter(|e| !delta.snapshots.contains(e))
                    .collect();
                let held = if replicas.is_empty() || parent_keys.is_empty() {
                    BTreeSet::new()
                } else {
                    self.held(conn, reference.parent, Some(&parent_keys), Some(&replicas))
                        .await?
                };
                for (replica, keys) in &sent {
                    for parent in keys.iter().filter_map(|k| parent_of.get(k)) {
                        if !held.contains(&(*replica, parent.clone()))
                            && !delta.has_upsert(*replica, reference.parent, parent)
                        {
                            delta.upsert(*replica, reference.parent, parent.clone());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// The parent keys a row points at through `reference`, read off a row as
    /// it was (`previous`, a changed or deleted row's old values).
    fn parent_in(reference: &Reference, row: &serde_json::Value) -> Option<String> {
        let parts: Option<Vec<String>> = reference
            .columns
            .iter()
            .map(|c| match row.get(c)? {
                serde_json::Value::String(v) => Some(v.clone()),
                serde_json::Value::Number(v) => Some(v.to_string()),
                _ => None,
            })
            .collect();
        parts.map(|p| p.join("|"))
    }

    /// Takes back what nothing an replica keeps points at any more, and
    /// keeps what something still does.
    ///
    /// The candidates are the deletes already decided — a row an replica
    /// should no longer hold — and the parents a change released: the ones a
    /// changed row pointed at before, for whoever holds it. Tables are visited
    /// children first, so a delete that stands releases its own parents in
    /// turn. A candidate stays when a row its replica keeps or is being sent
    /// points at it, or when its own strategy still sends it there.
    async fn release_references(
        &self,
        conn: &mut Conn,
        delta: &mut Delta,
        released: &Released,
    ) -> Result<(), Error> {
        let mut candidates: BTreeMap<usize, BTreeSet<(Uuid, String)>> = BTreeMap::new();
        let mut by_table: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        for (i, key) in released.previous.keys() {
            by_table.entry(*i).or_default().push(key.clone());
        }
        let mut holders_of: BTreeMap<(usize, String), Vec<Uuid>> = BTreeMap::new();
        for (i, keys) in &by_table {
            for (replica, key) in self.held(conn, *i, Some(keys), None).await? {
                holders_of.entry((*i, key)).or_default().push(replica);
            }
        }
        for ((i, key), versions) in &released.previous {
            let holders = holders_of
                .get(&(*i, key.clone()))
                .cloned()
                .unwrap_or_default();
            for reference in &self.plans[*i].references {
                for previous in versions {
                    let Some(parent) = Self::parent_in(reference, previous) else {
                        continue;
                    };
                    for replica in &holders {
                        candidates
                            .entry(reference.parent)
                            .or_default()
                            .insert((*replica, parent.clone()));
                    }
                }
            }
        }

        for &p in self.order.iter().rev() {
            let mut examined: BTreeSet<(Uuid, String)> = delta
                .deletes
                .iter()
                .flat_map(|(e, t)| {
                    t.get(&p)
                        .into_iter()
                        .flatten()
                        .map(move |k| (*e, k.clone()))
                })
                .collect();
            if let Some(released) = candidates.remove(&p) {
                // Only what is held and not on its way back can be taken back.
                let keys: Vec<String> = released.iter().map(|(_, k)| k.clone()).collect();
                let replicas: Vec<Uuid> = released.iter().map(|(e, _)| *e).collect();
                let held = delta.fresh(self.held(conn, p, Some(&keys), Some(&replicas)).await?);
                let wanted = self.wanted(conn, p, Some(&keys), Some(&replicas)).await?;
                // A row on its way again only because it changed is still
                // examined: sending it does not keep it.
                for candidate in released {
                    if held.contains(&candidate) && !wanted.contains(&candidate) {
                        examined.insert(candidate);
                    }
                }
            }
            if examined.is_empty() {
                continue;
            }
            let kept = self.pointed_at(conn, delta, p, &examined).await?;
            for (replica, key) in &examined {
                if kept.contains(&(*replica, key.clone())) {
                    if let Some(keys) = delta.deletes.get_mut(replica).and_then(|t| t.get_mut(&p)) {
                        keys.remove(key);
                    }
                } else {
                    delta.unsend(*replica, p, key);
                    delta.delete(*replica, p, key.clone());
                }
            }
            // A delete that stands releases what the row points at.
            let standing: Vec<(Uuid, String)> = examined.difference(&kept).cloned().collect();
            let keys: Vec<String> = standing
                .iter()
                .map(|(_, k)| k.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            for reference in &self.plans[p].references {
                let mut parents: BTreeMap<String, String> = self
                    .parents_of(conn, p, reference, &keys, false)
                    .await?
                    .into_iter()
                    .collect();
                for key in &keys {
                    if parents.contains_key(key) {
                        continue;
                    }
                    if let Some(parent) = released
                        .previous
                        .get(&(p, key.clone()))
                        .and_then(|versions| versions.last())
                        .and_then(|row| Self::parent_in(reference, row))
                    {
                        parents.insert(key.clone(), parent);
                    }
                }
                for (replica, key) in &standing {
                    if let Some(parent) = parents.get(key) {
                        candidates
                            .entry(reference.parent)
                            .or_default()
                            .insert((*replica, parent.clone()));
                    }
                }
            }
        }
        for tables in delta.deletes.values_mut() {
            tables.retain(|_, keys| !keys.is_empty());
        }
        delta.deletes.retain(|_, tables| !tables.is_empty());
        Ok(())
    }

    /// Which of `candidates` (rows of `p`, per replica) a row the replica
    /// keeps — held and not being taken back, or being sent — points at.
    async fn pointed_at(
        &self,
        conn: &mut Conn,
        delta: &Delta,
        p: usize,
        candidates: &BTreeSet<(Uuid, String)>,
    ) -> Result<BTreeSet<(Uuid, String)>, Error> {
        let keys: Vec<String> = candidates
            .iter()
            .map(|(_, k)| k.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let replicas: Vec<Uuid> = candidates
            .iter()
            .map(|(e, _)| *e)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut kept = BTreeSet::new();
        for (c, plan) in self.plans.iter().enumerate() {
            // A follower goes where its parent goes: it never keeps it.
            let follows = match plan.sync {
                Sync::Follows(column) => Some(column),
                _ => None,
            };
            for reference in plan
                .references
                .iter()
                .filter(|r| r.parent == p && follows.is_none_or(|f| r.columns != [f.to_owned()]))
            {
                let parent_key = key_sql("t", &reference.columns);
                let pointing: Vec<(Uuid, String, String)> = sqlx::query_as(&format!(
                    "SELECT h.replica_id, h.row_key, {parent_key} FROM sync.held h \
                     JOIN {} t ON {} \
                     WHERE h.relation = $1 AND h.replica_id = ANY($2) AND {}",
                    qualified(plan),
                    key_join("t", "h.row_key", &plan.key),
                    key_in("t", &reference.columns, &self.plans[p].key, "$3"),
                ))
                .bind(plan.table)
                .bind(&replicas)
                .bind(&keys)
                .fetch_all(&mut *conn)
                .await?;
                for (replica, child, parent) in pointing {
                    if !delta.has_delete(replica, c, &child) {
                        kept.insert((replica, parent));
                    }
                }
                let sent: Vec<(Uuid, Vec<String>)> = delta
                    .sent(c)
                    .into_iter()
                    .filter(|(e, _)| replicas.contains(e))
                    .collect();
                let child_keys: Vec<String> = sent
                    .iter()
                    .flat_map(|(_, keys)| keys.iter().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                if child_keys.is_empty() {
                    continue;
                }
                let parent_of: BTreeMap<String, String> = self
                    .parents_of(conn, c, reference, &child_keys, false)
                    .await?
                    .into_iter()
                    .collect();
                for (replica, keys) in &sent {
                    for parent in keys.iter().filter_map(|k| parent_of.get(k)) {
                        kept.insert((*replica, parent.clone()));
                    }
                }
            }
        }
        Ok(kept.intersection(candidates).cloned().collect())
    }

    /// Followers go wherever their parent goes, parents first: a parent newly
    /// sent to an replica brings its followers, one taken back takes them
    /// back, and a follower that changed goes to whoever will hold its parent.
    async fn follow_parents(
        &self,
        conn: &mut Conn,
        delta: &mut Delta,
        touched: &BTreeMap<usize, BTreeMap<String, i64>>,
    ) -> Result<(), Error> {
        for &c in &self.order {
            let Sync::Follows(column) = self.plans[c].sync else {
                continue;
            };
            let reference = self.plans[c]
                .references
                .iter()
                .find(|r| r.columns == [column.to_owned()])
                .expect("checked in new()")
                .clone();
            let parent = reference.parent;
            let child_key = key_sql(
                "t",
                &self.plans[c].key.iter().map(|k| k.name).collect::<Vec<_>>(),
            );

            // Followers that changed: to whoever will hold their parent.
            if let Some(notes) = touched.get(&c) {
                let keys: Vec<String> = notes.keys().cloned().collect();
                let keys = &keys;
                // Read from the active rows: a retired follower goes to no
                // one, and is taken back from whoever holds it.
                let parents: BTreeMap<String, String> = self
                    .parents_of(conn, c, &reference, keys, true)
                    .await?
                    .into_iter()
                    .collect();
                let parent_keys: Vec<String> = parents
                    .values()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let parent_held =
                    delta.fresh(self.held(conn, parent, Some(&parent_keys), None).await?);
                let horizons = self.horizons(conn, c, keys).await?;
                let holders = delta.fresh(horizons.keys().cloned().collect());
                let mut targets: BTreeSet<(Uuid, String)> = BTreeSet::new();
                for (key, pk) in &parents {
                    for (replica, _) in parent_held.iter().filter(|(_, k)| k == pk) {
                        if !delta.has_delete(*replica, parent, pk) {
                            targets.insert((*replica, key.clone()));
                        }
                    }
                    for (replica, tables) in &delta.upserts {
                        if tables.get(&parent).is_some_and(|k| k.contains(pk)) {
                            targets.insert((*replica, key.clone()));
                        }
                    }
                }
                for target in &targets {
                    if delta.snapshots.contains(&target.0)
                        || Self::owed(&horizons, Some(notes), target)
                    {
                        delta.upsert(target.0, c, target.1.clone());
                    }
                }
                for (replica, key) in holders.difference(&targets) {
                    delta.delete(*replica, c, key.clone());
                }
            }

            let column_in = |param: &str| key_in("t", &[column], &self.plans[parent].key, param);

            // Parents newly sent bring their followers.
            let sent = delta.sent(parent);
            let parent_keys: Vec<String> = sent
                .iter()
                .flat_map(|(_, keys)| keys.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if !parent_keys.is_empty() {
                let replicas: Vec<Uuid> = sent
                    .iter()
                    .map(|(e, _)| *e)
                    .filter(|e| !delta.snapshots.contains(e))
                    .collect();
                let held = if replicas.is_empty() {
                    BTreeSet::new()
                } else {
                    self.held(conn, parent, Some(&parent_keys), Some(&replicas))
                        .await?
                };
                let followers: Vec<(String, String)> = sqlx::query_as(&format!(
                    "SELECT {child_key}, t.{column}::text FROM {} t WHERE {}",
                    self.plans[c].active,
                    column_in("$1"),
                ))
                .bind(&parent_keys)
                .fetch_all(&mut *conn)
                .await?;
                let mut of_parent: BTreeMap<String, Vec<String>> = BTreeMap::new();
                for (follower, parent_key) in followers {
                    of_parent.entry(parent_key).or_default().push(follower);
                }
                for (replica, keys) in &sent {
                    for key in keys {
                        if held.contains(&(*replica, key.clone())) {
                            continue;
                        }
                        for follower in of_parent.get(key).into_iter().flatten() {
                            delta.upsert(*replica, c, follower.clone());
                        }
                    }
                }
            }

            // Parents taken back take their followers first.
            let taken = delta.taken(parent);
            let parent_keys: Vec<String> = taken
                .iter()
                .flat_map(|(_, keys)| keys.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if !parent_keys.is_empty() {
                let replicas: Vec<Uuid> = taken.iter().map(|(e, _)| *e).collect();
                let held: Vec<(Uuid, String, String)> = sqlx::query_as(&format!(
                    "SELECT h.replica_id, h.row_key, t.{column}::text FROM sync.held h JOIN {} t ON {} \
                     WHERE h.relation = $1 AND h.replica_id = ANY($2) AND {}",
                    qualified(&self.plans[c]),
                    key_join("t", "h.row_key", &self.plans[c].key),
                    column_in("$3"),
                ))
                .bind(self.plans[c].table)
                .bind(&replicas)
                .bind(&parent_keys)
                .fetch_all(&mut *conn)
                .await?;
                for (replica, key, parent_key) in held {
                    let taken_here = delta.has_delete(replica, parent, &parent_key);
                    if taken_here && !delta.has_upsert(replica, c, &key) {
                        delta.delete(replica, c, key);
                    }
                }
            }
        }
        Ok(())
    }

    /// Loads and encodes every row the delta sends, once however many
    /// replicas it goes to. A row gone since the plan read it is taken back
    /// instead of sent — and so never recorded as held. A row that does not
    /// encode is left out of the delta and answered in [`Loaded::failed`]: a
    /// replica keeps whatever version of it it has, rather than lose it to a
    /// bug in the encoding. So is one larger than `largest_row` bytes, which no
    /// message could carry.
    pub async fn load<P: std::marker::Sync + 'static>(
        &self,
        conn: &mut Conn,
        codecs: &[Codec<P>],
        delta: &mut Delta,
        largest_row: usize,
    ) -> Result<Loaded, Error> {
        let mut wanted: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
        for tables in delta.upserts.values() {
            for (i, keys) in tables {
                wanted.entry(*i).or_default().extend(keys.iter().cloned());
            }
        }
        let mut loaded = Loaded::default();
        let mut failed: BTreeMap<(usize, String), String> = BTreeMap::new();
        for (i, keys) in wanted {
            let plan = &self.plans[i];
            let codec = codecs
                .iter()
                .find(|c| c.table.name == plan.table)
                .ok_or_else(|| {
                    Error::Misuse(format!("no codec for mirrored table `{}`", plan.table))
                })?;
            let parsed: Vec<Key> = keys
                .iter()
                .map(|k| parse_key(&plan.key, k))
                .collect::<Result<_, _>>()?;
            let encoded = codec.load(conn, &parsed).await?;
            for (key, bytes) in encoded.rows {
                if bytes.len() > largest_row {
                    let reason = format!(
                        "a row of {} bytes, larger than any message may be ({largest_row})",
                        bytes.len()
                    );
                    failed.insert((i, crate::row::key_text(&key)), reason);
                    continue;
                }
                loaded.rows.insert((i, crate::row::key_text(&key)), bytes);
            }
            for (key, e) in encoded.failed {
                failed.insert((i, crate::row::key_text(&key)), e.to_string());
            }
        }
        for (replica, tables) in delta.upserts.iter_mut() {
            for (i, keys) in tables.iter_mut() {
                for key in keys.iter() {
                    if let Some(reason) = failed.get(&(*i, key.clone())) {
                        loaded.failed.push(Unsent {
                            replica: *replica,
                            table: self.plans[*i].table,
                            key: key.clone(),
                            reason: reason.clone(),
                        });
                    }
                }
                keys.retain(|k| !failed.contains_key(&(*i, k.clone())));
                let gone: Vec<String> = keys
                    .iter()
                    .filter(|k| !loaded.rows.contains_key(&(*i, (*k).clone())))
                    .cloned()
                    .collect();
                for key in gone {
                    keys.remove(&key);
                    delta
                        .deletes
                        .entry(*replica)
                        .or_default()
                        .entry(*i)
                        .or_default()
                        .insert(key);
                }
            }
        }
        Ok(loaded)
    }

    /// One replica's rows from what [`Engine::load`] read: its upserts
    /// parents first, then its deletes children first.
    pub fn rows(&self, delta: &Delta, loaded: &Loaded, replica: Uuid) -> Vec<envelope::Table> {
        let mut out = Vec::new();
        if let Some(tables) = delta.upserts.get(&replica) {
            for &i in &self.order {
                let Some(keys) = tables.get(&i) else { continue };
                let sent: Vec<(&String, &Vec<u8>)> = keys
                    .iter()
                    .filter_map(|k| loaded.rows.get(&(i, k.clone())).map(|bytes| (k, bytes)))
                    .collect();
                out.push(envelope::Table {
                    table: self.plans[i].table.to_owned(),
                    upserts: sent.iter().map(|(_, bytes)| (*bytes).clone()).collect(),
                    deletes: Vec::new(),
                    keys: sent
                        .iter()
                        .map(|(k, _)| envelope::Key {
                            parts: k.split('|').map(str::to_owned).collect(),
                        })
                        .collect(),
                });
            }
        }
        if let Some(tables) = delta.deletes.get(&replica) {
            for &i in self.order.iter().rev() {
                let Some(keys) = tables.get(&i) else { continue };
                out.push(envelope::Table {
                    table: self.plans[i].table.to_owned(),
                    upserts: Vec::new(),
                    keys: Vec::new(),
                    deletes: keys
                        .iter()
                        .map(|k| envelope::Key {
                            parts: k.split('|').map(str::to_owned).collect(),
                        })
                        .collect(),
                });
            }
        }
        out
    }

    /// Records what the replicas hold once their messages are out: what was
    /// sent, less what was taken back. A snapshot replaces everything its
    /// replica held.
    pub async fn record(&self, conn: &mut Conn, delta: &Delta) -> Result<(), Error> {
        let snapshots: Vec<Uuid> = delta.snapshots.iter().copied().collect();
        if !snapshots.is_empty() {
            sqlx::query("DELETE FROM sync.held WHERE replica_id = ANY($1)")
                .bind(&snapshots)
                .execute(&mut *conn)
                .await?;
        }
        let replicas: Vec<Uuid> = delta.upserts.keys().copied().collect();
        let partitions: HashMap<Uuid, Option<Uuid>> =
            sqlx::query_as::<_, (Uuid, Option<Uuid>)>(&format!(
                "SELECT id, {} FROM {} WHERE id = ANY($1)",
                self.replicas.partition.unwrap_or("NULL::uuid"),
                self.replicas.relation
            ))
            .bind(&replicas)
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .collect();

        let (mut e, mut t, mut k, mut n) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (replica, tables) in &delta.upserts {
            let partition = *partitions
                .get(replica)
                .ok_or_else(|| Error::Misuse(format!("{replica} is not a replica")))?;
            for (i, keys) in tables {
                for key in keys {
                    e.push(*replica);
                    t.push(self.plans[*i].table);
                    k.push(key.as_str());
                    n.push(partition);
                }
            }
        }
        for start in (0..e.len()).step_by(RECORD_CHUNK) {
            let range = start..(start + RECORD_CHUNK).min(e.len());
            sqlx::query(
                "INSERT INTO sync.held (replica_id, relation, row_key, partition_id, horizon) \
                 SELECT e, t, k, n, $5::text::xid8 \
                   FROM unnest($1::uuid[], $2::text[], $3::text[], $4::uuid[]) AS u(e, t, k, n) \
                 ON CONFLICT (replica_id, relation, row_key) \
                 DO UPDATE SET held_since = now(), horizon = EXCLUDED.horizon",
            )
            .bind(&e[range.clone()])
            .bind(&t[range.clone()])
            .bind(&k[range.clone()])
            .bind(&n[range])
            .bind(delta.horizon.to_string())
            .execute(&mut *conn)
            .await?;
        }

        let (mut e, mut t, mut k) = (Vec::new(), Vec::new(), Vec::new());
        for (replica, tables) in &delta.deletes {
            for (i, keys) in tables {
                for key in keys {
                    e.push(*replica);
                    t.push(self.plans[*i].table);
                    k.push(key.as_str());
                }
            }
        }
        for start in (0..e.len()).step_by(RECORD_CHUNK) {
            let range = start..(start + RECORD_CHUNK).min(e.len());
            sqlx::query(
                "DELETE FROM sync.held h \
                  USING unnest($1::uuid[], $2::text[], $3::text[]) AS d(e, t, k) \
                  WHERE h.replica_id = d.e AND h.relation = d.t AND h.row_key = d.k",
            )
            .bind(&e[range.clone()])
            .bind(&t[range.clone()])
            .bind(&k[range])
            .execute(&mut *conn)
            .await?;
        }
        Ok(())
    }

    /// The tables every snapshot names, parents first.
    pub fn snapshot_tables(&self) -> Vec<&'static str> {
        self.order.iter().map(|&i| self.plans[i].table).collect()
    }

    /// Every replica the delta touches.
    pub fn replicas(delta: &Delta) -> BTreeSet<Uuid> {
        delta
            .upserts
            .keys()
            .chain(delta.deletes.keys())
            .chain(delta.snapshots.iter())
            .copied()
            .collect()
    }
}

/// A note as the engine reads it.
#[derive(sqlx::FromRow)]
struct Noted {
    id: i64,
    kind: String,
    relation: Option<String>,
    row_key: Option<String>,
    replica_id: Option<Uuid>,
    previous: Option<serde_json::Value>,
    scope: Option<Uuid>,
    xid: i64,
}

impl Noted {
    /// The change a note records; `None` for one this build cannot read.
    fn change(self) -> Option<Change> {
        Some(match self.kind.as_str() {
            "resync" => Change::Resync {
                replica: self.replica_id?,
            },
            "row" => Change::Row {
                table: self.relation?,
                key: self.row_key?,
                previous: self.previous,
                xid: self.xid,
            },
            "members" => Change::Members {
                table: self.relation?,
                member: self.previous?,
            },
            "assigned" => Change::Assigned {
                table: self.relation?,
                replica: self.replica_id?,
            },
            _ => return None,
        })
    }
}

/// The pending notes, oldest first: not parked, and not owed to a replica
/// that is waiting out a failure. A note this build cannot read is left out,
/// and stays pending where [`Engine::health`] counts it.
pub async fn pending(conn: &mut Conn, limit: i64) -> Result<Vec<Note>, Error> {
    let rows: Vec<Noted> = sqlx::query_as(
        "SELECT id, kind, relation, row_key, replica_id, previous, scope, xid::text::bigint AS xid \
           FROM sync.notes n \
          WHERE done_at IS NULL AND parked_at IS NULL \
            AND NOT EXISTS (SELECT 1 FROM sync.replicas r \
                             WHERE r.replica_id = n.scope AND r.retry_after > now()) \
          ORDER BY id LIMIT $1",
    )
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|noted| {
            let (id, scope) = (noted.id, noted.scope);
            noted.change().map(|change| Note { id, change, scope })
        })
        .collect())
}

/// Queues `replica`'s provisioning from scratch, in the caller's transaction:
/// it is sent everything it should hold and sweeps the rest.
pub async fn note_resync(
    conn: &mut Conn,
    partition: Option<Uuid>,
    replica: Uuid,
) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO sync.notes (kind, replica_id, partition_id) VALUES ('resync', $1, $2)",
    )
    .bind(replica)
    .bind(partition)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Forgets what `replica` was known to hold, when what held it is gone.
pub async fn forget_held(conn: &mut Conn, replica: Uuid) -> Result<(), Error> {
    sqlx::query("DELETE FROM sync.held WHERE replica_id = $1")
        .bind(replica)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Marks notes the engine has sent and recorded.
pub async fn mark_done(conn: &mut Conn, ids: &[i64]) -> Result<(), Error> {
    sqlx::query("UPDATE sync.notes SET done_at = now() WHERE id = ANY($1)")
        .bind(ids)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Deletes the notes done longer ago than `age`; answers how many.
pub async fn prune(conn: &mut Conn, age: std::time::Duration) -> Result<u64, Error> {
    let done =
        sqlx::query("DELETE FROM sync.notes WHERE done_at < now() - make_interval(secs => $1)")
            .bind(age.as_secs_f64())
            .execute(&mut *conn)
            .await?;
    Ok(done.rows_affected())
}

/// Pending changes per table of one partition: `(table, count, oldest age)`.
pub async fn pending_by_table(
    conn: &mut Conn,
    partition: Uuid,
) -> Result<Vec<(String, i64, Option<std::time::Duration>)>, Error> {
    let rows: Vec<(String, i64, Option<f64>)> = sqlx::query_as(
        "SELECT coalesce(relation, kind), count(*), extract(epoch FROM now() - min(noted_at))::float8 \
           FROM sync.notes WHERE done_at IS NULL AND partition_id = $1 \
          GROUP BY 1 ORDER BY 1",
    )
    .bind(partition)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(table, count, age)| {
            (
                table,
                count,
                age.map(|secs| std::time::Duration::from_secs_f64(secs.max(0.0))),
            )
        })
        .collect())
}

/// What a batch of changes may have released: each changed row as it was.
#[derive(Default)]
struct Released {
    previous: BTreeMap<(usize, String), Vec<serde_json::Value>>,
}

/// `alias.k1 = split_part(text, '|', 1)::type AND …`: a key compared to its
/// text without casting the column, so the column's index still serves.
fn key_join(alias: &str, text: &str, key: &[Column]) -> String {
    key.iter()
        .enumerate()
        .map(|(n, c)| {
            format!(
                "{alias}.{} = split_part({text}, '|', {})::{}",
                c.name,
                n + 1,
                KeyPart::sql_type(c)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn qualified(plan: &Plan) -> String {
    format!("{}.{}", plan.schema, plan.table)
}

/// A grouped table's members and assigned tables, read off their keys: the
/// members' column pointing at the grouped table is the row, the other one the
/// group; the assigned table's column pointing at the replicas is the
/// replica, and it shares the group column.
fn group_of(
    foreign_keys: &[(String, String, Vec<String>, Vec<String>)],
    table: &str,
    members: &str,
    assigned: &str,
    replicas: &str,
    partition: Option<&str>,
) -> Result<Group, Error> {
    let not_partition = |c: &String| Some(c.as_str()) != partition;
    let bad = |why: &str| {
        Error::Misuse(format!(
            "`{table}` is grouped by `{members}` and `{assigned}`, but {why}"
        ))
    };
    let single = |child: &str, parent: &str| {
        foreign_keys
            .iter()
            .filter(|(c, p, _, _)| c == child && p == parent)
            .find_map(|(_, _, cols, _)| cols.iter().find(|c| not_partition(c)).cloned())
    };
    let member_row = single(members, table).ok_or_else(|| bad("no member column points at it"))?;
    let assigned_replica =
        single(assigned, replicas).ok_or_else(|| bad("no assigned column points at a replica"))?;
    let member_columns: BTreeSet<String> = foreign_keys
        .iter()
        .filter(|(c, _, _, _)| c == members)
        .flat_map(|(_, _, cols, _)| cols.clone())
        .filter(|c| not_partition(c) && *c != member_row)
        .collect();
    let group = foreign_keys
        .iter()
        .filter(|(c, _, _, _)| c == assigned)
        .flat_map(|(_, _, cols, _)| cols.clone())
        .find(|c| member_columns.contains(c))
        .ok_or_else(|| bad("the two do not share a group column"))?;
    Ok(Group {
        members: members.to_owned(),
        member_row,
        group,
        assigned: assigned.to_owned(),
        assigned_replica,
    })
}

/// Parents before children; a cycle between mirrored tables is refused.
fn topological(plans: &[Plan]) -> Result<Vec<usize>, Error> {
    let mut order = Vec::with_capacity(plans.len());
    let mut state = vec![0u8; plans.len()];
    fn visit(
        i: usize,
        plans: &[Plan],
        state: &mut [u8],
        order: &mut Vec<usize>,
    ) -> Result<(), Error> {
        match state[i] {
            2 => return Ok(()),
            1 => {
                return Err(Error::Misuse(format!(
                    "mirrored tables point at each other in a cycle through `{}`",
                    plans[i].table
                )))
            }
            _ => {}
        }
        state[i] = 1;
        for reference in &plans[i].references {
            visit(reference.parent, plans, state, order)?;
        }
        state[i] = 2;
        order.push(i);
        Ok(())
    }
    for i in 0..plans.len() {
        visit(i, plans, &mut state, &mut order)?;
    }
    Ok(order)
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod plan_tests;

#[cfg(test)]
#[path = "plan_bench.rs"]
mod plan_bench;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The engine's schema as it ships, whatever the cluster's template
    /// already holds: a host may have primed it with its own copy.
    pub(crate) async fn install(conn: &mut Conn) {
        sqlx::raw_sql("DROP SCHEMA IF EXISTS sync CASCADE")
            .execute(&mut *conn)
            .await
            .expect("drop the schema");
        sqlx::raw_sql(crate::POSTGRES_SCHEMA)
            .execute(&mut *conn)
            .await
            .expect("the schema");
    }

    #[test]
    fn a_change_is_sent_again_only_where_the_held_version_predates_it() {
        let replica = Uuid::now_v7();
        let held = |horizon: i64| BTreeMap::from([((replica, "k".to_owned()), horizon)]);
        let notes = BTreeMap::from([("k".to_owned(), 100_i64)]);
        let holder = (replica, "k".to_owned());

        assert!(Engine::owed(&BTreeMap::new(), None, &holder), "not held");
        assert!(
            Engine::owed(&held(90), Some(&notes), &holder),
            "sent before the change"
        );
        assert!(
            Engine::owed(&held(100), Some(&notes), &holder),
            "the change still running then"
        );
        assert!(
            !Engine::owed(&held(101), Some(&notes), &holder),
            "the send carried it"
        );
        assert!(
            !Engine::owed(&held(90), None, &holder),
            "only who holds it moved"
        );
    }
}
