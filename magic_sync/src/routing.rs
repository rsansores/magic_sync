//! What a routed table's view reads, read off the catalog.
//!
//! A table routed by a view (`routed(view = …)`) holds a row on the replicas
//! the view pairs it with, so every write that changes what the view answers
//! is a change of where rows go. The engine does not take a list of those
//! tables from the host — one forgotten is a replica that keeps rows it
//! should no longer hold, silently — but reads it where PostgreSQL keeps it:
//! a view's rule depends on each column it reads (`pg_depend`, column by
//! column), a view it reads is another rule to read, and a function with a
//! SQL-standard body (`BEGIN ATOMIC … END`, or `RETURN …`) depends on what
//! its body reads, the same way. What PostgreSQL ships is pinned and depends
//! on nothing of the host's; a function an extension installed is taken to
//! read no table of the host's. Neither is read through, so each has to be
//! immutable: one that is not — `now()`, `current_setting()` — answers
//! otherwise with no write a trigger sees. The catalog records no call of
//! what PostgreSQL ships, so those are read off the rule itself. Any other
//! function's body is text the catalog does not look into — PL/pgSQL, or SQL
//! written as a string — so a view that calls one is refused, naming it, as
//! is a view that calls a function that is not immutable, or reads a
//! materialized view, a foreign table, a partitioned table or a table of an
//! inheritance tree, whose rows change where no trigger sees; and one that
//! reads a column of the routed table its replicas write, whose writes note
//! no change of the row.

use super::{ident, readable, typed_columns, Relation};
use crate::{Conn, Error};
use std::collections::{BTreeMap, BTreeSet};

/// How a routed table reaches replicas: its view, and what the view reads.
#[derive(Clone, Debug)]
pub(super) struct Routing {
    /// The view, as SQL names it.
    pub(super) view: String,
    /// Its columns that hold the routed row's key, in key order.
    pub(super) row: Vec<String>,
    /// Its column naming the replica.
    pub(super) replica: String,
    /// Its column naming the partition: the replicas' partition column,
    /// when they have one.
    pub(super) partition: Option<String>,
    /// Every table the view reads — through the views and functions it
    /// reads — but the routed table itself, with the columns it reads of
    /// each: a write of one of those columns re-routes.
    pub(super) reads: BTreeMap<Relation, BTreeSet<String>>,
    /// The tables of `reads` that have the replicas' partition column, by
    /// which a write there names the partition it re-routes.
    pub(super) partitioned: BTreeSet<Relation>,
    /// The definitions of the view and of everything it reads through, by
    /// name: a deploy that changes one routes rows otherwise.
    pub(super) definitions: BTreeMap<String, String>,
}

/// An object whose dependencies say what a view reads: a view's rule, or a
/// function with a body the catalog looks into.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Reader {
    /// A view, by its relation's oid.
    View(i64),
    /// A function, by its oid.
    Function(i64),
}

/// A relation the catalog says an object reads: its oid, kind, schema and
/// name, and the column read — `None` for the whole of it, as a `count(*)`
/// reads it.
type ReadRelation = (i64, String, String, String, Option<String>);

/// A function the catalog says an object calls, directly or through an
/// operator: its oid, its name with its arguments, whether its body is
/// SQL-standard, whether PostgreSQL or an extension installed it, and the
/// operator it was called through.
type Called = (i64, String, bool, bool, Option<String>);

impl Routing {
    /// The routing of `table` — keyed by `key`, each column with its SQL
    /// type, its replicas writing the columns `owned` — by `view`, its
    /// columns `row` and `replica`, against replicas partitioned by
    /// `partition`: the view checked against the key and the replicas, and
    /// what it reads found. Refuses a view that does not read, lacks a
    /// column, holds the key or the replica in a type that is not theirs,
    /// reads what no trigger can see, or reads a column of `owned`.
    pub(super) async fn read(
        conn: &mut Conn,
        table: &Relation,
        key: &[(&str, &str)],
        owned: &[&str],
        (view, row, replica): (&str, &[&str], &str),
        partition: Option<&str>,
    ) -> Result<Self, Error> {
        let name = &table.name;
        let refuse = |why: String| Error::Misuse(format!("`{name}` is routed by {view}, {why}"));
        let columns = typed_columns(conn, view).await?;
        if columns.is_empty() {
            return Err(refuse(
                "which is no table or view of the database".to_owned(),
            ));
        }
        readable(
            conn,
            &format!("SELECT 1 FROM {view} LIMIT 0"),
            &format!("{view}, the view `{name}` is routed by,"),
        )
        .await?;
        let type_of = |column: &str| {
            columns
                .iter()
                .find(|(c, _)| c == column)
                .map(|(_, t)| t.as_str())
        };
        // A key column is a uuid or an integer, of any width.
        let family = |t: &str| match t {
            "smallint" | "integer" | "bigint" => "an integer",
            "uuid" => "a uuid",
            _ => "",
        };
        for (column, (key_column, key_type)) in row.iter().zip(key) {
            match type_of(column) {
                None => {
                    return Err(refuse(format!(
                        "which has no column `{column}` to hold its key column `{key_column}`"
                    )))
                }
                Some(t) if family(t) != family(key_type) => {
                    return Err(refuse(format!(
                        "whose column `{column}` is {t}, and the key column `{key_column}` it \
                         holds {key_type}"
                    )))
                }
                Some(_) => {}
            }
        }
        for (role, column) in [("the replica", Some(replica)), ("the partition", partition)] {
            let Some(column) = column else { continue };
            if type_of(column) != Some("uuid") {
                return Err(refuse(format!(
                    "which has no uuid column `{column}` to name {role} of each pair"
                )));
            }
        }
        // The view as the caller's path finds it, which may name it alone;
        // then read with nothing on the search path, so a definition names
        // everything it reads by its schema, whatever the connection's
        // path; and in a transaction of its own, rolled back, so the path
        // stays the caller's.
        let oid: i64 = sqlx::query_scalar("SELECT to_regclass($1)::oid::int8")
            .bind(view)
            .fetch_one(&mut *conn)
            .await?;
        let mut tx = sqlx::Connection::begin(&mut *conn).await?;
        sqlx::query("SET LOCAL search_path = pg_catalog")
            .execute(&mut *tx)
            .await?;
        let found = reach(
            &mut tx,
            oid,
            &[row, &[replica], partition.as_slice()].concat(),
        )
        .await;
        tx.rollback().await?;
        let (mut reads, definitions) = found?.map_err(refuse)?;
        // The table's own rows are planned by their key, off its row
        // trigger, which notes no write of the columns its replicas write
        // alone: a view that reads one would not move with it.
        if let Some(column) = reads
            .remove(table)
            .and_then(|read| read.into_iter().find(|c| owned.contains(&c.as_str())))
        {
            return Err(refuse(format!(
                "which reads `{column}` of `{name}` itself, a column its replicas write \
                 (`replica_owned`): a write of it notes no change of the row, and would route \
                 it nowhere again. Route by what the server writes"
            )));
        }
        let mut partitioned = BTreeSet::new();
        if let Some(partition) = partition {
            for relation in reads.keys() {
                let columns = typed_columns(conn, &relation.sql()).await?;
                match columns.iter().find(|(c, _)| c == partition) {
                    Some((_, t)) if t == "uuid" => {
                        partitioned.insert(relation.clone());
                    }
                    Some(_) => {
                        return Err(Error::Misuse(format!(
                            "{} has a column `{partition}` that is not a uuid: the replicas are \
                             partitioned by the uuid of that name",
                            relation.sql()
                        )))
                    }
                    None => {}
                }
            }
        }
        Ok(Routing {
            view: view.to_owned(),
            row: row.iter().map(|c| (*c).to_owned()).collect(),
            replica: replica.to_owned(),
            partition: partition.map(str::to_owned),
            reads,
            partitioned,
            definitions,
        })
    }

    /// The pairs the view answers, as a join: the row aliased `t` meets the
    /// view's pairs for it, and they meet the replicas aliased `s` they
    /// name, in their partition — the row's own too, when it has the
    /// partition column (`partitioned`), as a broadcast row's is: a view
    /// that pairs a row with a replica of another partition sends it
    /// nothing.
    pub(super) fn join(&self, key: &[&str], replicas: &str, partitioned: bool) -> String {
        let row: Vec<String> = self
            .row
            .iter()
            .zip(key)
            .map(|(v, k)| format!("v.{} = t.{}", ident(v), ident(k)))
            .collect();
        let partition = self.partition.as_ref().map_or(String::new(), |p| {
            let own = if partitioned {
                format!(" AND s.partition = t.{}", ident(p))
            } else {
                String::new()
            };
            format!(" AND s.partition = v.{}{own}", ident(p))
        });
        format!(
            "JOIN {} v ON {} JOIN {replicas} ON s.id = v.{}{partition}",
            self.view,
            row.join(" AND "),
            ident(&self.replica),
        )
    }

    /// What decides where the view sends rows, as one text: what it reads,
    /// and every definition it was read through.
    pub(super) fn declared(&self) -> String {
        let definitions: Vec<String> = self
            .definitions
            .iter()
            .map(|(name, body)| format!("{name}: {body}"))
            .collect();
        format!(" reads {:?} {}", self.reads, definitions.join(" "))
    }
}

/// What a view reads, table by table, with the columns it reads of each;
/// and the definitions it was read through, by name.
type Reach = (
    BTreeMap<Relation, BTreeSet<String>>,
    BTreeMap<String, String>,
);

/// A relation's kind, as [`unseen`] tells them apart, of the `pg_class`
/// aliased `c`: its `relkind`, but `inherited` for a table that inherits
/// or is inherited — a partition too.
const KIND: &str = "CASE WHEN c.relkind = 'r' AND EXISTS ( \
                             SELECT 1 FROM pg_inherits i WHERE c.oid IN (i.inhrelid, i.inhparent)) \
                         THEN 'inherited' ELSE c.relkind::text END";

/// Whether the function of the `pg_proc` aliased `p`, in the namespace
/// aliased `pn`, is PostgreSQL's or an extension's: one the engine does not
/// read through.
const SHIPPED: &str = "(pn.nspname = 'pg_catalog' OR EXISTS ( \
                           SELECT 1 FROM pg_depend e \
                            WHERE e.classid = 'pg_proc'::regclass AND e.objid = p.oid \
                              AND e.deptype = 'e'))";

/// What the relation of oid `view` reads — read in the caller's
/// transaction — and the definitions of `view` and of every view and
/// function it reads through ([`Reach`]). `used`: the columns the engine
/// reads of `view` itself, which are what it reads of a table named as the
/// view. The inner error says why the catalog cannot see what it reads; the
/// outer one is the database's.
async fn reach(conn: &mut Conn, view: i64, used: &[&str]) -> Result<Result<Reach, String>, Error> {
    let (oid, kind, schema, name): (i64, String, String, String) = sqlx::query_as(&format!(
        "SELECT c.oid::int8, {KIND}, n.nspname::text, c.relname::text \
           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
          WHERE c.oid = $1::int8::oid"
    ))
    .bind(view)
    .fetch_one(&mut *conn)
    .await?;
    let mut reads: BTreeMap<Relation, BTreeSet<String>> = BTreeMap::new();
    let mut definitions = BTreeMap::new();
    match kind.as_str() {
        "r" => {
            reads.insert(
                Relation { schema, name },
                used.iter().map(|c| (*c).to_owned()).collect(),
            );
            return Ok(Ok((reads, definitions)));
        }
        "v" => {}
        _ => return Ok(Err(format!("which is {}", unseen(&kind)))),
    }
    let mut seen = BTreeSet::new();
    let mut queue = vec![Reader::View(oid)];
    while let Some(reader) = queue.pop() {
        if !seen.insert(reader) {
            continue;
        }
        // The object whose dependencies to read, which of its own to leave
        // out — a view's rule depends on the view — its name, its
        // definition, and its parse tree, as text.
        let (class, object, own, (reader, body, tree)) = match reader {
            Reader::View(view) => {
                let (rule, reader, body, tree): (i64, String, String, String) = sqlx::query_as(
                    "SELECT r.oid::int8, format('the view %s', r.ev_class::regclass), \
                            pg_get_viewdef(r.ev_class), r.ev_action::text \
                       FROM pg_rewrite r WHERE r.ev_class = $1::int8::oid AND r.rulename = '_RETURN'",
                )
                .bind(view)
                .fetch_one(&mut *conn)
                .await?;
                ("pg_rewrite", rule, view, (reader, body, tree))
            }
            Reader::Function(function) => {
                let read: (String, String, String) = sqlx::query_as(
                    "SELECT format('the function %s', p.oid::regprocedure), pg_get_functiondef(p.oid), \
                            coalesce(p.prosqlbody::text, '') \
                       FROM pg_proc p WHERE p.oid = $1::int8::oid",
                )
                .bind(function)
                .fetch_one(&mut *conn)
                .await?;
                ("pg_proc", function, 0, read)
            }
        };
        definitions.insert(reader.clone(), body);
        // Whether it reads a whole row somewhere.
        let whole = tree.contains(":varattno 0 ");
        let relations: Vec<ReadRelation> = sqlx::query_as(&format!(
            "SELECT c.oid::int8, {KIND}, n.nspname::text, c.relname::text, \
                    CASE WHEN d.refobjsubid > 0 THEN a.attname::text END \
               FROM pg_depend d \
               JOIN pg_class c ON c.oid = d.refobjid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.refobjsubid \
              WHERE d.classid = $1::regclass AND d.objid = $2::int8::oid AND d.deptype = 'n' \
                AND d.refclassid = 'pg_class'::regclass AND d.refobjid <> $3::int8::oid"
        ))
        .bind(class)
        .bind(object)
        .bind(own)
        .fetch_all(&mut *conn)
        .await?;
        for (oid, kind, schema, table, column) in relations {
            let relation = Relation {
                schema,
                name: table,
            };
            match kind.as_str() {
                "r" => {
                    // A whole-row reference names no column of its own, and
                    // reads every one: the catalog keeps no trace of which
                    // table it is of, so this reader's tables are read whole.
                    let every = column.is_none() || whole;
                    let columns: Vec<String> = if every {
                        typed_columns(conn, &relation.sql())
                            .await?
                            .into_iter()
                            .map(|(c, _)| c)
                            .collect()
                    } else {
                        column.into_iter().collect()
                    };
                    reads.entry(relation).or_default().extend(columns);
                }
                "v" => queue.push(Reader::View(oid)),
                "m" | "f" | "p" | "inherited" => {
                    return Ok(Err(format!(
                        "whose {reader} reads {}, {}",
                        relation.sql(),
                        unseen(&kind)
                    )))
                }
                // A sequence, a composite type: no rows to route by.
                _ => {}
            }
        }
        if let Some(function) = unsteady(conn, &tree).await? {
            return Ok(Err(format!(
                "whose {reader} calls {function}, a function that is not immutable: what it \
                 answers changes with no write a trigger sees, and would route no row again. Keep \
                 what it answers in a column a write sets — a flag a job raises — and read that"
            )));
        }
        let called: Vec<Called> = sqlx::query_as(&format!(
            "SELECT p.oid::int8, p.oid::regprocedure::text, p.prosqlbody IS NOT NULL, {SHIPPED}, \
                    o.oid::regoperator::text \
               FROM pg_depend d \
               LEFT JOIN pg_operator o ON d.refclassid = 'pg_operator'::regclass AND o.oid = d.refobjid \
               JOIN pg_proc p ON p.oid = CASE WHEN d.refclassid = 'pg_proc'::regclass THEN d.refobjid \
                                              ELSE o.oprcode END \
               JOIN pg_namespace pn ON pn.oid = p.pronamespace \
              WHERE d.classid = $1::regclass AND d.objid = $2::int8::oid AND d.deptype = 'n' \
                AND d.refclassid IN ('pg_proc'::regclass, 'pg_operator'::regclass)"
        ))
        .bind(class)
        .bind(object)
        .fetch_all(&mut *conn)
        .await?;
        for (function, signature, standard, builtin, operator) in called {
            if builtin {
                continue;
            }
            if !standard {
                let through = operator.map_or(String::new(), |o| format!(", the operator {o},"));
                return Ok(Err(format!(
                    "whose {reader}{through} calls {signature}, a function whose body PostgreSQL \
                     does not look into: what it reads is not known, nor noted. Write its body \
                     SQL-standard (`BEGIN ATOMIC … END`, or `RETURN …`), whose reads the catalog \
                     keeps, or read its tables in the view"
                )));
            }
            queue.push(Reader::Function(function));
        }
    }
    Ok(Ok((reads, definitions)))
}

/// What `current_date`, `current_user` and their kin are written as, by
/// their number in a rule (`SQLValueFunctionOp`): nodes of their own, no
/// call of a function, and none immutable.
const SQL_VALUES: [&str; 15] = [
    "current_date",
    "current_time",
    "current_time",
    "current_timestamp",
    "current_timestamp",
    "localtime",
    "localtime",
    "localtimestamp",
    "localtimestamp",
    "current_role",
    "current_user",
    "user",
    "session_user",
    "current_catalog",
    "current_schema",
];

/// The first function of PostgreSQL's or an extension's that `tree` — a
/// rule's or a SQL-standard body's parse tree, as text — calls, directly,
/// through an operator, as an aggregate or a window, and that is not
/// immutable (`provolatile`), by name. The catalog records no dependency on
/// what PostgreSQL ships, so the calls are read off the tree; the host's own
/// functions are read through, or refused, instead.
async fn unsteady(conn: &mut Conn, tree: &str) -> Result<Option<String>, Error> {
    let value = tree
        .split("{SQLVALUEFUNCTION :op ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next()?.parse::<usize>().ok());
    if let Some(op) = value {
        let name = SQL_VALUES
            .get(op)
            .copied()
            .unwrap_or("a SQL value function");
        return Ok(Some(name.to_owned()));
    }
    Ok(sqlx::query_scalar(&format!(
        "SELECT p.oid::regprocedure::text \
           FROM regexp_matches($1, ':(?:funcid|opfuncid|aggfnoid|winfnoid) (\\d+)', 'g') m \
           JOIN pg_proc p ON p.oid = m[1]::oid \
           JOIN pg_namespace pn ON pn.oid = p.pronamespace \
          WHERE p.provolatile <> 'i' AND {SHIPPED} \
          ORDER BY 1 LIMIT 1"
    ))
    .bind(tree)
    .fetch_optional(&mut *conn)
    .await?)
}

/// Why a relation of kind `kind` (`pg_class.relkind`, or `inherited`)
/// cannot be routed through.
fn unseen(kind: &str) -> &'static str {
    match kind {
        "m" => {
            "a materialized view, whose rows change on a refresh no trigger sees: a change of it \
             would route no row again"
        }
        "f" => {
            "a foreign table, whose rows change where no trigger sees: a change of it would route \
             no row again"
        }
        "p" => {
            "a partitioned table, whose rows a statement on one of its partitions writes where \
             no trigger of its own sees: a change of it would route no row again"
        }
        "inherited" => {
            "a table of an inheritance tree — a partition, a parent or a child — whose rows a \
             statement on another table of the tree writes where no trigger of its own sees: a \
             change of it would route no row again"
        }
        _ => "no table or view",
    }
}
