//! `#[derive(Row)]` and `#[derive(Columns)]` for [magic_sync]: one struct is
//! one table, column for column, and says how it travels.
//!
//! ```ignore
//! #[derive(sqlx::FromRow, Clone, magic_sync::Row)]
//! #[row(table = "products", schema = "shop", mirror = "catalog", sync = "broadcast")]
//! pub struct Product {
//!     pub id: Uuid,
//!     pub company_id: Uuid,
//!     pub name: String,
//!     #[row(version)]
//!     pub updated_at: DateTime<Utc>,
//! }
//! ```
//!
//! On the struct:
//!
//! | key | means |
//! |---|---|
//! | `table` | the table's name (required) |
//! | `schema` | its PostgreSQL schema (`public` when left out) |
//! | `mirror` | `catalog`, `record`, `both` or `server` (required) |
//! | `sync` | `broadcast`, `assigned(col)`, `follows(col)` or `grouped(members = t, assigned = t)`; left out, a row travels only when a row that travels points at it |
//! | `key` | the primary key's columns, when it is not `id` |
//! | `active` | the relation the rows replicas hold are read through (a view leaving rows out) |
//! | `retired` | the column a replica sets on a row it lets go of but must keep |
//! | `origin` | the key column naming the replica a record belongs to |
//! | `unmirrored` | columns one side's table has that the row leaves out |
//! | `crate` | the path to `magic_sync`, when it is re-exported |
//!
//! On a field: `#[row(version)]` (a column whose value only moves forward),
//! `#[row(created)]` (written once), `#[row(replica_owned)]` (in a `both`
//! table, a column only replicas write), `#[row(generated)]` (computed by the
//! database: read, never written).
//!
//! Fields follow `sqlx::FromRow`: `#[sqlx(rename = "…")]` names the column,
//! `#[sqlx(try_from = "…")]` is the type the column decodes into,
//! `#[sqlx(skip)]` is no column, and `#[sqlx(flatten)]` contributes the nested
//! struct's columns, which that struct declares with `#[derive(Columns)]`.
//! `#[sqlx(json)]` and a struct-level `#[sqlx(rename_all)]` are refused rather
//! than half-understood.
//!
//! [magic_sync]: https://docs.rs/magic_sync

use proc_macro::TokenStream as Tokens;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Field, Fields, LitStr, Type};

/// One struct is one table. See the crate's documentation.
#[proc_macro_derive(Row, attributes(row))]
pub fn derive_row(input: Tokens) -> Tokens {
    let input = parse_macro_input!(input as DeriveInput);
    row(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// A part of a row that a row flattens in (`#[sqlx(flatten)]`).
#[proc_macro_derive(Columns, attributes(row))]
pub fn derive_columns(input: Tokens) -> Tokens {
    let input = parse_macro_input!(input as DeriveInput);
    columns_only(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Default)]
struct Args {
    table: Option<String>,
    key: Option<String>,
    schema: Option<String>,
    mirror: Option<String>,
    sync: Option<LitStr>,
    active: Option<String>,
    retired: Option<String>,
    origin: Option<String>,
    unmirrored: Vec<String>,
    krate: Option<syn::Path>,
}

fn parse_args(input: &DeriveInput) -> syn::Result<Args> {
    let mut args = Args::default();
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("row")) {
        attr.parse_nested_meta(|meta| {
            let value: LitStr = meta.value()?.parse()?;
            let text = || Some(value.value());
            if meta.path.is_ident("table") {
                args.table = text();
            } else if meta.path.is_ident("key") {
                args.key = text();
            } else if meta.path.is_ident("schema") {
                args.schema = text();
            } else if meta.path.is_ident("mirror") {
                args.mirror = text();
            } else if meta.path.is_ident("sync") {
                args.sync = Some(value.clone());
            } else if meta.path.is_ident("active") {
                args.active = text();
            } else if meta.path.is_ident("retired") {
                args.retired = text();
            } else if meta.path.is_ident("origin") {
                args.origin = text();
            } else if meta.path.is_ident("unmirrored") {
                args.unmirrored = value
                    .value()
                    .split(',')
                    .map(|c| c.trim().to_owned())
                    .filter(|c| !c.is_empty())
                    .collect();
            } else if meta.path.is_ident("crate") {
                args.krate = Some(value.parse()?);
            } else {
                return Err(meta.error(
                    "expected `table`, `schema`, `mirror`, `sync`, `key`, `active`, `retired`, \
                     `origin`, `unmirrored` or `crate`",
                ));
            }
            Ok(())
        })?;
    }
    Ok(args)
}

fn krate(args: &Args) -> syn::Path {
    args.krate
        .clone()
        .unwrap_or_else(|| syn::parse_quote!(::magic_sync))
}

/// Read off the type as written: an alias to `Option` reads as NOT NULL.
fn is_option(ty: &Type) -> bool {
    matches!(ty, Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == "Option"))
}

/// The `#[sqlx(…)]` keys on a field.
#[derive(Default)]
struct SqlxField {
    skip: bool,
    flatten: bool,
    rename: Option<String>,
    try_from: Option<Type>,
}

fn sqlx_field(field: &Field) -> syn::Result<SqlxField> {
    let mut out = SqlxField::default();
    for attr in field.attrs.iter().filter(|a| a.path().is_ident("sqlx")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                out.skip = true;
            } else if meta.path.is_ident("flatten") {
                out.flatten = true;
            } else if meta.path.is_ident("rename") {
                out.rename = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("try_from") {
                out.try_from = Some(meta.value()?.parse::<LitStr>()?.parse()?);
            } else if meta.path.is_ident("json") {
                return Err(meta.error("#[derive(Row)] does not support #[sqlx(json)] yet"));
            } else if meta.input.peek(syn::Token![=]) {
                let _: syn::Expr = meta.value()?.parse()?;
            } else if meta.input.peek(syn::token::Paren) {
                meta.parse_nested_meta(|_| Ok(()))?;
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// The `#[row(…)]` keys on a field.
#[derive(Default)]
struct RowField {
    generated: bool,
    replica_owned: bool,
    version: bool,
    created: bool,
}

fn row_field(field: &Field) -> syn::Result<RowField> {
    let mut out = RowField::default();
    for attr in field.attrs.iter().filter(|a| a.path().is_ident("row")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("generated") {
                out.generated = true;
            } else if meta.path.is_ident("replica_owned") {
                out.replica_owned = true;
            } else if meta.path.is_ident("version") {
                out.version = true;
            } else if meta.path.is_ident("created") {
                out.created = true;
            } else {
                return Err(meta.error(
                    "expected `version`, `created`, `replica_owned` or `generated`",
                ));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// The type inside an `Option`, or the type itself.
fn unwrapped(ty: &Type) -> &Type {
    if let Type::Path(p) = ty {
        if let Some(last) = p.path.segments.last() {
            if last.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return inner;
                    }
                }
            }
        }
    }
    ty
}

/// How a column travels on the wire: a proto scalar for the numbers and
/// booleans, text for everything else (ids, instants, decimals, enums).
fn wire(ty: &Type) -> TokenStream {
    let name = match unwrapped(ty) {
        Type::Path(p) => p
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default(),
        _ => String::new(),
    };
    match name.as_str() {
        "i16" => quote!(Int16),
        "i32" => quote!(Int32),
        "i64" => quote!(Int64),
        "bool" => quote!(Bool),
        "f32" => quote!(Float),
        "f64" => quote!(Double),
        "Value" => quote!(Json),
        _ => quote!(String),
    }
}

fn refuse_rename_all(input: &DeriveInput) -> syn::Result<()> {
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("sqlx")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                return Err(meta.error(
                    "#[derive(Row)] reads column names off the fields: rename them one by one",
                ));
            }
            if meta.input.peek(syn::Token![=]) {
                let _: syn::Expr = meta.value()?.parse()?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

/// A column a struct declares directly.
struct Direct {
    name: String,
    ident: syn::Ident,
    nullable: bool,
    flags: RowField,
}

fn columns_impl(input: &DeriveInput, krate: &syn::Path) -> syn::Result<(TokenStream, Vec<Direct>)> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(&input.ident, "needs a struct"));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(&input.ident, "needs named fields"));
    };

    refuse_rename_all(input)?;
    let mut push = Vec::new();
    let mut bind = Vec::new();
    let mut direct = Vec::new();
    let mut flattened: Vec<Type> = Vec::new();
    for field in &fields.named {
        let sqlx = sqlx_field(field)?;
        let flags = row_field(field)?;
        let marked = flags.generated || flags.replica_owned || flags.version || flags.created;
        if marked && (sqlx.skip || sqlx.flatten) {
            return Err(syn::Error::new_spanned(
                field,
                "a #[row(…)] field names a column; a skipped or flattened field is none",
            ));
        }
        if sqlx.skip {
            continue;
        }
        let ty = &field.ty;
        let ident = field.ident.as_ref().expect("named field");
        if sqlx.flatten {
            flattened.push(ty.clone());
            push.push(quote!(<#ty as #krate::row::Columns>::columns(out);));
            bind.push(quote!(#krate::row::Columns::push_binds(self.#ident, sep);));
            continue;
        }
        if !flags.generated {
            bind.push(quote!(sep.push_bind(self.#ident);));
        }
        let name = sqlx
            .rename
            .unwrap_or_else(|| ident.to_string().trim_start_matches("r#").to_owned());
        let nullable = is_option(ty);
        if flags.version && nullable {
            return Err(syn::Error::new_spanned(
                field,
                "#[row(version)] orders a row's versions: it cannot be NULL",
            ));
        }
        let decoded = sqlx.try_from.as_ref().unwrap_or(ty);
        let wire = wire(ty);
        let field_name = ident.to_string().trim_start_matches("r#").to_owned();
        let generated = flags.generated;
        let replica_owned = flags.replica_owned;
        push.push(quote!(out.push(#krate::row::Column {
            name: #name,
            field: #field_name,
            nullable: #nullable,
            generated: #generated,
            replica_owned: #replica_owned,
            wire: #krate::row::Wire::#wire,
            compatible: |info| <#decoded as #krate::__sqlx::Type<#krate::Db>>::compatible(info),
        });));
        direct.push(Direct {
            name,
            ident: ident.clone(),
            nullable,
            flags,
        });
    }

    // A marked column here, or else the first flattened part's.
    let role = |pick: fn(&RowField) -> bool, which: TokenStream, what: &str| -> syn::Result<TokenStream> {
        let marked: Vec<&Direct> = direct.iter().filter(|d| pick(&d.flags)).collect();
        if let [_, second, ..] = marked.as_slice() {
            return Err(syn::Error::new_spanned(
                &second.ident,
                format!("one column at most is #[row({what})]"),
            ));
        }
        let mut value = match marked.first() {
            Some(column) => {
                let name = &column.name;
                quote!(Some(#name))
            }
            None => quote!(None),
        };
        for part in flattened.iter().rev() {
            value = quote!(match #value {
                Some(column) => Some(column),
                None => <#part as #krate::row::Columns>::#which,
            });
        }
        Ok(value)
    };
    let version = role(|f| f.version, quote!(VERSION_COLUMN), "version")?;
    let created = role(|f| f.created, quote!(CREATED_COLUMN), "created")?;

    let ident = &input.ident;
    Ok((
        quote! {
            impl #krate::row::Columns for #ident {
                const VERSION_COLUMN: Option<&'static str> = #version;
                const CREATED_COLUMN: Option<&'static str> = #created;

                fn columns(out: &mut Vec<#krate::row::Column>) {
                    #(#push)*
                }

                fn push_binds<'args>(
                    self,
                    sep: &mut #krate::__sqlx::query_builder::Separated<'_, 'args, #krate::Db, &'static str>,
                ) {
                    #(#bind)*
                }
            }
        },
        direct,
    ))
}

fn columns_only(input: DeriveInput) -> syn::Result<TokenStream> {
    let args = parse_args(&input)?;
    if args.table.is_some()
        || args.key.is_some()
        || args.schema.is_some()
        || args.mirror.is_some()
        || args.sync.is_some()
    {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Columns)] is a part a row flattens in: only `crate` applies to it",
        ));
    }
    Ok(columns_impl(&input, &krate(&args))?.0)
}

fn plain(s: &str) -> bool {
    s.chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn row(input: DeriveInput) -> syn::Result<TokenStream> {
    let args = parse_args(&input)?;
    let ident = &input.ident;
    let error = |what: String| syn::Error::new_spanned(ident, what);
    let table = args
        .table
        .clone()
        .ok_or_else(|| error("#[row] needs `table`".into()))?;
    let schema = args.schema.clone().unwrap_or_else(|| "public".to_owned());
    let mirror = args
        .mirror
        .clone()
        .ok_or_else(|| error("#[row] needs `mirror`".into()))?;
    if !plain(&table) || !plain(&schema) {
        return Err(error(
            "`table` and `schema` are plain snake_case names".into(),
        ));
    }
    let variant = match mirror.as_str() {
        "catalog" => quote!(Catalog),
        "record" => quote!(Record),
        "both" => quote!(Both),
        "server" => quote!(ServerOnly),
        other => {
            return Err(error(format!(
                "mirror = \"{other}\": expected \"catalog\", \"record\", \"both\" or \"server\""
            )))
        }
    };
    let krate = krate(&args);
    let sync = sync_strategy(args.sync.as_ref(), &mirror, &krate)?;
    let (columns, direct) = columns_impl(&input, &krate)?;

    if mirror != "both" {
        if let Some(owned) = direct.iter().find(|d| d.flags.replica_owned) {
            return Err(syn::Error::new_spanned(
                &owned.ident,
                "#[row(replica_owned)] splits a table written on both sides: needs `mirror = \"both\"`",
            ));
        }
    }
    let key: Vec<String> = args
        .key
        .as_deref()
        .unwrap_or("id")
        .split(',')
        .map(|k| k.trim().to_owned())
        .collect();
    let mut key_idents = Vec::new();
    for k in &key {
        let Some(column) = direct.iter().find(|d| &d.name == k) else {
            return Err(error(format!("key column `{k}` is not a field of the row")));
        };
        if column.nullable {
            return Err(syn::Error::new_spanned(
                &column.ident,
                "a key column cannot be NULL",
            ));
        }
        key_idents.push(column.ident.clone());
    }
    if let Some(origin) = &args.origin {
        if !key.contains(origin) {
            return Err(error(format!(
                "origin = \"{origin}\": the replica a record belongs to is a key column"
            )));
        }
    }

    let option = |value: &Option<String>| match value {
        Some(v) => quote!(Some(#v)),
        None => quote!(None),
    };
    let active = option(&args.active);
    let retired = option(&args.retired);
    let origin = option(&args.origin);
    let unmirrored = &args.unmirrored;

    Ok(quote! {
        #columns

        impl #krate::row::Row for #ident {
            const TABLE: &'static str = #table;
            const KEY: &'static [&'static str] = &[#(#key),*];
            const QUALIFIED: &'static str = #krate::qualified!(#schema, #table);
            const SCHEMA: &'static str = #schema;
            const MIRROR: #krate::row::Mirror = #krate::row::Mirror::#variant;
            const SYNC: #krate::row::Sync = #sync;
            const ACTIVE: Option<&'static str> = #active;
            const RETIRED: Option<&'static str> = #retired;
            const UNMIRRORED: &'static [&'static str] = &[#(#unmirrored),*];
            const ORIGIN: Option<&'static str> = #origin;
            const VERSION: Option<&'static str> = <Self as #krate::row::Columns>::VERSION_COLUMN;
            const CREATED: Option<&'static str> = <Self as #krate::row::Columns>::CREATED_COLUMN;

            fn key_repr(&self) -> String {
                let mut repr = String::new();
                #(repr.push_str(&format!("{:?}\u{1f}", self.#key_idents));)*
                repr
            }

            fn key(&self) -> #krate::row::Key {
                vec![#(#krate::row::KeyValue::key_part(self.#key_idents)),*]
            }
        }
    })
}

/// `sync = "…"`: how a table's rows find the replicas that hold them.
fn sync_strategy(
    sync: Option<&LitStr>,
    mirror: &str,
    krate: &syn::Path,
) -> syn::Result<TokenStream> {
    let Some(lit) = sync else {
        return Ok(quote!(#krate::row::Sync::Referenced));
    };
    if mirror == "record" || mirror == "server" {
        return Err(syn::Error::new_spanned(
            lit,
            "only a table the server sends down (`catalog` or `both`) takes a `sync` strategy",
        ));
    }
    let text = lit.value();
    let bad = |why: &str| syn::Error::new_spanned(lit, format!("sync = \"{text}\": {why}"));
    let (name, inner) = match text.split_once('(') {
        Some((name, rest)) => {
            let inner = rest
                .strip_suffix(')')
                .ok_or_else(|| bad("missing the closing parenthesis"))?;
            (name.trim(), Some(inner))
        }
        None => (text.trim(), None),
    };
    let column = |inner: Option<&str>| -> syn::Result<String> {
        let arg = inner.map(str::trim).unwrap_or_default();
        if plain(arg) {
            Ok(arg.to_owned())
        } else {
            Err(bad("expected one column name"))
        }
    };
    Ok(match name {
        "broadcast" if inner.is_none() => quote!(#krate::row::Sync::Broadcast),
        "assigned" => {
            let column = column(inner)?;
            quote!(#krate::row::Sync::Assigned(#column))
        }
        "follows" => {
            let column = column(inner)?;
            quote!(#krate::row::Sync::Follows(#column))
        }
        "grouped" => {
            let mut members = None;
            let mut assigned = None;
            for part in inner.unwrap_or_default().split(',') {
                let (key, value) = part
                    .split_once('=')
                    .ok_or_else(|| bad("expected `members = …, assigned = …`"))?;
                let value = value.trim();
                if !plain(value) {
                    return Err(bad("a group table is a plain snake_case name"));
                }
                match key.trim() {
                    "members" => members = Some(value.to_owned()),
                    "assigned" => assigned = Some(value.to_owned()),
                    _ => return Err(bad("expected `members = …, assigned = …`")),
                }
            }
            let (Some(members), Some(assigned)) = (members, assigned) else {
                return Err(bad("expected `members = …, assigned = …`"));
            };
            quote!(#krate::row::Sync::Grouped {
                members: #members,
                assigned: #assigned,
            })
        }
        _ => {
            return Err(bad(
                "expected `broadcast`, `assigned(col)`, `grouped(members = …, assigned = …)` or `follows(col)`",
            ))
        }
    })
}
