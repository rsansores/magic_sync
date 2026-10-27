//! `#[derive(Row)]` and `#[derive(Columns)]` for [magic_sync]: one struct is
//! one table, column for column, and says how it travels. Use them through
//! `magic_sync`, which re-exports both; every attribute is documented there,
//! on [`magic_sync::Row`].
//!
//! Everything the derive is told is checked where it is written: a key that
//! is not a field, a column named twice, an attribute a table of its kind
//! cannot have, a key or version of a type the engine cannot read — each is a
//! compile error pointing at the token that says it.
//!
//! [magic_sync]: https://docs.rs/magic_sync
//! [`magic_sync::Row`]: https://docs.rs/magic_sync/latest/magic_sync/derive.Row.html

#![warn(missing_docs)]

use proc_macro::TokenStream as Tokens;
use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned, ToTokens};
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{parse_macro_input, Data, DeriveInput, Field, Fields, Ident, LitStr, Type};

/// Implements `magic_sync::Row` for a struct that is one table, column for
/// column. Use it as `magic_sync::Row`: the attributes it reads (`#[row(…)]`
/// on the struct and its fields, and the `#[sqlx(…)]` keys that name a
/// column) are documented there.
#[proc_macro_derive(Row, attributes(row))]
pub fn derive_row(input: Tokens) -> Tokens {
    let input = parse_macro_input!(input as DeriveInput);
    row(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Implements `magic_sync::Columns` for a part of a row, which the row brings
/// in with `#[sqlx(flatten)]`. Use it as `magic_sync::Columns`: its fields take
/// the same `#[row(…)]` flags as a row's, and the struct only `crate`.
#[proc_macro_derive(Columns, attributes(row))]
pub fn derive_columns(input: Tokens) -> Tokens {
    let input = parse_macro_input!(input as DeriveInput);
    columns_only(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// The keys `#[row(…)]` takes on a struct that derives `Row`.
const ROW_KEYS: &str = "`table`, `schema`, `mirror`, `sync`, `key`, `conflict`, `active`, \
                        `from`, `depends`, `retired`, `origin`, `unmirrored`, `apply` or `crate`";

/// PostgreSQL keeps the first 63 bytes of a longer name, and would answer
/// for a table or column the row does not name.
const LONGEST_NAME: usize = 63;

#[derive(Default)]
struct Args {
    table: Option<LitStr>,
    key: Option<LitStr>,
    conflict: Option<LitStr>,
    schema: Option<LitStr>,
    mirror: Option<LitStr>,
    sync: Option<LitStr>,
    active: Option<LitStr>,
    from: Option<LitStr>,
    depends: Option<LitStr>,
    retired: Option<LitStr>,
    origin: Option<LitStr>,
    unmirrored: Option<LitStr>,
    apply: Option<LitStr>,
    krate: Option<LitStr>,
}

/// The struct's `#[row(…)]` keys. `only_crate`: a part a row flattens in,
/// which takes `crate` and nothing else.
fn parse_args(input: &DeriveInput, only_crate: bool) -> syn::Result<Args> {
    let mut args = Args::default();
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("row")) {
        attr.parse_nested_meta(|meta| {
            let slot = match meta.path.get_ident().map(Ident::to_string).as_deref() {
                Some("crate") => &mut args.krate,
                Some(_) if only_crate => return Err(meta.error(
                    "#[derive(Columns)] is a part a row flattens in: only `crate` applies to it",
                )),
                Some("table") => &mut args.table,
                Some("key") => &mut args.key,
                Some("conflict") => &mut args.conflict,
                Some("schema") => &mut args.schema,
                Some("mirror") => &mut args.mirror,
                Some("sync") => &mut args.sync,
                Some("active") => &mut args.active,
                Some("from") => &mut args.from,
                Some("depends") => &mut args.depends,
                Some("retired") => &mut args.retired,
                Some("origin") => &mut args.origin,
                Some("unmirrored") => &mut args.unmirrored,
                Some("apply") => &mut args.apply,
                _ => return Err(meta.error(format!("expected {ROW_KEYS}"))),
            };
            once(slot, &meta)
        })?;
    }
    Ok(args)
}

/// Sets `slot` to the literal `meta` carries, refusing it a second time.
fn once(slot: &mut Option<LitStr>, meta: &ParseNestedMeta) -> syn::Result<()> {
    let name = meta.path.to_token_stream().to_string();
    if slot.is_some() {
        return Err(meta.error(format!("`{name}` is given twice")));
    }
    let value = meta
        .value()
        .map_err(|_| meta.error(format!("`{name}` takes a value: `{name} = \"…\"`")))?;
    *slot = Some(value.parse()?);
    Ok(())
}

/// A name the engine writes into SQL, quoted: refused when no database could
/// hold it as written.
fn identifier(text: &str, at: Span) -> syn::Result<String> {
    if text.is_empty() {
        return Err(syn::Error::new(at, "an empty name"));
    }
    if text.contains('\0') {
        return Err(syn::Error::new(at, "a name cannot hold a NUL character"));
    }
    if text.len() > LONGEST_NAME {
        return Err(syn::Error::new(
            at,
            format!("`{text}` is longer than the {LONGEST_NAME} bytes PostgreSQL keeps of a name"),
        ));
    }
    Ok(text.to_owned())
}

/// `value` as the generated code's `Option`.
fn option(value: Option<impl ToTokens>) -> TokenStream {
    match value {
        Some(value) => quote!(::core::option::Option::Some(#value)),
        None => quote!(::core::option::Option::None),
    }
}

/// `name` quoted as SQL quotes an identifier.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A comma-separated list of names, each checked, none twice.
fn names(lit: &LitStr) -> syn::Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for part in lit.value().split(',') {
        let name = identifier(part.trim(), lit.span())?;
        if out.contains(&name) {
            return Err(syn::Error::new_spanned(
                lit,
                format!("`{name}` is listed twice"),
            ));
        }
        out.push(name);
    }
    Ok(out)
}

fn krate(args: &Args) -> syn::Result<syn::Path> {
    match &args.krate {
        Some(lit) => lit.parse(),
        None => Ok(syn::parse_quote!(::magic_sync)),
    }
}

/// What `ty` holds, when it is written as the standard `Option<T>`: bare,
/// or by its path in `std` or `core`. Read off the type as written: an alias
/// to `Option` reads as NOT NULL, which the engine's check of the table
/// against its row then refuses.
fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else { return None };
    if path.qself.is_some() {
        return None;
    }
    let names: Vec<String> = path
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect();
    let std_path = matches!(
        names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice(),
        ["Option"] | ["std" | "core", "option", "Option"]
    );
    if !std_path {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &path.path.segments.last()?.arguments else {
        return None;
    };
    match args.args.first()? {
        syn::GenericArgument::Type(inner) if args.args.len() == 1 => Some(inner),
        _ => None,
    }
}

/// The `#[sqlx(…)]` keys on a field that name or shape its column.
#[derive(Default)]
struct SqlxField {
    skip: bool,
    flatten: bool,
    rename: Option<LitStr>,
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
                out.rename = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("try_from") {
                out.try_from = Some(meta.value()?.parse::<LitStr>()?.parse()?);
            } else if meta.path.is_ident("json") {
                return Err(meta.error("#[derive(Row)] does not support #[sqlx(json)]"));
            } else if meta.input.peek(syn::Token![=]) {
                let _: syn::Expr = meta.value()?.parse()?;
            } else if meta.input.peek(syn::token::Paren) {
                meta.parse_nested_meta(|_| Ok(()))?;
            }
            Ok(())
        })?;
    }
    if out.skip || out.flatten {
        let stray = out
            .rename
            .as_ref()
            .map(Spanned::span)
            .or_else(|| out.try_from.as_ref().map(Spanned::span));
        if let Some(at) = stray {
            return Err(syn::Error::new(
                at,
                "a skipped or flattened field names no column: nothing reads its `rename` or `try_from`",
            ));
        }
    }
    Ok(out)
}

/// The `#[row(…)]` flags on a field, each where it is written.
#[derive(Default)]
struct RowField {
    generated: Option<Span>,
    replica_owned: Option<Span>,
    version: Option<Span>,
    created: Option<Span>,
    desired: Option<Span>,
}

impl RowField {
    fn any(&self) -> Option<Span> {
        self.generated
            .or(self.replica_owned)
            .or(self.version)
            .or(self.created)
            .or(self.desired)
    }
}

fn row_field(field: &Field) -> syn::Result<RowField> {
    let mut out = RowField::default();
    for attr in field.attrs.iter().filter(|a| a.path().is_ident("row")) {
        attr.parse_nested_meta(|meta| {
            let flag =
                match meta.path.get_ident().map(Ident::to_string).as_deref() {
                    Some("generated") => &mut out.generated,
                    Some("replica_owned") => &mut out.replica_owned,
                    Some("version") => &mut out.version,
                    Some("created") => &mut out.created,
                    Some("desired") => &mut out.desired,
                    _ => return Err(meta.error(
                        "expected `version`, `created`, `replica_owned`, `generated` or `desired`",
                    )),
                };
            let name = meta.path.to_token_stream().to_string();
            if flag.is_some() {
                return Err(meta.error(format!("`{name}` is given twice")));
            }
            if !meta.input.is_empty() && !meta.input.peek(syn::Token![,]) {
                return Err(meta.error(format!("`{name}` takes no value")));
            }
            *flag = Some(meta.path.span());
            Ok(())
        })?;
    }
    let refuse = |at: Option<Span>, why: &str| match at {
        Some(at) => Err(syn::Error::new(at, why)),
        None => Ok(()),
    };
    if out.generated.is_some() {
        refuse(
            out.version
                .or(out.created)
                .or(out.replica_owned)
                .or(out.desired),
            "a #[row(generated)] column is computed by the database: nothing writes it",
        )?;
    }
    if out.desired.is_some() {
        refuse(
            out.version.or(out.created),
            "a #[row(desired)] column is one a wish sets: the version moves by itself, and a \
             #[row(created)] column never moves",
        )?;
    }
    if out.version.is_some() {
        refuse(
            out.created,
            "a #[row(version)] column moves with every write; a #[row(created)] one never moves",
        )?;
    }
    Ok(out)
}

/// A column a struct declares directly.
struct Direct {
    name: String,
    /// Where the name is written: its `rename`, or the field.
    at: Span,
    ident: Ident,
    ty: Type,
    nullable: bool,
    flags: RowField,
}

/// What `#[derive(Columns)]` and `#[derive(Row)]` share: the columns, and the
/// `Columns` impl that lists and binds them.
struct Declared {
    tokens: TokenStream,
    direct: Vec<Direct>,
    flattened: Vec<Type>,
}

fn refuse_shape(input: &DeriveInput) -> syn::Result<&syn::FieldsNamed> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "a row is one table: it takes no generic parameters",
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "a row is a struct, one field per column",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "a row is a struct with named fields, one per column",
        ));
    };
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("sqlx")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                return Err(meta.error(
                    "#[derive(Row)] reads column names off the fields: rename them one by one",
                ));
            }
            if meta.input.peek(syn::Token![=]) {
                let _: syn::Expr = meta.value()?.parse()?;
            } else if meta.input.peek(syn::token::Paren) {
                meta.parse_nested_meta(|_| Ok(()))?;
            }
            Ok(())
        })?;
    }
    Ok(fields)
}

/// `key`: the row's key columns, whose kind the engine reads off their type.
fn declared(input: &DeriveInput, krate: &syn::Path, key: &[String]) -> syn::Result<Declared> {
    let fields = refuse_shape(input)?;
    let private = quote!(#krate::__private);
    let mut push = Vec::new();
    let mut bind = Vec::new();
    let mut direct: Vec<Direct> = Vec::new();
    let mut flattened: Vec<Type> = Vec::new();
    for field in &fields.named {
        let sqlx = sqlx_field(field)?;
        let flags = row_field(field)?;
        if let Some(at) = flags.any().filter(|_| sqlx.skip || sqlx.flatten) {
            return Err(syn::Error::new(
                at,
                "a #[row(…)] field names a column; a skipped or flattened field is none",
            ));
        }
        if sqlx.skip {
            continue;
        }
        let ty = &field.ty;
        let ident = field.ident.clone().expect("a named field");
        if sqlx.flatten {
            push.push(quote_spanned! {ty.span()=>
                <#ty as #private::Columns>::columns(out);
            });
            bind.push(quote_spanned! {ty.span()=>
                <#ty as #private::Columns>::push_binds(self.#ident, sep);
            });
            flattened.push(ty.clone());
            continue;
        }
        let field_name = ident.to_string().trim_start_matches("r#").to_owned();
        let (name, at) = match &sqlx.rename {
            Some(lit) => (identifier(&lit.value(), lit.span())?, lit.span()),
            None => (identifier(&field_name, ident.span())?, ident.span()),
        };
        if let Some(earlier) = direct.iter().find(|d| d.name == name) {
            return Err(syn::Error::new(
                at,
                format!(
                    "the column `{name}` is already the field `{}`",
                    earlier.ident
                ),
            ));
        }
        let nullable = option_inner(ty).is_some();
        if let Some(version) = flags.version.filter(|_| nullable) {
            return Err(syn::Error::new(
                version,
                "#[row(version)] orders a row's versions: it cannot be NULL",
            ));
        }
        if flags.generated.is_none() {
            bind.push(quote!(sep.push_bind(self.#ident);));
        }
        let decoded = sqlx.try_from.as_ref().unwrap_or(ty);
        let key = if key.contains(&name) {
            quote_spanned! {ty.span()=>
                ::core::option::Option::Some(<#ty as #krate::row::KeyValue>::KIND)
            }
        } else {
            quote!(::core::option::Option::None)
        };
        let version = if flags.version.is_some() {
            quote_spanned! {ty.span()=>
                ::core::option::Option::Some(<#ty as #krate::row::Version>::KIND)
            }
        } else {
            quote!(::core::option::Option::None)
        };
        let generated = flags.generated.is_some();
        let replica_owned = flags.replica_owned.is_some();
        let desired = flags.desired.is_some();
        push.push(quote! {
            out.push(#private::Column {
                name: #name,
                field: #field_name,
                nullable: #nullable,
                generated: #generated,
                replica_owned: #replica_owned,
                desired: #desired,
                key: #key,
                version: #version,
                int64: #private::is_int64::<#ty>(),
                compatible: |info| {
                    <#decoded as #private::sqlx::Type<#private::Db>>::compatible(info)
                },
            });
        });
        direct.push(Direct {
            name,
            at,
            ident,
            ty: ty.clone(),
            nullable,
            flags,
        });
    }

    // The one column marked so, here or in a part flattened in.
    let role = |pick: fn(&RowField) -> Option<Span>, which: TokenStream, what: &str| {
        let marked: Vec<&Direct> = direct.iter().filter(|d| pick(&d.flags).is_some()).collect();
        if let [_, second, ..] = marked.as_slice() {
            return Err(syn::Error::new(
                pick(&second.flags).unwrap_or(second.at),
                format!("one column at most is #[row({what})]"),
            ));
        }
        let here = option(marked.first().map(|column| &column.name));
        let parts = flattened
            .iter()
            .map(|part| quote!(<#part as #private::Columns>::#which));
        let refusal =
            format!("one column at most is #[row({what})], here or in a part flattened in");
        Ok(quote! {
            {
                let found = [#here, #(#parts),*];
                if #private::count(&found) > 1 {
                    ::core::panic!(#refusal)
                }
                #private::first(&found)
            }
        })
    };
    let version = role(|f| f.version, quote!(VERSION), "version")?;
    let created = role(|f| f.created, quote!(CREATED), "created")?;
    let owned_here = direct.iter().any(|d| d.flags.replica_owned.is_some());
    let owned_parts = flattened
        .iter()
        .map(|part| quote!(|| <#part as #private::Columns>::REPLICA_OWNED));

    let ident = &input.ident;
    let tokens = quote! {
        // The roles are settled where the struct is declared, not where a
        // row of it is first used.
        const _: () = {
            let _ = <#ident as #private::Columns>::VERSION;
            let _ = <#ident as #private::Columns>::CREATED;
        };

        impl #private::Sealed for #ident {}

        impl #private::Columns for #ident {
            const VERSION: ::core::option::Option<&'static ::core::primitive::str> = #version;
            const CREATED: ::core::option::Option<&'static ::core::primitive::str> = #created;
            const REPLICA_OWNED: ::core::primitive::bool = #owned_here #(#owned_parts)*;

            fn columns(out: &mut ::std::vec::Vec<#private::Column>) {
                #(#push)*
            }

            fn push_binds<'args>(
                self,
                sep: &mut #private::sqlx::query_builder::Separated<'_, 'args, #private::Db, &'static ::core::primitive::str>,
            ) {
                #(#bind)*
            }
        }
    };
    Ok(Declared {
        tokens,
        direct,
        flattened,
    })
}

fn columns_only(input: &DeriveInput) -> syn::Result<TokenStream> {
    let args = parse_args(input, true)?;
    let declared = declared(input, &krate(&args)?, &[])?;
    if let Some(at) = declared.direct.iter().find_map(|d| d.flags.desired) {
        return Err(syn::Error::new(
            at,
            "#[row(desired)] is a field of the setting itself, not of a part flattened in",
        ));
    }
    Ok(declared.tokens)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mirror {
    Catalog,
    Record,
    Both,
    Setting,
}

impl Mirror {
    fn parse(lit: &LitStr) -> syn::Result<Self> {
        Ok(match lit.value().as_str() {
            "catalog" => Mirror::Catalog,
            "record" => Mirror::Record,
            "both" => Mirror::Both,
            "setting" => Mirror::Setting,
            other => {
                return Err(syn::Error::new_spanned(
                    lit,
                    format!(
                        "mirror = \"{other}\": expected \"catalog\", \"record\", \"both\" or \"setting\""
                    ),
                ))
            }
        })
    }

    fn variant(self) -> TokenStream {
        match self {
            Mirror::Catalog => quote!(Catalog),
            Mirror::Record => quote!(Record),
            Mirror::Both => quote!(Both),
            Mirror::Setting => quote!(Setting),
        }
    }

    /// Whether the server sends the table's rows down.
    fn sent_down(self) -> bool {
        matches!(self, Mirror::Catalog | Mirror::Both)
    }

    /// Whether a replica's rows of the table go up, ordered by a version.
    fn versioned(self) -> bool {
        matches!(self, Mirror::Record | Mirror::Setting)
    }
}

/// The relation `key = "…"` names, `view` or `schema.view`, quoted.
fn relation(lit: Option<&LitStr>, key: &str) -> syn::Result<Option<String>> {
    let Some(lit) = lit else {
        return Ok(None);
    };
    let text = lit.value();
    qualified(&text, lit.span())
        .ok_or_else(|| {
            syn::Error::new_spanned(
                lit,
                format!("{key} = \"{text}\": expected `view` or `schema.view`"),
            )
        })?
        .map(Some)
}

/// `text`, a relation written `view` or `schema.view`, quoted: `None` when it
/// is written otherwise.
fn qualified(text: &str, at: Span) -> Option<syn::Result<String>> {
    let parts: Vec<&str> = text.split('.').collect();
    if parts.len() > 2 {
        return None;
    }
    Some(
        parts
            .into_iter()
            .map(|p| identifier(p, at).map(|p| quoted(&p)))
            .collect::<syn::Result<Vec<_>>>()
            .map(|parts| parts.join(".")),
    )
}

/// `text` split at its commas, but those inside parentheses: the arguments
/// of a strategy, one of which may be a list.
fn arguments(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let (mut depth, mut from) = (0_usize, 0);
    for (at, c) in text.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                out.push(&text[from..at]);
                from = at + 1;
            }
            _ => {}
        }
    }
    out.push(&text[from..]);
    out
}

/// Refuses `lit`, given on a table that is not sent down.
fn only_sent_down(lit: Option<&LitStr>, mirror: Mirror, what: &str) -> syn::Result<()> {
    match lit {
        Some(lit) if !mirror.sent_down() => Err(syn::Error::new_spanned(
            lit,
            format!("only a table the server sends down (`catalog` or `both`) has {what}"),
        )),
        _ => Ok(()),
    }
}

fn row(input: &DeriveInput) -> syn::Result<TokenStream> {
    let args = parse_args(input, false)?;
    let krate = krate(&args)?;
    let private = quote!(#krate::__private);
    let ident = &input.ident;
    let missing = |what: &str| {
        syn::Error::new(
            Span::call_site(),
            format!("#[derive(Row)] needs #[row({what} = \"…\")]"),
        )
    };
    let table_lit = args.table.as_ref().ok_or_else(|| missing("table"))?;
    let table = identifier(&table_lit.value(), table_lit.span())?;
    let schema = match &args.schema {
        Some(lit) => identifier(&lit.value(), lit.span())?,
        None => "public".to_owned(),
    };
    let mirror = Mirror::parse(args.mirror.as_ref().ok_or_else(|| missing("mirror"))?)?;
    let key = match &args.key {
        Some(lit) => names(lit)?,
        None => vec!["id".to_owned()],
    };
    let Declared {
        tokens: columns,
        direct,
        flattened,
    } = declared(input, &krate, &key)?;
    let column = |name: &str| direct.iter().find(|d| d.name == name);

    if mirror != Mirror::Both {
        if let Some(at) = direct.iter().find_map(|d| d.flags.replica_owned) {
            return Err(syn::Error::new(
                at,
                "#[row(replica_owned)] splits a table written on both sides: needs `mirror = \"both\"`",
            ));
        }
    }
    if !mirror.versioned() {
        if let Some(at) = direct.iter().find_map(|d| d.flags.version) {
            return Err(syn::Error::new(
                at,
                "#[row(version)] orders the reports of a replica's table: only a `record` or a \
                 `setting` has one, and a replica takes what the server sends whatever its version",
            ));
        }
    }
    if mirror != Mirror::Setting {
        if let Some(at) = direct.iter().find_map(|d| d.flags.desired) {
            return Err(syn::Error::new(
                at,
                "#[row(desired)] is a column the server may wish a replica's setting to hold: \
                 needs `mirror = \"setting\"`",
            ));
        }
        if let Some(lit) = &args.apply {
            return Err(syn::Error::new_spanned(
                lit,
                "only a `setting` has an `apply` rule: it says which wish a replica carries out",
            ));
        }
    }
    let (route, mut checks) = route(args.sync.as_ref(), mirror, &direct, &key, &krate)?;
    only_sent_down(args.active.as_ref(), mirror, "an `active` relation")?;
    if let Some(lit) = &args.from {
        if mirror != Mirror::Catalog {
            return Err(syn::Error::new_spanned(
                lit,
                "a row read `from` a relation of its own is only sent down: needs \
                 `mirror = \"catalog\"`",
            ));
        }
        if args.active.is_some() {
            return Err(syn::Error::new_spanned(
                lit,
                "the rows a row is read `from` are the rows it sends: `active` is left out",
            ));
        }
    }
    only_sent_down(args.retired.as_ref(), mirror, "a `retired` column")?;

    let key_at = args.key.as_ref().map_or(ident.span(), LitStr::span);
    let mut key_columns = Vec::new();
    for name in &key {
        let Some(column) = column(name) else {
            let why = if flattened.is_empty() {
                format!("the key column `{name}` is not a field of the row")
            } else {
                format!("the key column `{name}` is not a field of the row itself: a key is not flattened in")
            };
            return Err(syn::Error::new(key_at, why));
        };
        if column.nullable {
            return Err(syn::Error::new(column.at, "a key column cannot be NULL"));
        }
        if let Some(at) = column.flags.any() {
            return Err(syn::Error::new(
                at,
                "a key column is the row's identity: it is not generated, owned by a replica, \
                 a version, a creation time or desired",
            ));
        }
        key_columns.push(column);
    }

    let conflict = match &args.conflict {
        Some(lit) => {
            if mirror != Mirror::Record {
                return Err(syn::Error::new_spanned(
                    lit,
                    "only a `record` has a `conflict`",
                ));
            }
            let names = names(lit)?;
            if let Some(name) = key.iter().find(|k| !names.contains(k)) {
                return Err(syn::Error::new_spanned(
                    lit,
                    format!("conflict: holds every key column, and `{name}` is missing"),
                ));
            }
            for name in names.iter().filter(|n| !key.contains(n)) {
                let Some(column) = column(name) else {
                    return Err(syn::Error::new_spanned(
                        lit,
                        format!("conflict: `{name}` is not a field of the row itself"),
                    ));
                };
                if column.nullable {
                    return Err(syn::Error::new(
                        column.at,
                        "a conflict column cannot be NULL",
                    ));
                }
                if let Some(at) = column.flags.any() {
                    return Err(syn::Error::new(
                        at,
                        "a conflict column is written once with the row: it is not generated, \
                         owned by a replica, a version, a creation time or desired",
                    ));
                }
            }
            names
        }
        None => key.clone(),
    };

    if let Some(lit) = &args.origin {
        let origin = identifier(&lit.value(), lit.span())?;
        if matches!(mirror, Mirror::Catalog) {
            return Err(syn::Error::new_spanned(
                lit,
                "only a table a replica writes (`record`, `both` or `setting`) has an `origin`",
            ));
        }
        let Some(column) = key_columns.iter().find(|c| c.name == origin) else {
            return Err(syn::Error::new_spanned(
                lit,
                format!("origin = \"{origin}\": the replica a record belongs to is a key column"),
            ));
        };
        let ty = &column.ty;
        let refusal = format!("origin = \"{origin}\": the replica a record belongs to is a uuid");
        checks.push(quote_spanned! {lit.span()=>
            const _: () = ::core::assert!(
                ::core::matches!(<#ty as #krate::row::KeyValue>::KIND, #krate::row::KeyKind::Uuid),
                #refusal,
            );
        });
    }
    let retired = match &args.retired {
        Some(lit) => Some(identifier(&lit.value(), lit.span())?),
        None => None,
    };
    let unmirrored = match &args.unmirrored {
        Some(lit) => {
            let names = names(lit)?;
            for name in &names {
                if column(name).is_some() {
                    return Err(syn::Error::new_spanned(
                        lit,
                        format!("`{name}` is a field of the row: a column the row leaves out is not one of its fields"),
                    ));
                }
                if retired.as_ref() == Some(name) {
                    return Err(syn::Error::new_spanned(
                        lit,
                        format!("`{name}` is the retired column already"),
                    ));
                }
            }
            names
        }
        None => Vec::new(),
    };
    let active = relation(args.active.as_ref(), "active")?;
    let from = relation(args.from.as_ref(), "from")?;
    only_sent_down(args.depends.as_ref(), mirror, "rows it `depends` on")?;
    let depends = match &args.depends {
        Some(lit) => names(lit)?,
        None => Vec::new(),
    };
    if !mirror.versioned() {
        let refusal = "#[row(version)] in a part flattened in orders the reports of a replica's \
                       table: only a `record` or a `setting` has one";
        checks.extend(flattened.iter().map(|part| {
            quote_spanned! {part.span()=>
                const _: () = ::core::assert!(<#part as #private::Columns>::VERSION.is_none(), #refusal);
            }
        }));
    }
    if mirror != Mirror::Both {
        let refusal = "#[row(replica_owned)] in a part flattened in splits a table written on \
                       both sides: the row needs `mirror = \"both\"`";
        checks.extend(flattened.iter().map(|part| {
            quote_spanned! {part.span()=>
                const _: () = ::core::assert!(!<#part as #private::Columns>::REPLICA_OWNED, #refusal);
            }
        }));
    }

    let setting = if mirror == Mirror::Setting {
        Some(setting(&args, ident, &direct, &krate)?)
    } else {
        None
    };

    let active = option(active);
    let from = option(from);
    let retired = option(retired);
    let origin = option(args.origin.as_ref().map(LitStr::value));
    let variant = mirror.variant();
    let (quoted_schema, quoted_table) = (quoted(&schema), quoted(&table));
    let version_text = match direct.iter().find(|d| d.flags.version.is_some()) {
        Some(Direct { ident, ty, .. }) => quote_spanned! {ty.span()=>
            ::core::option::Option::Some(<#ty as #krate::row::Version>::text(&self.#ident))
        },
        None => quote!(::core::option::Option::None),
    };
    let key_parts = key_columns.iter().map(|c| {
        let (ident, ty) = (&c.ident, &c.ty);
        quote_spanned! {ty.span()=>
            <#ty as #krate::row::KeyValue>::key_part(&self.#ident)
        }
    });

    Ok(quote! {
        #columns

        impl #private::Row for #ident {
            const TABLE: &'static ::core::primitive::str = #table;
            const SCHEMA: &'static ::core::primitive::str = #schema;
            const KEY: &'static [&'static ::core::primitive::str] = &[#(#key),*];
            const CONFLICT: &'static [&'static ::core::primitive::str] = &[#(#conflict),*];
            const MIRROR: #krate::row::Mirror = #krate::row::Mirror::#variant;
            const QUALIFIED: &'static ::core::primitive::str = #krate::__qualified!(#quoted_schema, #quoted_table);
            const ROUTE: #private::Route = #route;
            const ACTIVE: ::core::option::Option<&'static ::core::primitive::str> = #active;
            const FROM: ::core::option::Option<&'static ::core::primitive::str> = #from;
            const DEPENDS: &'static [&'static ::core::primitive::str] = &[#(#depends),*];
            const RETIRED: ::core::option::Option<&'static ::core::primitive::str> = #retired;
            const UNMIRRORED: &'static [&'static ::core::primitive::str] = &[#(#unmirrored),*];
            const ORIGIN: ::core::option::Option<&'static ::core::primitive::str> = #origin;

            fn key(&self) -> #krate::row::Key {
                ::std::vec![#(#key_parts),*]
            }

            fn version_text(&self) -> ::core::option::Option<::std::string::String> {
                #version_text
            }
        }

        #(#checks)*

        #setting
    })
}

/// A setting's `Setting` impl: its desired columns, its `apply` rule, and
/// what the engine does to a row of it when a wish is carried out. A setting
/// needs an `origin` (where a wish goes), a version of its own (what a wish
/// is based on) and at least one desired column.
fn setting(
    args: &Args,
    ident: &Ident,
    direct: &[Direct],
    krate: &syn::Path,
) -> syn::Result<TokenStream> {
    let refuse = |why: &str| syn::Error::new_spanned(ident, why);
    if args.origin.is_none() {
        return Err(refuse(
            "a setting needs `origin = \"…\"`: the key column naming the replica it belongs to, \
             which is where a wish of it goes",
        ));
    }
    let Some(version) = direct.iter().find(|d| d.flags.version.is_some()) else {
        return Err(refuse(
            "a setting needs a #[row(version)] field of its own: what a wish is based on, and what \
             moves when one is carried out",
        ));
    };
    let desired: Vec<&Direct> = direct
        .iter()
        .filter(|d| d.flags.desired.is_some())
        .collect();
    if desired.is_empty() {
        return Err(refuse(
            "a setting needs at least one #[row(desired)] field: a column a wish may set",
        ));
    }
    let apply = match &args.apply {
        None => quote!(Always),
        Some(lit) => match lit.value().as_str() {
            "always" => quote!(Always),
            "if_unchanged" => quote!(IfUnchanged),
            other => {
                return Err(syn::Error::new_spanned(
                    lit,
                    format!("apply = \"{other}\": expected \"always\" or \"if_unchanged\""),
                ))
            }
        },
    };
    let fields = desired.iter().map(|d| &d.ident);
    let (version_ident, version_ty) = (&version.ident, &version.ty);
    Ok(quote! {
        impl #krate::row::Setting for #ident {
            const APPLY: #krate::row::Apply = #krate::row::Apply::#apply;

            fn take_desired(&mut self, from: Self) {
                #(self.#fields = from.#fields;)*
            }

            fn raise_version(&mut self) {
                self.#version_ident = <#version_ty as #krate::row::Version>::raised(&self.#version_ident);
            }
        }
    })
}

/// `sync = "…"`: how a table's rows find the replicas that hold them, and
/// the checks of the types of the columns it names, which are the row's own.
/// `key`: the row's key columns, in key order.
fn route(
    sync: Option<&LitStr>,
    mirror: Mirror,
    direct: &[Direct],
    key: &[String],
    krate: &syn::Path,
) -> syn::Result<(TokenStream, Vec<TokenStream>)> {
    let private = quote!(#krate::__private);
    let Some(lit) = sync else {
        return Ok((quote!(#private::Route::Referenced), Vec::new()));
    };
    if !mirror.sent_down() {
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
    let column = |inner: Option<&str>| -> syn::Result<&Direct> {
        let arg = inner.map(str::trim).unwrap_or_default();
        if arg.is_empty() || arg.contains(',') || arg.contains('=') {
            return Err(bad("expected one column name"));
        }
        let name = identifier(arg, lit.span())?;
        direct
            .iter()
            .find(|d| d.name == name)
            .ok_or_else(|| bad(&format!("`{name}` is not a field of the row")))
    };
    // The column holds a uuid or an integer, or is nullable.
    let of_a_key = |column: &Direct| {
        let ty = option_inner(&column.ty).unwrap_or(&column.ty);
        quote_spanned! {lit.span()=>
            const _: #krate::row::KeyKind = <#ty as #krate::row::KeyValue>::KIND;
        }
    };
    Ok(match name {
        "broadcast" if inner.is_none() => (quote!(#private::Route::Broadcast), Vec::new()),
        "assigned" => {
            let column = column(inner)?;
            let name = &column.name;
            let ty = option_inner(&column.ty).unwrap_or(&column.ty);
            let refusal = format!("sync = \"{text}\": the replica a row is sent to is a uuid");
            let check = quote_spanned! {lit.span()=>
                const _: () = ::core::assert!(
                    ::core::matches!(<#ty as #krate::row::KeyValue>::KIND, #krate::row::KeyKind::Uuid),
                    #refusal,
                );
            };
            (quote!(#private::Route::Assigned(#name)), vec![check])
        }
        "follows" => {
            let column = column(inner)?;
            let name = &column.name;
            (
                quote!(#private::Route::Follows(#name)),
                vec![of_a_key(column)],
            )
        }
        "grouped" => {
            let mut members = None;
            let mut assigned = None;
            for part in inner.unwrap_or_default().split(',') {
                let expected = || bad("expected `members = …, assigned = …`");
                let (key, value) = part.split_once('=').ok_or_else(expected)?;
                let value = identifier(value.trim(), lit.span())?;
                let slot = match key.trim() {
                    "members" => &mut members,
                    "assigned" => &mut assigned,
                    _ => return Err(expected()),
                };
                if slot.replace(value).is_some() {
                    return Err(bad(&format!("`{}` is given twice", key.trim())));
                }
            }
            let (Some(members), Some(assigned)) = (members, assigned) else {
                return Err(bad("expected `members = …, assigned = …`"));
            };
            (
                quote!(#private::Route::Grouped {
                    members: #members,
                    assigned: #assigned,
                }),
                Vec::new(),
            )
        }
        "routed" => {
            let expected = || bad("expected `view = …, row = …, replica = …`");
            let (mut view, mut row, mut replica) = (None, None, None);
            for part in arguments(inner.unwrap_or_default()) {
                let (name, value) = part.split_once('=').ok_or_else(expected)?;
                let slot = match name.trim() {
                    "view" => &mut view,
                    "row" => &mut row,
                    "replica" => &mut replica,
                    _ => return Err(expected()),
                };
                if slot.replace(value.trim()).is_some() {
                    return Err(bad(&format!("`{}` is given twice", name.trim())));
                }
            }
            let (Some(view), Some(row), Some(replica)) = (view, row, replica) else {
                return Err(expected());
            };
            let view = qualified(view, lit.span()).ok_or_else(|| {
                bad(&format!(
                    "`view = {view}`: expected a relation, `view` or `schema.view`"
                ))
            })??;
            // One column, or a list of them in parentheses: the row's key,
            // in key order.
            let row: Vec<String> = match row.strip_prefix('(') {
                Some(list) => {
                    let list = list
                        .strip_suffix(')')
                        .ok_or_else(|| bad("missing the closing parenthesis of `row`"))?;
                    list.split(',')
                        .map(|c| identifier(c.trim(), lit.span()))
                        .collect::<syn::Result<_>>()?
                }
                None => vec![identifier(row, lit.span())?],
            };
            if row.len() != key.len() {
                let columns = |n: usize| match n {
                    1 => "1 column".to_owned(),
                    n => format!("{n} columns"),
                };
                return Err(bad(&format!(
                    "the row's key is {}, and `row` names {}: the view's columns that hold the \
                     key, in key order — `row = (a, b)` for a key of two",
                    columns(key.len()),
                    row.len()
                )));
            }
            let replica = identifier(replica, lit.span())?;
            (
                quote!(#private::Route::Routed {
                    view: #view,
                    row: &[#(#row),*],
                    replica: #replica,
                }),
                Vec::new(),
            )
        }
        _ => {
            return Err(bad(
                "expected `broadcast`, `assigned(col)`, `grouped(members = …, assigned = …)`, \
                 `routed(view = …, row = …, replica = …)` or `follows(col)`",
            ))
        }
    })
}
