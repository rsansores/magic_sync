-- A register's tables. The engine's own it installs itself, and the triggers
-- that note every receipt written (`replica::install`).

-- Which register this is: made up the first time it starts.
CREATE TABLE IF NOT EXISTS me (id BLOB PRIMARY KEY NOT NULL);

-- ANCHOR: tables
CREATE TABLE IF NOT EXISTS products (
    id          BLOB PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    updated_at  TEXT NOT NULL,
    retired_at  TEXT
);

CREATE TABLE IF NOT EXISTS receipts (
    id          BLOB PRIMARY KEY NOT NULL,
    register_id BLOB NOT NULL,
    product_id  BLOB NOT NULL REFERENCES products (id),
    quantity    INTEGER NOT NULL,
    total_cents INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);

-- What it prints under every receipt: its own, which the head office may
-- wish another.
CREATE TABLE IF NOT EXISTS footers (
    register_id BLOB PRIMARY KEY NOT NULL,
    text        TEXT NOT NULL,
    changed_at  TEXT NOT NULL
);
-- ANCHOR_END: tables
