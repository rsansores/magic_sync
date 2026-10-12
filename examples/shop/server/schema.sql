-- The head office's tables. The engine's own it installs itself, and the
-- triggers that note every change (`Server::install`).

CREATE SCHEMA IF NOT EXISTS shop;

-- ANCHOR: tables
-- The registers: the replicas, and which of them receive anything.
CREATE TABLE IF NOT EXISTS shop.registers (
    id     UUID PRIMARY KEY,
    name   TEXT NOT NULL,
    url    TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true
);

-- Written here, held by every register.
CREATE TABLE IF NOT EXISTS shop.products (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    price_cents BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

-- Written by a register, filed here.
CREATE TABLE IF NOT EXISTS shop.receipts (
    id          UUID PRIMARY KEY,
    register_id UUID NOT NULL,
    product_id  UUID NOT NULL,
    quantity    INTEGER NOT NULL,
    total_cents BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

-- Each register's footer, as it last said: a setting the head office may
-- wish another.
CREATE TABLE IF NOT EXISTS shop.footers (
    register_id UUID PRIMARY KEY,
    text        TEXT NOT NULL,
    changed_at  TIMESTAMPTZ NOT NULL
);
-- ANCHOR_END: tables

-- ANCHOR: deliveries
-- The transport's queue: each register's messages in the order they were
-- sent, each kept until the register acknowledges it by its id. In the
-- database, so a restart loses none.
CREATE TABLE IF NOT EXISTS shop.deliveries (
    seq         BIGSERIAL PRIMARY KEY,
    register_id UUID NOT NULL,
    id          TEXT NOT NULL,
    message     BYTEA NOT NULL,
    UNIQUE (register_id, id)
);
-- ANCHOR_END: deliveries
