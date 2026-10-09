# Releasing

1. Move the `CHANGELOG.md` entry from *unreleased* to the version and date,
   and set `version` in the root `Cargo.toml` (and the `=` pin of
   `magic_sync_derive` in `magic_sync/Cargo.toml`) to it.
2. Check what goes out. A build of `magic_sync` is one side, so packaging names
   one:

   ```sh
   cargo package --workspace --locked --features magic_sync/postgres
   ```

3. Publish the derive first, then the engine:

   ```sh
   cargo publish -p magic_sync_derive
   cargo publish -p magic_sync --features postgres
   ```

4. Tag the release (`v0.1.0`) and push the tag.

docs.rs builds the server's side (`[package.metadata.docs.rs]`); the replica's
is documented in the book.
