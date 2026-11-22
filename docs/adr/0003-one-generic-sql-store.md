---
status: accepted
---

# One generic SQL store for SQLite and PostgreSQL

`SqlStore<D>` implements the whole `Store` trait once, over sqlx's `Database`. Each backend is a small `Backend` impl (`store/sqlite.rs`, `store/postgres.rs`) for what really differs: connecting, migrations, the placeholder style (`?` or `$1`), the read snapshot, rows affected and database size. Statements are written once with `?`; Postgres rewrites them to `$1`, `$2`, ... (`store/dialect.rs`).

Before this, two near-identical ~1100-line files held the same statements. A fix could land in one and be missed in the other, and after normalizing placeholders, no query differed.

Season, episode and `changes` are 32-bit `INTEGER` columns on both backends. SQLite used to accept values up to `u32::MAX` there while Postgres refused them; both now refuse anything above `i32::MAX`. A SQLite database that already holds such a value would no longer read back cleanly; no provider sends numbers that large, and Postgres never accepted them.

## Consequences

The shared bounds on the `Store` impl (`Encode`, `Decode`, `Type` for each bound type) are long. A new column type needs a bound added there. The `Store` trait stays the seam; `tests/common::run_contract` runs against both backends and is the test surface.

Do not split the backends into separate files again to work around a dialect difference. Add a method to `Backend` for it.
