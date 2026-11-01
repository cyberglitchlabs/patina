// Build script. `sqlx::migrate!` embeds the migration files at compile time,
// but Cargo does not know that, so adding a new .sql file would not trigger a
// rebuild. This line tells Cargo to rebuild when anything under migrations/
// changes.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
