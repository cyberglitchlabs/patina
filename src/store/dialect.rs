//! Where the two SQL backends differ, in one place.

/// Rewrite SQLite-style `?` placeholders as PostgreSQL's numbered `$1`, `$2`,
/// ... in order. The statements are written once with `?`; none of them has a
/// literal `?` in text.
pub(crate) fn numbered(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut n = 0;
    for c in sql.chars() {
        if c == '?' {
            n += 1;
            out.push('$');
            out.push_str(&n.to_string());
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn question_marks_become_numbered_placeholders_in_order() {
        assert_eq!(
            numbered("INSERT INTO t (a, b, c) VALUES (?, ?, ?)"),
            "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"
        );
        assert_eq!(numbered("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn numbering_reaches_two_digits() {
        let sql = format!("VALUES ({})", ["?"; 11].join(", "));
        assert!(numbered(&sql).ends_with("$10, $11)"), "{}", numbered(&sql));
    }
}
