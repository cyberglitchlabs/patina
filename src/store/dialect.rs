//! Where the two SQL backends differ, in one place.

/// Rewrite SQLite-style `?` placeholders as PostgreSQL's numbered `$1`, `$2`,
/// ... in order. The statements are written once with `?`. A `?` inside a
/// single-quoted string literal is text, not a placeholder, and is left alone.
pub(crate) fn numbered(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut n = 0;
    let mut in_literal = false;
    for c in sql.chars() {
        if c == '\'' {
            in_literal = !in_literal;
        }
        if c == '?' && !in_literal {
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
    fn a_question_mark_inside_a_string_literal_is_left_alone() {
        assert_eq!(
            numbered("SELECT 1 WHERE a = ? AND b LIKE '%?%' AND c = ?"),
            "SELECT 1 WHERE a = $1 AND b LIKE '%?%' AND c = $2"
        );
    }

    #[test]
    fn numbering_reaches_two_digits() {
        let sql = format!("VALUES ({})", ["?"; 11].join(", "));
        assert!(numbered(&sql).ends_with("$10, $11)"), "{}", numbered(&sql));
    }
}
