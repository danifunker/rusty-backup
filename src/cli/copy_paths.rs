//! Glob-path mapping helpers shared by `get` (image -> host) and `cp`
//! (image -> image).
//!
//! Both verbs accept a glob source, walk the source volume, and need to
//! lay each match out under a destination rooted at the longest non-glob
//! prefix of the pattern. These four helpers are the host-/destination-
//! agnostic core of that mapping; `get` keeps its host `PathBuf`
//! assembly and `cp` keeps its in-image string assembly on top.

/// True when `s` contains a glob metacharacter (`*`, `?`, `[`, `{`).
pub fn has_glob_chars(s: &str) -> bool {
    s.chars().any(|c| matches!(c, '*' | '?' | '[' | '{'))
}

/// Compute the longest leading prefix of a glob pattern that contains no
/// glob characters — the segments to strip when laying matches under a
/// destination.
///
/// Examples:
///  * `/foo/*.txt` -> `/foo`
///  * `/foo/**/*.txt` -> `/foo` (since `**` is a glob char)
///  * `*.txt` -> `/`
///  * `/foo/bar` -> `/foo/bar` (all literal; rare in this codepath)
pub fn compute_glob_root(src: &str) -> String {
    let trimmed = src.trim_start_matches('/').trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_string();
    }
    let mut root_parts: Vec<&str> = Vec::new();
    for part in trimmed.split('/') {
        if part.is_empty() {
            continue;
        }
        if has_glob_chars(part) {
            break;
        }
        root_parts.push(part);
    }
    if root_parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", root_parts.join("/"))
    }
}

/// Strip `glob_root` from the front of `full_path` and return the
/// remainder (without a leading `/`). Returns an empty string when
/// `full_path == glob_root` exactly.
pub fn strip_root_prefix(full_path: &str, glob_root: &str) -> String {
    if glob_root == "/" {
        return full_path.trim_start_matches('/').to_string();
    }
    if let Some(rest) = full_path.strip_prefix(glob_root) {
        return rest.trim_start_matches('/').to_string();
    }
    // Fallback: shouldn't happen — if glob_root isn't a prefix, the glob
    // walker wouldn't have matched. Use the basename as a safe fallback
    // rather than panicking.
    full_path.rsplit('/').next().unwrap_or("").to_string()
}

/// Basename of a filesystem-internal path, e.g. `/foo/bar` -> `bar`.
pub fn base_name_of(path: &str) -> String {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_root_strips_globbed_tail() {
        assert_eq!(compute_glob_root("/foo/*.txt"), "/foo");
        assert_eq!(compute_glob_root("/foo/bar/*.txt"), "/foo/bar");
        assert_eq!(compute_glob_root("/foo/**/*.txt"), "/foo");
        assert_eq!(compute_glob_root("/foo/*/baz/*.txt"), "/foo");
    }

    #[test]
    fn glob_root_unanchored_pattern_uses_root() {
        assert_eq!(compute_glob_root("*.txt"), "/");
        assert_eq!(compute_glob_root("**/*.txt"), "/");
    }

    #[test]
    fn glob_root_literal_pattern_is_whole_path() {
        assert_eq!(compute_glob_root("/foo/bar"), "/foo/bar");
        assert_eq!(compute_glob_root("/foo/bar/"), "/foo/bar");
    }

    #[test]
    fn glob_root_brace_in_segment_stops_walk() {
        assert_eq!(compute_glob_root("/foo/{a,b}/c"), "/foo");
    }

    #[test]
    fn strip_root_returns_relative_remainder() {
        assert_eq!(strip_root_prefix("/foo/a.txt", "/foo"), "a.txt");
        assert_eq!(strip_root_prefix("/foo/sub/a.txt", "/foo"), "sub/a.txt");
        assert_eq!(strip_root_prefix("/a.txt", "/"), "a.txt");
    }

    #[test]
    fn strip_root_handles_exact_match() {
        assert_eq!(strip_root_prefix("/foo", "/foo"), "");
    }

    #[test]
    fn base_name_of_strips_trailing_slash() {
        assert_eq!(base_name_of("/foo/bar"), "bar");
        assert_eq!(base_name_of("/foo/bar/"), "bar");
        assert_eq!(base_name_of("/foo"), "foo");
        assert_eq!(base_name_of("/"), "");
    }

    #[test]
    fn has_glob_chars_recognises_each_metacharacter() {
        assert!(has_glob_chars("*"));
        assert!(has_glob_chars("?"));
        assert!(has_glob_chars("[abc]"));
        assert!(has_glob_chars("{a,b}"));
        assert!(!has_glob_chars("/literal/path.txt"));
    }
}
