//! Bash-style globbing for filesystem-internal paths.
//!
//! The CLI's read-only verbs (`ls`, `rm`, `get` in a later phase) accept
//! glob patterns. We rely on the `globset` crate for the heavy lifting
//! (`*`, `?`, `[abc]`, `**`, `\` escape) and add a thin helper around
//! it that walks an FS through the [`crate::fs::filesystem::Filesystem`]
//! trait, accumulating entries whose `/`-joined paths match the pattern.
//!
//! Brace expansion (`{a,b,c}`) is implemented by pre-expanding into a
//! product of patterns before handing each one to globset.
//!
//! ## Pattern shape
//!
//! - Leading `/` anchors the pattern to the filesystem root.
//! - Patterns without a leading `/` match anywhere (treated as
//!   `**/<pat>`).
//! - `**` (globstar) matches across `/` boundaries.
//! - Case sensitivity follows the filesystem's native rule; the caller
//!   passes that into [`compile_pattern`]. CLI verbs default to the
//!   target's native rule and accept `--ignore-case` /
//!   `--case-sensitive` to override.
//!
//! Include / exclude lists follow the **exclude-always-wins** rule
//! documented in `docs/cli-todo.md`: a match-set is built from
//! includes, then exclude matches are subtracted unconditionally.

use anyhow::{anyhow, Result};
use globset::{Glob, GlobBuilder, GlobMatcher};

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::Filesystem;

/// One compiled pattern. Matched against `/`-joined entry paths.
pub struct Pattern {
    inner: GlobMatcher,
    /// Original unexpanded pattern, for error messages.
    pub source: String,
}

impl Pattern {
    /// Compile a single pattern. Internally calls [`expand_braces`] —
    /// only the first expansion is used; callers wanting multi-match
    /// from brace expansion should compile the full expansion via
    /// [`compile_patterns`].
    pub fn compile_one(pattern: &str, case_insensitive: bool) -> Result<Self> {
        let g = build_glob(pattern, case_insensitive)?;
        Ok(Self {
            inner: g.compile_matcher(),
            source: pattern.to_string(),
        })
    }

    pub fn is_match(&self, path: &str) -> bool {
        self.inner.is_match(path)
    }
}

/// Compile one user-supplied pattern, expanding `{a,b}` braces into
/// the cartesian product of sub-patterns.
pub fn compile_patterns(pattern: &str, case_insensitive: bool) -> Result<Vec<Pattern>> {
    expand_braces(pattern)
        .into_iter()
        .map(|p| Pattern::compile_one(&p, case_insensitive))
        .collect()
}

fn build_glob(pattern: &str, case_insensitive: bool) -> Result<Glob> {
    // Normalize the anchoring rule: leading `/` is anchored; otherwise
    // we wrap with `**/` so the pattern matches anywhere.
    let normalized = if pattern.starts_with('/') {
        pattern.to_string()
    } else if pattern.contains('/') {
        // Mid-path pattern but no leading slash — keep as-is and let
        // globset's `**` handling do the work via `**/` wrap below.
        format!("**/{pattern}")
    } else {
        // Single-component pattern: match anywhere.
        format!("**/{pattern}")
    };

    GlobBuilder::new(&normalized)
        .case_insensitive(case_insensitive)
        .literal_separator(true)
        .build()
        .map_err(|e| anyhow!("invalid glob pattern {pattern:?}: {e}"))
}

/// Expand `{a,b,c}` brace alternatives into individual patterns.
/// Recursive — `foo/{a,b}/{1,2}` becomes 4 patterns.
///
/// Backslash escapes the next character so `\{` is literal.
pub fn expand_braces(pattern: &str) -> Vec<String> {
    let mut chars = pattern.chars().peekable();
    let mut prefix = String::new();
    while let Some(&c) = chars.peek() {
        if c == '\\' {
            chars.next();
            if let Some(esc) = chars.next() {
                prefix.push('\\');
                prefix.push(esc);
            }
            continue;
        }
        if c == '{' {
            // Find the matching '}' at the same nesting depth.
            chars.next(); // consume '{'
            let mut depth = 1usize;
            let mut group = String::new();
            for c in chars.by_ref() {
                if c == '\\' {
                    group.push('\\');
                    continue;
                }
                if c == '{' {
                    depth += 1;
                    group.push(c);
                } else if c == '}' {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                    group.push(c);
                } else {
                    group.push(c);
                }
            }
            let suffix: String = chars.collect();
            let alternatives = split_top_level(&group);
            let mut out = Vec::new();
            for alt in alternatives {
                let combined = format!("{prefix}{alt}{suffix}");
                out.extend(expand_braces(&combined));
            }
            return out;
        }
        prefix.push(c);
        chars.next();
    }
    vec![prefix]
}

/// Split `a,b,{c,d}` at top-level commas (i.e. commas not inside a
/// nested `{...}`).
fn split_top_level(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut depth = 0i32;
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            buf.push('\\');
            if let Some(esc) = chars.next() {
                buf.push(esc);
            }
            continue;
        }
        if c == '{' {
            depth += 1;
            buf.push(c);
        } else if c == '}' {
            depth -= 1;
            buf.push(c);
        } else if c == ',' && depth == 0 {
            out.push(std::mem::take(&mut buf));
        } else {
            buf.push(c);
        }
    }
    out.push(buf);
    out
}

/// Walk `fs` from `root` and collect every entry whose `/`-joined
/// path matches any of `includes` and is NOT excluded by any of
/// `excludes`.
///
/// Returns `(parent_entry, child_entry, full_path_string)` triples so
/// callers (e.g. `rm`) have both the parent FileEntry needed for
/// delete operations and the leaf entry to act on.
pub fn collect_matches(
    fs: &mut dyn Filesystem,
    includes: &[Pattern],
    excludes: &[Pattern],
) -> Result<Vec<(FileEntry, FileEntry, String)>> {
    let root = fs.root().map_err(|e| anyhow!("root: {e}"))?;
    let mut out = Vec::new();
    walk(fs, &root, "", includes, excludes, &mut out)?;
    Ok(out)
}

fn walk(
    fs: &mut dyn Filesystem,
    parent: &FileEntry,
    parent_path: &str,
    includes: &[Pattern],
    excludes: &[Pattern],
    out: &mut Vec<(FileEntry, FileEntry, String)>,
) -> Result<()> {
    let children = fs
        .list_directory(parent)
        .map_err(|e| anyhow!("list_directory({parent_path:?}): {e}"))?;
    let parent_entry = parent.clone();
    for child in children {
        let full = if parent_path.is_empty() {
            format!("/{}", child.name)
        } else {
            format!("{parent_path}/{}", child.name)
        };
        let match_in = includes.iter().any(|p| p.is_match(&full));
        let match_ex = excludes.iter().any(|p| p.is_match(&full));
        if match_in && !match_ex {
            out.push((parent_entry.clone(), child.clone(), full.clone()));
        }
        if child.is_directory() {
            walk(fs, &child, &full, includes, excludes, out)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brace_expansion_single_level() {
        let mut out = expand_braces("foo/{a,b}");
        out.sort();
        assert_eq!(out, vec!["foo/a", "foo/b"]);
    }

    #[test]
    fn brace_expansion_nested() {
        let mut out = expand_braces("{x,y}/{1,2}");
        out.sort();
        assert_eq!(out, vec!["x/1", "x/2", "y/1", "y/2"]);
    }

    #[test]
    fn brace_expansion_escaped_brace_is_literal() {
        let out = expand_braces(r"foo\{not a brace");
        assert_eq!(out, vec![r"foo\{not a brace"]);
    }

    #[test]
    fn brace_expansion_no_braces_passthrough() {
        let out = expand_braces("plain/path");
        assert_eq!(out, vec!["plain/path"]);
    }

    #[test]
    fn star_matches_one_segment() {
        let p = Pattern::compile_one("/System/*.txt", false).unwrap();
        assert!(p.is_match("/System/Notes.txt"));
        assert!(!p.is_match("/System/Sub/Notes.txt"));
    }

    #[test]
    fn globstar_matches_across_segments() {
        let p = Pattern::compile_one("/System/**/*.txt", false).unwrap();
        assert!(p.is_match("/System/Notes.txt"));
        assert!(p.is_match("/System/Sub/Notes.txt"));
        assert!(p.is_match("/System/a/b/c/Notes.txt"));
        assert!(!p.is_match("/Other/Notes.txt"));
    }

    #[test]
    fn unanchored_matches_anywhere() {
        let p = Pattern::compile_one("*.txt", false).unwrap();
        assert!(p.is_match("/Notes.txt"));
        assert!(p.is_match("/System/Notes.txt"));
        assert!(p.is_match("/a/b/c.txt"));
    }

    #[test]
    fn case_insensitive_works() {
        let p = Pattern::compile_one("/SYSTEM/notes.txt", true).unwrap();
        assert!(p.is_match("/system/NOTES.TXT"));
        let p2 = Pattern::compile_one("/SYSTEM/notes.txt", false).unwrap();
        assert!(!p2.is_match("/system/NOTES.TXT"));
    }

    #[test]
    fn brace_expansion_with_glob() {
        let pats = compile_patterns("/Apps/*.{bin,exe}", false).unwrap();
        assert_eq!(pats.len(), 2);
        assert!(pats.iter().any(|p| p.is_match("/Apps/foo.bin")));
        assert!(pats.iter().any(|p| p.is_match("/Apps/foo.exe")));
    }
}
