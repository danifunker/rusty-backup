//! The flow suite's answer to a file or folder picker (`regression-tests/FLOWS.md`).
//!
//! When `RUSTY_BACKUP_TEST_PICKS` names a file, every shell's effect host asks [`scripted`] before
//! it opens a real dialog. The file holds `purpose<TAB>path` lines; `*` answers any purpose and an
//! unlisted purpose cancels. It is read on every pick, so a suite can change it between steps.

use std::path::PathBuf;

/// The environment variable naming the picks file.
pub const ENV: &str = "RUSTY_BACKUP_TEST_PICKS";

/// `None` when no script is set (open the real dialog); else the scripted answer, `None` to cancel.
pub fn scripted(purpose: &str) -> Option<Option<PathBuf>> {
    let file = std::env::var_os(ENV).filter(|f| !f.is_empty())?;
    let text = std::fs::read_to_string(file).unwrap_or_default();
    Some(answer(&text, purpose))
}

/// The answer a picks file's `text` gives for `purpose`.
pub fn answer(text: &str, purpose: &str) -> Option<PathBuf> {
    text.lines().find_map(|l| {
        let (p, path) = l.split_once('\t')?;
        (p == purpose || p == "*").then(|| PathBuf::from(path.trim_end_matches('\r')))
    })
}

#[cfg(test)]
mod tests {
    use super::answer;
    use std::path::PathBuf;

    #[test]
    fn purpose_then_star_then_cancel() {
        let text = "backup.dest\t/a\n*\t/b\r\n";
        assert_eq!(answer(text, "backup.dest"), Some(PathBuf::from("/a")));
        assert_eq!(answer(text, "other"), Some(PathBuf::from("/b")));
        assert_eq!(answer("x\t/c\n", "other"), None);
        assert_eq!(answer("", "x"), None);
    }
}
