//! Every controller's label table in one place (D8, Phase 2b): what each shell calls a control.
//!
//! A flow driver that finds controls by visible text (the TUI, GTK) reads this through
//! `rb-cli ui-labels`; shells that set the id as the accessibility identifier do not need it.

/// Every `(id, label)` pair, each controller's table in turn.
pub fn table() -> Vec<(&'static str, &'static str)> {
    let mut parts: Vec<&[(&str, &str)]> = vec![
        super::backup::labels::TABLE,
        super::restore::labels::TABLE,
        super::expand_hfs::labels::TABLE,
        super::build_disk::labels::TABLE,
    ];
    #[cfg(feature = "optical")]
    parts.push(super::optical::labels::TABLE);
    parts.iter().flat_map(|t| t.iter().copied()).collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn ids_are_unique() {
        let t = super::table();
        let mut ids: Vec<&str> = t.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), t.len());
    }
}
