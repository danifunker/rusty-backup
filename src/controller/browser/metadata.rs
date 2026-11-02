//! The File Info metadata editors' rules: what a row shows (a staged edit over the on-disk value),
//! what the dictionaries offer, and how each field's text parses.
//!
//! The egui editors in `src/gui/metadata_editor.rs` draw these and stage the edit they return;
//! the rules here are the ones those editors always ran, so the staged edits are unchanged.

use disk_core::entry::FileEntry;
use disk_core::xattr::Xattr;
use fs_apple2::prodos_types as pt;
use fs_mac::hfs_common;

use crate::model::edit_queue::{EditQueue, StagedEdit};

/// An HFS file's effective type and creator, as codes and as text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeCreator {
    pub type_code: [u8; 4],
    pub creator_code: [u8; 4],
    pub type_text: String,
    pub creator_text: String,
    /// The dictionary's description of the pair, if it knows it.
    pub description: Option<&'static str>,
}

impl TypeCreator {
    /// Both codes are zero (no FInfo).
    pub fn is_blank(&self) -> bool {
        self.type_code == [0; 4] && self.creator_code == [0; 4]
    }
}

/// The type and creator a staged edit or the catalog gives `entry`.
pub fn hfs_type_creator(queue: &EditQueue, entry: &FileEntry) -> TypeCreator {
    let (t, c) = queue.resolved_hfs_type_creator(entry);
    TypeCreator {
        type_code: t,
        creator_code: c,
        type_text: String::from_utf8_lossy(&t).to_string(),
        creator_text: String::from_utf8_lossy(&c).to_string(),
        description: hfs_common::describe_type_creator(&t, &c),
    }
}

/// The type/creator dictionary as (label, type, creator), in the dictionary's order.
pub fn known_type_creators() -> Vec<(String, [u8; 4], [u8; 4])> {
    hfs_common::known_type_creators()
        .iter()
        .map(|e| {
            let label = format!("{}/{} - {}", e.type_str(), e.creator_str(), e.description);
            (label, e.type_code, e.creator_code)
        })
        .collect()
}

/// Typed text as a four-byte code, space-padded.
pub fn encode_fourcc(text: &str) -> [u8; 4] {
    hfs_common::encode_fourcc(text)
}

/// The effective ProDOS (type byte, aux type): a pending add, then a staged change, then disk.
pub fn resolved_prodos_type(queue: &EditQueue, entry: &FileEntry) -> (u8, u16) {
    for edit in queue.iter() {
        if let StagedEdit::AddFile {
            parent,
            name,
            prodos_type,
            prodos_aux,
            ..
        } = edit
        {
            let path = if parent.path == "/" {
                format!("/{name}")
            } else {
                format!("{}/{name}", parent.path)
            };
            if path != entry.path {
                continue;
            }
            return (prodos_type.unwrap_or(0), prodos_aux.unwrap_or(0));
        }
    }
    if let Some((t, a)) = queue.iter().rev().find_map(|e| match e {
        StagedEdit::SetProdosType {
            entry: e2,
            type_byte,
            aux_type,
        } if e2.path == entry.path => Some((*type_byte, *aux_type)),
        _ => None,
    }) {
        return (t, a);
    }
    (
        entry.prodos_file_type.unwrap_or(0),
        entry.aux_type.unwrap_or(0),
    )
}

/// A ProDOS file's effective type, with the registry's name for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProdosType {
    pub type_byte: u8,
    pub aux_type: u16,
    pub known: bool,
    pub abbr: &'static str,
    pub description: &'static str,
}

pub fn prodos_type(queue: &EditQueue, entry: &FileEntry) -> ProdosType {
    let (t, a) = resolved_prodos_type(queue, entry);
    ProdosType {
        type_byte: t,
        aux_type: a,
        known: pt::is_known_type(t),
        abbr: pt::type_abbr(t),
        description: pt::type_description(t),
    }
}

/// Every registered ProDOS type as (byte, `$XX ABR - description`).
pub fn prodos_type_choices() -> Vec<(u8, String)> {
    pt::all_types()
        .iter()
        .map(|(byte, info)| {
            let label = format!("${:02X} {} - {}", byte, info.abbr, info.description);
            (*byte, label)
        })
        .collect()
}

/// The type (2 hex digits) and aux (4 hex digits) fields, or `None` while either is not hex.
pub fn parse_prodos_type(type_text: &str, aux_text: &str) -> Option<(u8, u16)> {
    let t = u8::from_str_radix(type_text.trim(), 16).ok()?;
    let a = u16::from_str_radix(aux_text.trim(), 16).ok()?;
    Some((t, a))
}

/// (create, modify, backup) in Mac seconds: a staged edit, else the catalog, else zeros.
pub fn hfs_dates(queue: &EditQueue, entry: &FileEntry) -> (u32, u32, u32) {
    queue
        .pending_dates_for(&entry.path)
        .or(entry.mac_dates)
        .unwrap_or((0, 0, 0))
}

/// A Mac date as `YYYY-MM-DD HH:MM:SS` (UTC); `None` for zero (no date).
pub fn hfs_date_text(secs: u32) -> Option<String> {
    hfs_common::format_mac_date(secs)
}

/// The three date fields; the error names the first field that does not parse.
pub fn parse_hfs_dates(
    created: &str,
    modified: &str,
    backup: &str,
) -> Result<(u32, u32, u32), String> {
    let field = |label: &str, text: &str| {
        hfs_common::parse_mac_date_checked(text).map_err(|e| format!("{label}: {e}"))
    };
    Ok((
        field("Created", created)?,
        field("Modified", modified)?,
        field("Backup", backup)?,
    ))
}

/// The effective permission bits (low 12): a staged edit, else the entry's mode, else 0.
pub fn permissions(queue: &EditQueue, entry: &FileEntry) -> u32 {
    queue
        .pending_permissions_for(&entry.path)
        .or(entry.mode)
        .map(|m| m & 0o7777)
        .unwrap_or(0)
}

/// Octal permission text, at most 0o7777.
pub fn parse_permissions(text: &str) -> Option<u32> {
    u32::from_str_radix(text.trim(), 8)
        .ok()
        .filter(|m| *m <= 0o7777)
}

/// The effective (uid, gid): a staged edit, else the entry's, else (0, 0).
pub fn owner(queue: &EditQueue, entry: &FileEntry) -> (u32, u32) {
    queue
        .pending_owner_for(&entry.path)
        .or_else(|| Some((entry.uid?, entry.gid?)))
        .unwrap_or((0, 0))
}

pub fn parse_owner(uid: &str, gid: &str) -> Option<(u32, u32)> {
    uid.trim()
        .parse::<u32>()
        .ok()
        .zip(gid.trim().parse::<u32>().ok())
}

/// The on-disk attributes with pending sets and removes applied, sorted by name.
pub fn effective_xattrs(queue: &EditQueue, entry: &FileEntry, on_disk: &[Xattr]) -> Vec<Xattr> {
    let mut effective: Vec<Xattr> = on_disk.to_vec();
    for (name, val) in &queue.pending_xattrs_for(&entry.path) {
        effective.retain(|x| &x.name != name);
        if let Some(v) = val {
            effective.push(Xattr {
                name: name.clone(),
                value: v.clone(),
            });
        }
    }
    effective.sort_by(|a, b| a.name.cmp(&b.name));
    effective
}

/// An attribute value typed as text or `0x` hex.
pub fn parse_xattr_value(text: &str) -> Result<Vec<u8>, String> {
    disk_core::xattr::parse_value(text)
}

/// The name starts with `user.`, `trusted.`, `security.` or `system.`.
pub fn valid_xattr_name(name: &str) -> bool {
    disk_core::xattr::has_valid_namespace(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_rules() {
        assert_eq!(parse_prodos_type("06", "2000"), Some((6, 0x2000)));
        assert_eq!(parse_prodos_type("zz", "0"), None);
        assert_eq!(parse_permissions("755"), Some(0o755));
        assert_eq!(parse_permissions("17777"), None);
        assert_eq!(parse_owner(" 0", "20 "), Some((0, 20)));
        assert_eq!(parse_owner("root", "0"), None);
        let err = parse_hfs_dates("", "bad", "").unwrap_err();
        assert!(err.starts_with("Modified: "), "{err}");
        assert_eq!(parse_hfs_dates("", "", ""), Ok((0, 0, 0)));
        assert!(valid_xattr_name("user.x") && !valid_xattr_name("x"));
    }

    #[test]
    fn staged_values_win_over_the_entry() {
        let mut e = FileEntry::new_file("f".into(), "/f".into(), 1, 2);
        e.mode = Some(0o100644);
        e.uid = Some(5);
        e.gid = Some(6);
        let q = EditQueue::new();
        assert_eq!(permissions(&q, &e), 0o644);
        assert_eq!(owner(&q, &e), (5, 6));
        assert_eq!(hfs_dates(&q, &e), (0, 0, 0));
        assert_eq!(resolved_prodos_type(&q, &e), (0, 0));
    }
}
