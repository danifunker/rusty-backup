//! File Info fields edited through text prompts, for views without the graphical editors (the TUI).
//!
//! Each field's prompt starts from the effective value (a staged edit over the on-disk one), its
//! validator is the rule the egui editors in `src/gui/metadata_editor.rs` run, and the answer is
//! staged as the same `MetadataEdit` they stage. The type/creator and ProDOS prompts offer the
//! dictionaries as suggestions a view can step through.

use super::{purpose, CommanderController, MetadataEdit};
use crate::controller::browser::metadata;
use crate::controller::{Effect, TextSpec};
use disk_core::entry::FileEntry;

/// A File Info value a prompt can change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailField {
    /// Permission bits, in octal.
    Mode,
    /// `uid:gid`.
    Owner,
    /// `name=value`; an empty value removes the attribute.
    Xattr,
    /// An HFS file's four-character type and creator, `TYPE CRTR`.
    TypeCreator,
    /// A ProDOS file type and aux type in hex, `TT AAAA`.
    ProdosType,
    /// The Mac dates, `YYYY-MM-DD HH:MM:SS` (UTC); `0` clears one.
    Created,
    Modified,
    Backup,
}

impl DetailField {
    const ALL: [DetailField; 8] = [
        DetailField::Mode,
        DetailField::Owner,
        DetailField::Xattr,
        DetailField::TypeCreator,
        DetailField::ProdosType,
        DetailField::Created,
        DetailField::Modified,
        DetailField::Backup,
    ];

    fn purpose(self) -> &'static str {
        match self {
            DetailField::Mode => purpose::DETAIL_MODE,
            DetailField::Owner => purpose::DETAIL_OWNER,
            DetailField::Xattr => purpose::DETAIL_XATTR,
            DetailField::TypeCreator => purpose::DETAIL_TYPE_CREATOR,
            DetailField::ProdosType => purpose::DETAIL_PRODOS,
            DetailField::Created => purpose::DETAIL_CREATED,
            DetailField::Modified => purpose::DETAIL_MODIFIED,
            DetailField::Backup => purpose::DETAIL_BACKUP,
        }
    }

    pub(super) fn for_purpose(p: &str) -> Option<DetailField> {
        Self::ALL.into_iter().find(|f| f.purpose() == p)
    }

    /// The field's name, for a hint or a window row.
    pub fn label(self) -> &'static str {
        match self {
            DetailField::Mode => "mode",
            DetailField::Owner => "owner",
            DetailField::Xattr => "xattr",
            DetailField::TypeCreator => "type/creator",
            DetailField::ProdosType => "ProDOS type",
            DetailField::Created => "created",
            DetailField::Modified => "modified",
            DetailField::Backup => "backup date",
        }
    }

    /// Whether `e` has this value to change (mode, owner and attributes are always offered).
    fn applies(self, e: &FileEntry) -> bool {
        match self {
            DetailField::Mode | DetailField::Owner | DetailField::Xattr => true,
            DetailField::TypeCreator => {
                e.is_file() && (e.type_code.is_some() || e.creator_code.is_some())
            }
            DetailField::ProdosType => e.prodos_file_type.is_some(),
            DetailField::Created | DetailField::Modified | DetailField::Backup => {
                e.mac_dates.is_some()
            }
        }
    }
}

/// The File Info entry's Mac and ProDOS values with staged edits applied, for display.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetailMetadata {
    pub type_creator: Option<metadata::TypeCreator>,
    pub prodos: Option<metadata::ProdosType>,
    /// Created, modified, backup as text (`None`: no date).
    pub dates: Option<[Option<String>; 3]>,
}

fn parse_mode(text: &str) -> Result<u32, String> {
    match u32::from_str_radix(text.trim(), 8) {
        Ok(m) if m <= 0o7777 => Ok(m),
        _ => Err("Enter 1 to 4 octal digits, at most 7777.".into()),
    }
}

fn parse_owner(text: &str) -> Result<(u32, u32), String> {
    let parsed = text.trim().split_once(':').and_then(|(u, g)| {
        let uid = u.trim().parse().ok()?;
        Some((uid, g.trim().parse().ok()?))
    });
    parsed.ok_or_else(|| "Enter the owner as uid:gid, e.g. 0:0.".into())
}

/// `name=value` -> the name and the value to set, or `None` to remove it.
fn parse_xattr(text: &str) -> Result<(String, Option<Vec<u8>>), String> {
    let Some((name, value)) = text.split_once('=') else {
        return Err("Enter name=value (an empty value removes it).".into());
    };
    let name = name.trim().to_string();
    if !disk_core::xattr::has_valid_namespace(&name) {
        return Err("The name needs a user. / trusted. / security. / system. prefix.".into());
    }
    if value.trim().is_empty() {
        return Ok((name, None));
    }
    disk_core::xattr::parse_value(value).map(|v| (name, Some(v)))
}

/// `TYPE CRTR`: two codes of 1 to 4 characters; a code with a trailing space keeps its column.
fn parse_type_creator(text: &str) -> Result<([u8; 4], [u8; 4]), String> {
    let bad = || "Enter the type and creator, e.g. TEXT ttxt (1 to 4 characters each).".to_string();
    let chars: Vec<char> = text.chars().collect();
    let (t, c) = if chars.len() == 9 && chars[4] == ' ' {
        let t: String = chars[..4].iter().collect();
        let c: String = chars[5..].iter().collect();
        (t, c)
    } else {
        let mut parts = text.split_whitespace();
        match (parts.next(), parts.next(), parts.next()) {
            (Some(t), Some(c), None) => (t.to_string(), c.to_string()),
            _ => return Err(bad()),
        }
    };
    let fits = |s: &str| !s.is_empty() && s.len() <= 4 && s.is_ascii();
    if !fits(t.as_str()) || !fits(c.as_str()) {
        return Err(bad());
    }
    Ok((
        metadata::encode_fourcc(t.as_str()),
        metadata::encode_fourcc(c.as_str()),
    ))
}

/// `TT AAAA`: the type byte and aux type in hex.
fn parse_prodos(text: &str) -> Result<(u8, u16), String> {
    let mut parts = text.split_whitespace();
    let parsed = match (parts.next(), parts.next(), parts.next()) {
        (Some(t), Some(a), None) => metadata::parse_prodos_type(t, a),
        _ => None,
    };
    parsed.ok_or_else(|| "Enter the type (2 hex digits) and aux type (4), e.g. 06 2000.".into())
}

fn parse_date(text: &str) -> Result<u32, String> {
    fs_mac::hfs_common::parse_mac_date_checked(text)
}

fn fourcc_text(code: [u8; 4]) -> String {
    String::from_utf8_lossy(&code[..]).to_string()
}

fn validate_detail(spec: &TextSpec, text: &str) -> Result<(), String> {
    if text.trim().is_empty() {
        return Err(String::new());
    }
    match DetailField::for_purpose(spec.purpose) {
        Some(DetailField::Mode) => parse_mode(text).map(drop),
        Some(DetailField::Owner) => parse_owner(text).map(drop),
        Some(DetailField::Xattr) => parse_xattr(text).map(drop),
        Some(DetailField::TypeCreator) => parse_type_creator(text).map(drop),
        Some(DetailField::ProdosType) => parse_prodos(text).map(drop),
        Some(DetailField::Created | DetailField::Modified | DetailField::Backup) => {
            parse_date(text).map(drop)
        }
        None => Ok(()),
    }
}

impl CommanderController {
    /// The fields File Info offers prompts for on its entry, in display order.
    pub fn detail_fields(&self) -> Vec<DetailField> {
        let Some(d) = self.detail.as_ref().filter(|d| d.editable) else {
            return Vec::new();
        };
        DetailField::ALL
            .into_iter()
            .filter(|f| f.applies(&d.entry))
            .collect()
    }

    /// The entry's type/creator, ProDOS type and Mac dates, staged edits applied.
    pub fn detail_metadata(&self) -> Option<DetailMetadata> {
        let d = self.detail.as_ref()?;
        let queue = &self.pane(d.side).queue;
        let e = &d.entry;
        let hfs = e.is_file() && (e.type_code.is_some() || e.creator_code.is_some());
        let dates = e.mac_dates.map(|_| {
            let (c, m, b) = metadata::hfs_dates(queue, e);
            [
                metadata::hfs_date_text(c),
                metadata::hfs_date_text(m),
                metadata::hfs_date_text(b),
            ]
        });
        Some(DetailMetadata {
            type_creator: hfs.then(|| metadata::hfs_type_creator(queue, e)),
            prodos: e.prodos_file_type.map(|_| metadata::prodos_type(queue, e)),
            dates,
        })
    }

    /// Values a view may step through in `field`'s prompt: (answer text, what it means).
    pub fn detail_suggestions(&self, field: DetailField) -> Vec<(String, String)> {
        match field {
            DetailField::TypeCreator => metadata::known_type_creators()
                .into_iter()
                .map(|(label, t, c)| (format!("{} {}", fourcc_text(t), fourcc_text(c)), label))
                .collect(),
            DetailField::ProdosType => {
                let aux = self
                    .detail_metadata()
                    .and_then(|m| m.prodos)
                    .map_or(0, |p| p.aux_type);
                metadata::prodos_type_choices()
                    .into_iter()
                    .map(|(t, label)| (format!("{t:02X} {aux:04X}"), label))
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    /// The suggestion list a prompt `purpose` steps through (empty for most fields).
    pub fn suggestions_for(&self, purpose: &str) -> Vec<(String, String)> {
        DetailField::for_purpose(purpose)
            .map(|f| self.detail_suggestions(f))
            .unwrap_or_default()
    }

    /// The current value of `field` as its prompt shows it.
    fn detail_current(&self, field: DetailField) -> Option<String> {
        let d = self.detail.as_ref()?;
        let e = &d.entry;
        let meta = self.detail_metadata();
        let date = |i: usize| -> Option<String> {
            let dates = meta.as_ref()?.dates.clone()?;
            Some(dates[i].clone().unwrap_or_else(|| "0".to_string()))
        };
        match field {
            DetailField::Mode => e.mode.map(|m| format!("{:o}", m & 0o7777)),
            DetailField::Owner => e.uid.zip(e.gid).map(|(u, g)| format!("{u}:{g}")),
            DetailField::Xattr => None,
            DetailField::TypeCreator => {
                let tc = meta?.type_creator?;
                Some(format!(
                    "{} {}",
                    fourcc_text(tc.type_code),
                    fourcc_text(tc.creator_code)
                ))
            }
            DetailField::ProdosType => {
                let p = meta?.prodos?;
                Some(format!("{:02X} {:04X}", p.type_byte, p.aux_type))
            }
            DetailField::Created => date(0),
            DetailField::Modified => date(1),
            DetailField::Backup => date(2),
        }
    }

    /// Ask for a new value of `field`; `again` re-asks with the refused text and why.
    pub(super) fn ask_detail(
        &mut self,
        field: DetailField,
        again: Option<String>,
        error: Option<String>,
    ) -> Vec<Effect> {
        let Some(d) = self.detail.as_mut() else {
            return Vec::new();
        };
        if !d.editable {
            d.result = Some("This entry is read-only here.".into());
            return Vec::new();
        }
        if !field.applies(&d.entry) {
            d.result = Some(format!("This entry has no {} to change.", field.label()));
            return Vec::new();
        }
        let (title, prompt) = match field {
            DetailField::Mode => ("Permissions", "New permissions (octal, e.g. 755):"),
            DetailField::Owner => ("Owner", "New owner (uid:gid, e.g. 0:0):"),
            DetailField::Xattr => (
                "Extended attribute",
                "name=value (0x.. for hex; an empty value removes it):",
            ),
            DetailField::TypeCreator => (
                "Type / creator",
                "Type and creator, 4 characters each (e.g. TEXT ttxt):",
            ),
            DetailField::ProdosType => (
                "ProDOS file type",
                "File type and aux type in hex (e.g. 06 2000):",
            ),
            DetailField::Created => ("Created", "Created (YYYY-MM-DD HH:MM:SS, UTC; 0 = none):"),
            DetailField::Modified => ("Modified", "Modified (YYYY-MM-DD HH:MM:SS, UTC; 0 = none):"),
            DetailField::Backup => (
                "Backup date",
                "Backed up (YYYY-MM-DD HH:MM:SS, UTC; 0 = none):",
            ),
        };
        let current = self.detail_current(field);
        vec![Effect::AskText(TextSpec {
            purpose: field.purpose(),
            title: title.into(),
            prompt: prompt.into(),
            initial: again.unwrap_or_else(|| current.unwrap_or_default()),
            ok_label: "Stage".into(),
            unchanged: String::new(),
            taken: Vec::new(),
            fold_case: false,
            forbidden: String::new(),
            error,
            validator: validate_detail,
        })]
    }

    /// The Mac dates edit with one of the three replaced, the other two kept as they are.
    fn dates_edit(&self, entry: FileEntry, slot: usize, secs: u32) -> MetadataEdit {
        let side = self.detail.as_ref().map_or(super::Side::Left, |d| d.side);
        let (c, m, b) = metadata::hfs_dates(&self.pane(side).queue, &entry);
        let mut dates = [c, m, b];
        dates[slot] = secs;
        MetadataEdit::Dates {
            entry,
            create: dates[0],
            modify: dates[1],
            backup: dates[2],
        }
    }

    /// Stage a prompt's answer on the File Info entry, re-asking when it does not parse.
    pub(super) fn detail_answered(&mut self, purpose: &str, text: Option<String>) -> Vec<Effect> {
        let (Some(field), Some(text)) = (DetailField::for_purpose(purpose), text) else {
            return Vec::new();
        };
        let Some(entry) = self.detail.as_ref().map(|d| d.entry.clone()) else {
            return Vec::new();
        };
        let edit = match field {
            DetailField::Mode => parse_mode(&text).map(|m| MetadataEdit::Permissions {
                // The drivers take the whole mode: keep the type bits, replace the permissions.
                mode: (entry.mode.unwrap_or(0) & !0o7777) | m,
                entry,
            }),
            DetailField::Owner => {
                parse_owner(&text).map(|(uid, gid)| MetadataEdit::Owner { entry, uid, gid })
            }
            DetailField::Xattr => parse_xattr(&text).map(|(name, value)| match value {
                Some(value) => MetadataEdit::SetXattr { entry, name, value },
                None => MetadataEdit::RemoveXattr { entry, name },
            }),
            DetailField::TypeCreator => {
                parse_type_creator(text.as_str()).map(|(type_code, creator_code)| {
                    MetadataEdit::TypeCreator {
                        entry,
                        type_code,
                        creator_code,
                    }
                })
            }
            DetailField::ProdosType => {
                parse_prodos(text.as_str()).map(|(type_byte, aux_type)| MetadataEdit::ProdosType {
                    entry,
                    type_byte,
                    aux_type,
                })
            }
            DetailField::Created => parse_date(text.as_str()).map(|s| self.dates_edit(entry, 0, s)),
            DetailField::Modified => {
                parse_date(text.as_str()).map(|s| self.dates_edit(entry, 1, s))
            }
            DetailField::Backup => parse_date(text.as_str()).map(|s| self.dates_edit(entry, 2, s)),
        };
        match edit {
            Ok(edit) => {
                self.stage_metadata(edit);
                Vec::new()
            }
            Err(e) => self.ask_detail(field, Some(text), Some(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_creator_and_prodos_text_rules() {
        assert_eq!(parse_type_creator("TEXT ttxt"), Ok((*b"TEXT", *b"ttxt")));
        assert_eq!(parse_type_creator("MPS  ttxt"), Ok((*b"MPS ", *b"ttxt")));
        assert_eq!(parse_type_creator("APPL ????"), Ok((*b"APPL", *b"????")));
        assert!(parse_type_creator("TEXTX ttxt").is_err());
        assert!(parse_type_creator("TEXT").is_err());
        assert_eq!(parse_prodos("06 2000"), Ok((6, 0x2000)));
        assert!(parse_prodos("6").is_err());
        assert!(parse_prodos("zz 0000").is_err());
        assert_eq!(parse_date("0"), Ok(0));
        assert!(parse_date("yesterday").is_err());
    }
}
