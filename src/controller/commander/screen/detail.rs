//! File Info fields edited through text prompts, for views without the graphical editors (the TUI).

use super::{purpose, CommanderController, MetadataEdit};
use crate::controller::{Effect, TextSpec};

/// A File Info value a prompt can change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailField {
    /// Permission bits, in octal.
    Mode,
    /// `uid:gid`.
    Owner,
    /// `name=value`; an empty value removes the attribute.
    Xattr,
}

impl DetailField {
    fn purpose(self) -> &'static str {
        match self {
            DetailField::Mode => purpose::DETAIL_MODE,
            DetailField::Owner => purpose::DETAIL_OWNER,
            DetailField::Xattr => purpose::DETAIL_XATTR,
        }
    }

    pub(super) fn for_purpose(p: &str) -> Option<DetailField> {
        let all = [DetailField::Mode, DetailField::Owner, DetailField::Xattr];
        all.into_iter().find(|f| f.purpose() == p)
    }
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

fn validate_detail(spec: &TextSpec, text: &str) -> Result<(), String> {
    if text.trim().is_empty() {
        return Err(String::new());
    }
    match DetailField::for_purpose(spec.purpose) {
        Some(DetailField::Mode) => parse_mode(text).map(drop),
        Some(DetailField::Owner) => parse_owner(text).map(drop),
        Some(DetailField::Xattr) => parse_xattr(text).map(drop),
        None => Ok(()),
    }
}

impl CommanderController {
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
        let e = &d.entry;
        let (title, prompt, current) = match field {
            DetailField::Mode => (
                "Permissions",
                "New permissions (octal, e.g. 755):",
                e.mode.map(|m| format!("{:o}", m & 0o7777)),
            ),
            DetailField::Owner => (
                "Owner",
                "New owner (uid:gid, e.g. 0:0):",
                e.uid.zip(e.gid).map(|(u, g)| format!("{u}:{g}")),
            ),
            DetailField::Xattr => (
                "Extended attribute",
                "name=value (0x.. for hex; an empty value removes it):",
                None,
            ),
        };
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
