//! `FileEntry` lives in the `disk-core` crate; this adds the app's ProDOS-aware type display.

pub use disk_core::entry::*;

/// Render a file type for display (`ls`, GUI, remote browse), resolved
/// per-filesystem: a Mac `OSType` lossily (non-ASCII → `.`), a ProDOS type as
/// `$XX`. `None` when neither is present. Shared by [`FileEntry`] and the
/// remote `WireEntry` so the display rule lives in exactly one place.
pub fn display_file_type(
    os_type: Option<&[u8; 4]>,
    prodos_file_type: Option<u8>,
) -> Option<String> {
    if let Some(code) = os_type {
        return Some(crate::fs::hfs_common::decode_ostype(code));
    }
    if let Some(byte) = prodos_file_type {
        return Some(crate::fs::prodos_types::format_type_code(byte));
    }
    None
}

/// Type-code display for [`FileEntry`]; app-side because ProDOS names come from a user-overridable table.
pub trait FileEntryTypeDisplay {
    /// Display-only type string; callers that need the exact bytes read `type_code` directly.
    fn type_code_display(&self) -> Option<String>;
}

impl FileEntryTypeDisplay for FileEntry {
    fn type_code_display(&self) -> Option<String> {
        display_file_type(self.type_code.as_ref(), self.prodos_file_type)
    }
}
