//! Shared source-selection picker (R1 of the Commander Mode plan, see
//! `docs/commander_mode.md` §3).
//!
//! A single `egui::ComboBox` widget that offers a configurable mix of source
//! entries — physical devices, "Open File…" (a disk image / container),
//! "Open Folder…" (a host directory), and "Open Backup Folder…" (a
//! rusty-backup backup) — and emits a [`SourceEvent`] describing the user's
//! choice. The widget owns the rfd file/folder dialogs and the disk-image
//! filter set + materialization, so the Inspect tab and each Commander pane
//! select sources through the same code rather than two divergent copies.
//!
//! Callers translate the emitted event into their own state (Inspect updates
//! its `selected_device_idx` / `image_file_path` / `backup_folder_path`;
//! Commander loads the pane source or stages a deferred switch). The widget is
//! pure rendering + dialogs — it holds no state of its own.

use std::path::PathBuf;

use rusty_backup::device::DiskDevice;

/// The user's source choice, emitted by [`show`]. Each variant corresponds to a
/// selectable entry the caller enabled via [`PickerConfig`].
pub enum SourceEvent {
    /// A physical device was picked (index into the `devices` slice).
    Device(usize),
    /// A disk image / container file was opened. `tempdir` holds the guard for
    /// a materialized wrapper format (e.g. `.adz` → `.adf`); `None` when the
    /// path was passed through unchanged.
    Image {
        path: PathBuf,
        tempdir: Option<tempfile::TempDir>,
    },
    /// A host folder was opened (to browse as a pane source).
    HostFolder(PathBuf),
    /// A rusty-backup backup folder was opened.
    BackupFolder(PathBuf),
}

/// Which entries the picker offers.
pub struct PickerConfig {
    /// List the physical devices passed in [`PickerState::devices`].
    pub show_devices: bool,
    /// Offer "Open File…" (a disk image / container).
    pub show_image: bool,
    /// Offer "Open Folder…" (a host directory).
    pub show_host_folder: bool,
    /// Offer "Open Backup Folder…" (a rusty-backup backup).
    pub show_backup_folder: bool,
    /// Materialize wrapper image formats (`.adz`/`.hdz`/…) to a tempfile via
    /// `prepare_disk_image_path`. Inspect sets this; Commander leaves it off
    /// and lets `BrowseSession` peel the container itself.
    pub materialize_image: bool,
    /// Add the "Mac archives" (`.sit`/`.hqx`/…) filter group to the image file
    /// dialog. Inspect can inspect those; the Commander panes (disk browsing
    /// only) leave it off so non-disk archives don't clutter the picker.
    pub include_mac_archives: bool,
    /// ComboBox width.
    pub width: f32,
}

/// Current selection highlight state, so the open entries render "active" and
/// the device list shows which row is selected.
pub struct PickerState<'a> {
    pub selected_device_idx: Option<usize>,
    pub image_active: bool,
    pub host_active: bool,
    pub backup_active: bool,
    pub devices: &'a [DiskDevice],
}

/// Run the disk-image / container file dialog (Disk Images + Mac archives +
/// All Files filters). When `materialize` is set, wrapper formats are decoded
/// to a tempfile via `prepare_disk_image_path` (the guard rides along in the
/// returned tuple). Returns `None` if the user cancels. Shared by the picker's
/// "Open File…" entry and Commander's "Open…" button.
pub fn pick_image_file(
    materialize: bool,
    include_mac_archives: bool,
) -> Option<(PathBuf, Option<tempfile::TempDir>)> {
    let mut dialog = super::file_dialog().add_filter(
        "Disk Images",
        rusty_backup::model::file_types::DISK_IMAGE_EXTS,
    );
    if include_mac_archives {
        dialog = dialog.add_filter(
            "Mac archives",
            rusty_backup::model::file_types::MAC_ARCHIVE_EXTS,
        );
    }
    let path = dialog.add_filter("All Files", &["*"]).pick_file()?;
    if !materialize {
        return Some((path, None));
    }
    // Transparently decompress .adz / .hdz (and other wrappers) so the rest of
    // the pipeline sees a raw image. Floppy containers are NOT decoded here
    // (false) — they open directly so edits persist back into the container.
    match super::prepare_disk_image_path(&path, false) {
        Ok((materialized, guard)) => Some((materialized, guard)),
        Err(e) => {
            log::error!("Failed to decompress {}: {}", path.display(), e);
            Some((path, None))
        }
    }
}

/// Render the source ComboBox and return the user's choice, if any. `id_salt`
/// keys the ComboBox (Commander draws two panes in one `Ui`, so each must pass
/// a distinct salt). `current_label` is the closed-state text.
pub fn show(
    ui: &mut egui::Ui,
    id_salt: &str,
    cfg: &PickerConfig,
    current_label: &str,
    state: &PickerState,
) -> Option<SourceEvent> {
    let mut event = None;
    egui::ComboBox::from_id_salt(id_salt)
        .selected_text(current_label)
        .width(cfg.width)
        .height(400.0) // Show many devices without scrolling.
        .show_ui(ui, |ui| {
            if cfg.show_devices {
                for (i, device) in state.devices.iter().enumerate() {
                    let selected = state.selected_device_idx == Some(i);
                    if ui
                        .selectable_label(selected, device.display_name())
                        .clicked()
                    {
                        event = Some(SourceEvent::Device(i));
                    }
                }
                if cfg.show_image || cfg.show_host_folder || cfg.show_backup_folder {
                    ui.separator();
                }
            }
            if cfg.show_image
                && ui
                    .selectable_label(state.image_active, "Open File...")
                    .clicked()
            {
                if let Some((path, tempdir)) =
                    pick_image_file(cfg.materialize_image, cfg.include_mac_archives)
                {
                    event = Some(SourceEvent::Image { path, tempdir });
                }
            }
            if cfg.show_host_folder
                && ui
                    .selectable_label(state.host_active, "Open Local Folder...")
                    .on_hover_text("Browse a folder on this computer (not a backup)")
                    .clicked()
            {
                if let Some(dir) = super::file_dialog().pick_folder() {
                    event = Some(SourceEvent::HostFolder(dir));
                }
            }
            if cfg.show_backup_folder
                && ui
                    .selectable_label(state.backup_active, "Open Backup Folder...")
                    .clicked()
            {
                if let Some(dir) = super::file_dialog().pick_folder() {
                    event = Some(SourceEvent::BackupFolder(dir));
                }
            }
        });
    event
}
