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
    /// The user chose "Connect to Remote..." — open the remote file-browser
    /// window. The connection details are gathered by that window, not here.
    Remote,
    /// A recently-opened path was picked from the "Recent" group. The raw path
    /// is returned un-materialized; the caller routes it (image / archive /
    /// backup folder) exactly as it would a drag-and-drop, so a mixed recent
    /// list (Commander's is merged across modes) opens correctly.
    Recent(PathBuf),
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
    /// Offer "Connect to Remote…" (browse an `rb-cli serve` daemon in a
    /// file-browser window).
    pub show_remote: bool,
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
    /// Render the "Recent" group as this many **fixed** rows — the newest
    /// entries fill the slots and any remainder shows as dimmed placeholders —
    /// so the row count (and thus the popup height) never changes as files are
    /// opened. This sidesteps a known egui bug where a ComboBox popup caches its
    /// height on first open and never regrows when its content later gets taller
    /// (emilk/egui#5225, #5138). `0` keeps the old behavior: show every recent
    /// entry, variable height (used by Commander, whose merged list is short).
    pub recent_slots: usize,
}

/// Current selection highlight state, so the open entries render "active" and
/// the device list shows which row is selected.
pub struct PickerState<'a> {
    pub selected_device_idx: Option<usize>,
    pub image_active: bool,
    pub host_active: bool,
    pub backup_active: bool,
    pub devices: &'a [DiskDevice],
    /// Recently-opened paths for this mode (newest first). Rendered as a
    /// "Recent" group at the **top** of the dropdown (so it's visible without
    /// scrolling past a long device list); a click emits [`SourceEvent::Recent`].
    /// Empty to omit the group.
    pub recent: &'a [String],
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
    // Grow the dropdown to the room actually available in the window instead of
    // a fixed cap. egui uses `.height()` as the max height of the popup's inner
    // scroll area; a fixed 400px scrolled the moment recents + devices + open
    // actions exceeded it (and plugging in more devices only made that worse).
    // We size it to the space below the button — or above, whichever is larger,
    // since egui flips the popup up when the button sits near the bottom — so
    // every row shows when the window can fit it, and a scrollbar only appears
    // when the window itself is genuinely too short. (Leaving `.height()` unset
    // would fall back to egui's even smaller 200px default, not "uncapped".)
    let window = ui.ctx().content_rect();
    let button_top = ui.cursor().top();
    let room_below = window.bottom() - button_top - ui.spacing().interact_size.y - 16.0;
    let room_above = button_top - window.top() - 16.0;
    let popup_max_height = room_below.max(room_above).max(120.0);
    egui::ComboBox::from_id_salt(id_salt)
        .selected_text(current_label)
        .width(cfg.width)
        .height(popup_max_height)
        .show_ui(ui, |ui| {
            // Recent files first, so they're visible without scrolling past a
            // long device list. A click routes back through the caller (image /
            // archive / backup folder), un-materialized.
            //
            // When `recent_slots` is set we always draw that many rows (newest
            // entries fill them, the rest are dimmed placeholders) so the popup's
            // height is constant as files are opened — see `recent_slots` for the
            // egui bug this dodges. `0` falls back to a plain variable-length list.
            let render_recent_row =
                |ui: &mut egui::Ui, event: &mut Option<SourceEvent>, p: &str| {
                    let label = std::path::Path::new(p)
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_else(|| p.to_string());
                    if ui.selectable_label(false, label).on_hover_text(p).clicked() {
                        *event = Some(SourceEvent::Recent(PathBuf::from(p)));
                    }
                };
            if cfg.recent_slots > 0 {
                ui.label(egui::RichText::new("Recent").weak());
                for slot in 0..cfg.recent_slots {
                    match state.recent.get(slot) {
                        Some(p) => render_recent_row(ui, &mut event, p),
                        // Dimmed, non-interactive placeholder — holds the row so
                        // the list height doesn't change when it later fills in.
                        None => {
                            ui.add_enabled(
                                false,
                                egui::Button::selectable(false, egui::RichText::new("-").weak()),
                            );
                        }
                    }
                }
                ui.separator();
            } else if !state.recent.is_empty() {
                ui.label(egui::RichText::new("Recent").weak());
                for p in state.recent {
                    render_recent_row(ui, &mut event, p);
                }
                ui.separator();
            }
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
            if cfg.show_remote {
                if cfg.show_image || cfg.show_host_folder || cfg.show_backup_folder {
                    ui.separator();
                }
                if ui
                    .selectable_label(false, "Connect to Remote...")
                    .on_hover_text("Browse a remote rb-cli serve daemon's filesystem")
                    .clicked()
                {
                    event = Some(SourceEvent::Remote);
                }
            }
        });
    event
}
