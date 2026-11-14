//! The TUI Backup screen: source -> options -> run over a headless `BackupController`.
//!
//! The same controller drives the egui Backup tab, so the screen offers what the tab does: a
//! device, image or remote-drive source, Load partition info, the partition grid (include, min
//! size, Calc min, fragmentation, Compact Space), the options form (format, CHD codecs,
//! sector-by-sector, resize, keep swap, split, checksum), the VHD window and Start with the
//! controller's waits. The form's rows are a list of [`Field`]s; keys act on the focused one.
//! See docs/phase2_commander_plan.md § "Slice 24".

use std::sync::{Arc, Mutex};

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Clear, Paragraph, Wrap};
use ratatui::Frame;

use super::{
    centered_rect, expand_tilde, hint, App, FilePicker, KeyHint, PickKind, PickResult, Progress,
    ProgressShared,
};
#[cfg(feature = "chd")]
use crate::controller::app::chd::{
    codec_choices, codec_label, hunk_presets, modes_for, unit_size_for, ChdOptionsControl,
    ChdOptionsMode, ChdProfile,
};
use crate::controller::app::{RateTracker, RecentHandle, RecentMode};
use crate::controller::backup::{
    format_size, labels, BackupAction, BackupController, BackupServices, ChecksumType, CompactCell,
    CompressionType, HostRequest, JobRegistry, MinCell, NoNotify, SizeMode, SPLIT_MIB_MAX,
    SPLIT_MIB_MIN,
};
use crate::controller::Controller as _;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum BackupStep {
    Source,
    Config,
    Run,
}

/// Output formats offered on the form (label -> compression).
const BACKUP_FORMATS: &[(&str, CompressionType)] = &[
    ("Zstd", CompressionType::Zstd),
    ("CHD (single-file)", CompressionType::Chd),
    ("VHD", CompressionType::Vhd),
    ("gzip", CompressionType::Gzip),
    ("LZ4", CompressionType::Lz4),
    ("Raw (uncompressed)", CompressionType::None),
];

/// Checksum choices on the form (label -> algorithm).
const BACKUP_CHECKSUMS: &[(&str, ChecksumType)] = &[
    ("SHA-256", ChecksumType::Sha256),
    ("CRC32", ChecksumType::Crc32),
];

/// One row of the options form.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Field {
    /// A partition of the grid, by its index.
    Partition(usize),
    Output,
    Name,
    Format,
    ChdMode,
    ChdHunk,
    ChdCodec(usize),
    SectorBySector,
    Resize,
    KeepSwap,
    Split,
    SplitMib,
    Checksum,
    Start,
}

impl Field {
    /// Typed into: letter shortcuts do not act here.
    fn is_text(self) -> bool {
        matches!(self, Field::Output | Field::Name | Field::SplitMib)
    }
}

pub(super) const HINTS_BACKUP_SOURCE: &[KeyHint] = &[
    hint(
        "Enter",
        "Continue to the options (or choose an image when none is set)",
    ),
    hint("o", "Choose a source image file"),
    hint("d", "Choose a physical disk"),
    hint("r", "Remote drive on an rb-cli serve daemon"),
    hint("c", "Close the source (release the disk / disconnect)"),
];

pub(super) const HINTS_BACKUP_CONFIG: &[KeyHint] = &[
    hint("Up/Down", "Move between rows  (Shift-Tab goes up)"),
    hint("Space", "Toggle the row, or the partition's Include"),
    hint("Left/Right", "Change a choice (format, CHD, checksum)"),
    hint("Tab", "Browse for the output folder"),
    hint("a / n", "Include all / no partitions"),
    hint("c", "Calc min on the partition row"),
    hint("x", "Compact Space on the partition row"),
    hint("p", "Load (or reload) partition info of a disk"),
    hint("s", "Start the backup  (also Enter on Start)"),
    hint("Esc", "Back to the source (stops a partition scan first)"),
];

pub(super) const HINTS_BACKUP_VHD: &[KeyHint] = &[
    hint("Up/Down", "Move between rows"),
    hint("Left/Right", "Change the layout or a partition's size mode"),
    hint("0-9", "Type a Custom size in MiB  (Backspace deletes)"),
    hint("c", "Calc min on the partition row"),
    hint("Enter", "Start the VHD backup on Start"),
    hint("Esc", "Close the window"),
];

pub(super) const HINTS_BACKUP_REMOTE: &[KeyHint] = &[
    hint("Enter", "Connect, or back up the selected drive"),
    hint("Tab", "Switch between the host field and the drive list"),
    hint("Up/Down", "Select a drive"),
    hint("Esc", "Close the window"),
];

pub(super) const HINTS_BACKUP_RUN: &[KeyHint] = &[hint(
    "Esc",
    "Cancel the run (or stop waiting); back to the options once it ended",
)];

pub(super) struct BackupState {
    pub(super) step: BackupStep,
    /// The app's recent-files store; a successful open is recorded through it.
    recent: RecentHandle,
    pub(super) ctl: BackupController,
    /// Source image or output folder picker, when open.
    picker: Option<FilePicker>,
    /// Physical-disk chooser sub-mode + its cursor.
    device_pick: bool,
    device_sel: usize,
    /// The typed output folder (the controller holds the expanded path).
    pub(super) out_dir: String,
    pub(super) focus: Field,
    /// Latest operation string from the run (shown during Run).
    op: String,
    /// The controller's latest log lines.
    pub(super) log: Vec<String>,
    /// Esc asked the running backup to stop.
    pub(super) cancel_note: bool,
    /// The CHD options control (the GUI's "Advanced" row); its choice travels with Start.
    #[cfg(feature = "chd")]
    pub(super) chd: ChdOptionsControl,
    /// Why Start was refused by the form's own check (CHD options).
    note: Option<String>,
    /// The remote window's focus is the drive list (else the host field), and its cursor.
    remote_list: bool,
    remote_sel: usize,
    remote_connecting: bool,
    /// The VHD window's cursor: 0 the layout, 1..=n the rows, n + 1 Start.
    pub(super) vhd_sel: usize,
}

impl BackupState {
    pub(super) fn new(recent: RecentHandle) -> Self {
        let ctl = BackupController::new(BackupServices {
            jobs: JobRegistry::new(),
            notify: Arc::new(NoNotify),
        });
        BackupState {
            step: BackupStep::Source,
            recent,
            ctl,
            picker: None,
            device_pick: false,
            device_sel: 0,
            out_dir: String::new(),
            focus: Field::Output,
            op: String::new(),
            log: Vec::new(),
            cancel_note: false,
            #[cfg(feature = "chd")]
            chd: {
                let mut c = ChdOptionsControl::new(ChdProfile::Hd);
                // The last Custom codecs and hunk size wait in the Custom slot, as in the GUI.
                c.custom = BackupController::saved_chd_custom(c.custom.clone());
                c
            },
            note: None,
            remote_list: false,
            remote_sel: 0,
            remote_connecting: false,
            vhd_sel: 0,
        }
    }

    pub(super) fn act(&mut self, action: BackupAction) {
        // The controller's pickers are the TUI's own file picker, opened by key.
        let _ = self.ctl.apply(action);
        self.after();
    }

    /// Carry out the controller's requests and keep its latest log lines.
    fn after(&mut self) {
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(path) => {
                    self.recent
                        .remember(RecentMode::Backup, path.to_string_lossy().as_ref());
                }
            }
        }
        self.log
            .extend(self.ctl.take_log().into_iter().map(|(_, msg)| msg));
        let n = self.log.len();
        if n > 6 {
            self.log.drain(..n - 6);
        }
    }

    /// A backup runs or waits to start.
    pub(super) fn busy(&self) -> bool {
        self.ctl.is_running() || self.ctl.start_pending()
    }

    /// How the run ended (text, is an error); `None` while it runs.
    pub(super) fn result(&self) -> Option<(String, bool)> {
        if self.busy() {
            return self
                .cancel_note
                .then(|| ("Cancel requested...".to_string(), true));
        }
        match self.ctl.last_result()? {
            Ok(()) => Some((labels::DONE.to_string(), false)),
            Err(e) => Some((e.clone(), true)),
        }
    }

    fn source_text(&self) -> String {
        if let Some(label) = self.ctl.remote_label() {
            return format!("Remote: {label}");
        }
        self.ctl
            .source_path()
            .map(|p| p.display().to_string())
            .unwrap_or_default()
    }

    fn format_index(&self) -> usize {
        let c = self.ctl.options().compression;
        BACKUP_FORMATS.iter().position(|f| f.1 == c).unwrap_or(0)
    }

    fn checksum_index(&self) -> usize {
        let c = self.ctl.options().checksum;
        BACKUP_CHECKSUMS.iter().position(|f| f.1 == c).unwrap_or(0)
    }

    fn chd_shown(&self) -> bool {
        cfg!(feature = "chd") && self.ctl.options().compression == CompressionType::Chd
    }

    #[cfg(feature = "chd")]
    fn chd_custom(&self) -> bool {
        self.chd.mode == ChdOptionsMode::Custom
    }

    #[cfg(not(feature = "chd"))]
    fn chd_custom(&self) -> bool {
        false
    }

    /// The form's rows, in drawing order (they follow the format and what was found).
    pub(super) fn fields(&self) -> Vec<Field> {
        let opts = self.ctl.options();
        let mut f: Vec<Field> = self
            .ctl
            .rows()
            .iter()
            .map(|r| Field::Partition(r.index))
            .collect();
        f.extend([Field::Output, Field::Name, Field::Format]);
        if self.chd_shown() {
            f.push(Field::ChdMode);
            if self.chd_custom() {
                f.push(Field::ChdHunk);
                f.extend((0..4).map(Field::ChdCodec));
            }
        }
        f.extend([Field::SectorBySector, Field::Resize, Field::KeepSwap]);
        if opts.split_shown() {
            f.push(Field::Split);
            if opts.split && opts.split_editable() {
                f.push(Field::SplitMib);
            }
        }
        f.extend([Field::Checksum, Field::Start]);
        f
    }

    fn move_focus(&mut self, delta: isize) {
        let fields = self.fields();
        let n = fields.len() as isize;
        let at = fields
            .iter()
            .position(|f| *f == self.focus)
            .unwrap_or_else(|| fields.iter().position(|f| *f == Field::Output).unwrap_or(0));
        self.focus = fields[((at as isize + delta).rem_euclid(n)) as usize];
    }

    /// Space / Left / Right on a choice or toggle row; `false` when the row has none.
    fn change(&mut self, field: Field, forward: bool) -> bool {
        let opts = self.ctl.options().clone();
        let step = |i: usize, n: usize| {
            if forward {
                (i + 1) % n
            } else {
                (i + n - 1) % n
            }
        };
        match field {
            Field::Format => {
                let i = step(self.format_index(), BACKUP_FORMATS.len());
                self.act(BackupAction::SetFormat(BACKUP_FORMATS[i].1));
            }
            Field::Checksum => {
                let i = step(self.checksum_index(), BACKUP_CHECKSUMS.len());
                self.act(BackupAction::SetChecksum(BACKUP_CHECKSUMS[i].1));
            }
            Field::SectorBySector => {
                self.act(BackupAction::SetSectorBySector(!opts.sector_by_sector))
            }
            Field::Resize if opts.size_options_editable() => {
                self.act(BackupAction::SetResize(!opts.resize))
            }
            Field::KeepSwap if opts.size_options_editable() => {
                self.act(BackupAction::SetKeepSwap(!opts.keep_swap))
            }
            Field::Split if opts.split_editable() => self.act(BackupAction::SetSplit(!opts.split)),
            Field::Partition(index) => {
                let on = self
                    .ctl
                    .rows()
                    .iter()
                    .any(|r| r.index == index && r.selected);
                self.act(BackupAction::SetPartition(index, !on));
            }
            Field::ChdMode | Field::ChdHunk | Field::ChdCodec(_) => {
                return self.change_chd(field, forward)
            }
            _ => return false,
        }
        true
    }

    #[cfg(feature = "chd")]
    fn change_chd(&mut self, field: Field, forward: bool) -> bool {
        fn next<T: Copy + PartialEq>(list: &[T], cur: T, forward: bool) -> T {
            let n = list.len();
            match list.iter().position(|x| *x == cur) {
                Some(i) if forward => list[(i + 1) % n],
                Some(i) => list[(i + n - 1) % n],
                None => list[0],
            }
        }
        match field {
            Field::ChdMode => {
                self.chd.mode = next(modes_for(ChdProfile::Hd).as_slice(), self.chd.mode, forward)
            }
            Field::ChdHunk => {
                let cur = self.chd.custom.hunk_size;
                self.chd.custom.hunk_size = next(hunk_presets(ChdProfile::Hd), cur, forward);
            }
            Field::ChdCodec(slot) => {
                let cur = self.chd.custom.codecs[slot];
                let choices = codec_choices(ChdProfile::Hd);
                self.chd.custom.codecs[slot] = next(choices.as_slice(), cur, forward);
            }
            _ => return false,
        }
        self.note = None;
        true
    }

    #[cfg(not(feature = "chd"))]
    fn change_chd(&mut self, _field: Field, _forward: bool) -> bool {
        false
    }

    /// The typed output folder; the controller gets it at once (`~` expanded).
    pub(super) fn set_out_dir(&mut self, text: String) {
        let path = expand_tilde(text.trim());
        let dest = (!path.as_os_str().is_empty()).then_some(path);
        self.out_dir = text;
        self.act(BackupAction::SetDestination(dest));
    }

    fn type_char(&mut self, c: char) {
        match self.focus {
            Field::Output => {
                let mut dir = self.out_dir.clone();
                dir.push(c);
                self.set_out_dir(dir);
            }
            Field::Name => {
                let mut name = self.ctl.name().to_string();
                name.push(c);
                self.act(BackupAction::SetName(name));
            }
            Field::SplitMib => {
                if let Some(d) = c.to_digit(10) {
                    let mib = self
                        .ctl
                        .options()
                        .split_mib
                        .saturating_mul(10)
                        .saturating_add(d);
                    self.act(BackupAction::SetSplitMib(mib));
                }
            }
            _ => {}
        }
    }

    fn backspace(&mut self) {
        match self.focus {
            Field::Output => {
                let mut dir = self.out_dir.clone();
                dir.pop();
                self.set_out_dir(dir);
            }
            Field::Name => {
                let mut name = self.ctl.name().to_string();
                name.pop();
                self.act(BackupAction::SetName(name));
            }
            Field::SplitMib => {
                let mib = self.ctl.options().split_mib / 10;
                self.act(BackupAction::SetSplitMib(mib));
            }
            _ => {}
        }
    }

    /// The CHD options Start carries, or why they cannot be used.
    #[cfg(feature = "chd")]
    fn chd_choice(&self) -> Result<Option<crate::controller::backup::ChdChoice>, String> {
        if self.ctl.options().compression != CompressionType::Chd {
            return Ok(None);
        }
        self.chd
            .validate(ChdProfile::Hd)
            .map_err(|e| format!("CHD options: {e}."))?;
        Ok(Some(crate::controller::backup::ChdChoice {
            options: self.chd.effective(ChdProfile::Hd),
            custom: self.chd_custom(),
        }))
    }

    #[cfg(not(feature = "chd"))]
    fn chd_choice(&self) -> Result<Option<crate::controller::backup::ChdChoice>, String> {
        Ok(None)
    }

    /// The VHD window's rows after the layout row.
    fn vhd_len(&self) -> usize {
        self.ctl.vhd_rows().len()
    }

    fn vhd_key(&mut self, code: KeyCode) {
        let rows = self.ctl.vhd_rows().to_vec();
        let n = rows.len();
        let row = self
            .vhd_sel
            .checked_sub(1)
            .and_then(|i| rows.get(i))
            .cloned();
        match code {
            KeyCode::Esc => self.act(BackupAction::VhdClose),
            KeyCode::Up | KeyCode::BackTab => self.vhd_sel = (self.vhd_sel + n + 1) % (n + 2),
            KeyCode::Down | KeyCode::Tab => self.vhd_sel = (self.vhd_sel + 1) % (n + 2),
            KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') if self.vhd_sel == 0 => {
                let whole = self.ctl.vhd_whole_disk();
                self.act(BackupAction::VhdWholeDisk(!whole));
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') => {
                if let Some(row) = row {
                    let modes = row.mode_choices();
                    let i = modes.iter().position(|m| *m == row.mode).unwrap_or(0);
                    let m = modes.len();
                    let j = if code == KeyCode::Left {
                        (i + m - 1) % m
                    } else {
                        (i + 1) % m
                    };
                    self.act(BackupAction::VhdSize {
                        index: row.index,
                        mode: modes[j],
                        custom_mib: row.custom_mib,
                    });
                }
            }
            KeyCode::Char('c') => {
                if let Some(row) = row.filter(|r| r.deferred_fs.is_some()) {
                    self.act(BackupAction::CalcMin(row.index));
                }
            }
            KeyCode::Char(ch) if ch.is_ascii_digit() => {
                if let Some(row) = row.filter(|r| r.mode == SizeMode::Custom) {
                    let d = ch.to_digit(10).unwrap_or(0);
                    self.act(BackupAction::VhdSize {
                        index: row.index,
                        mode: SizeMode::Custom,
                        custom_mib: row.custom_mib.saturating_mul(10).saturating_add(d),
                    });
                }
            }
            KeyCode::Backspace => {
                if let Some(row) = row.filter(|r| r.mode == SizeMode::Custom) {
                    self.act(BackupAction::VhdSize {
                        index: row.index,
                        mode: SizeMode::Custom,
                        custom_mib: row.custom_mib / 10,
                    });
                }
            }
            KeyCode::Enter if self.vhd_sel == n + 1 => self.act(BackupAction::VhdStart),
            KeyCode::Enter => self.vhd_sel = (self.vhd_sel + 1) % (n + 2),
            _ => {}
        }
    }

    /// Keys of the Remote Drive window; `true` when a drive was picked.
    fn remote_key(&mut self, code: KeyCode) -> bool {
        let drives = self.ctl.remote_drives();
        if code == KeyCode::Esc {
            self.act(BackupAction::RemoteClose);
            self.remote_list = false;
            return false;
        }
        if self.remote_list && !drives.is_empty() {
            match code {
                KeyCode::Up => self.remote_sel = self.remote_sel.saturating_sub(1),
                KeyCode::Down => self.remote_sel = (self.remote_sel + 1).min(drives.len() - 1),
                KeyCode::Tab | KeyCode::BackTab => self.remote_list = false,
                KeyCode::Enter => {
                    let d = drives[self.remote_sel.min(drives.len() - 1)].clone();
                    self.remote_list = false;
                    self.act(BackupAction::RemotePick {
                        path: d.path,
                        size: d.size_bytes,
                    });
                    return true;
                }
                _ => {}
            }
            return false;
        }
        match code {
            KeyCode::Enter => {
                self.act(BackupAction::RemoteConnect);
                self.remote_connecting = self.ctl.remote_busy();
            }
            KeyCode::Tab | KeyCode::Down if !drives.is_empty() => {
                self.remote_list = true;
                self.remote_sel = self.remote_sel.min(drives.len() - 1);
            }
            KeyCode::Backspace => {
                let mut a = self.ctl.remote_address().to_string();
                a.pop();
                self.act(BackupAction::RemoteSetAddress(a));
            }
            KeyCode::Char(c) if !c.is_control() => {
                let mut a = self.ctl.remote_address().to_string();
                a.push(c);
                self.act(BackupAction::RemoteSetAddress(a));
            }
            _ => {}
        }
        false
    }
}

impl App {
    /// The Backup screen's name and key list, for the `?` window.
    pub(super) fn backup_key_context(&self) -> (&'static str, &'static [KeyHint]) {
        let Some(b) = self.backup.as_ref() else {
            return ("Backup - source", HINTS_BACKUP_SOURCE);
        };
        if b.ctl.remote_dialog_open() {
            return ("Backup - remote drive", HINTS_BACKUP_REMOTE);
        }
        if b.ctl.vhd_open() {
            return ("Backup - VHD options", HINTS_BACKUP_VHD);
        }
        match b.step {
            BackupStep::Source => ("Backup - source", HINTS_BACKUP_SOURCE),
            BackupStep::Config => ("Backup - options", HINTS_BACKUP_CONFIG),
            BackupStep::Run => ("Backup - run", HINTS_BACKUP_RUN),
        }
    }

    /// The footer's short key strip for the Backup screen.
    pub(super) fn backup_footer_keys(&self) -> Vec<(&'static str, &'static str)> {
        let Some(b) = self.backup.as_ref() else {
            return Vec::new();
        };
        if b.ctl.remote_dialog_open() {
            return vec![
                ("Enter", "Connect/Pick"),
                ("Tab", "Host/List"),
                ("Esc", "Close"),
            ];
        }
        if b.ctl.vhd_open() {
            return vec![
                ("Up/Dn", "Row"),
                ("<-/->", "Change"),
                ("0-9", "MiB"),
                ("c", "Calc min"),
                ("Enter", "Start"),
                ("Esc", "Close"),
            ];
        }
        match b.step {
            BackupStep::Source => vec![
                ("<-/->", "Tabs"),
                ("Enter/o", "Image"),
                ("d", "Disk"),
                ("r", "Remote"),
                ("?", "Help"),
            ],
            BackupStep::Config => vec![
                ("Up/Dn", "Row"),
                ("Space", "Toggle"),
                ("s", "Start"),
                ("Esc", "Back"),
                ("?", "Keys"),
            ],
            BackupStep::Run => vec![("Esc", "Back")],
        }
    }

    /// Backup screen keys. Returns `true` when consumed; `false` lets a key fall
    /// through to the shell (Left/Right switch tabs, `q` quits from idle steps).
    pub(super) fn handle_backup_key(&mut self, code: KeyCode) -> bool {
        if self.backup.is_none() {
            self.backup = Some(BackupState::new(self.shell.recent_handle()));
        }
        let b = self.backup.as_mut().unwrap();
        // Source image or output folder picker (modal).
        if let Some(picker) = b.picker.as_mut() {
            let is_dir = matches!(picker.kind, PickKind::Dir);
            match picker.handle_key(code) {
                Some(PickResult::Cancel) => b.picker = None,
                Some(PickResult::Confirm(path)) => {
                    b.picker = None;
                    if is_dir {
                        b.set_out_dir(path.to_string_lossy().into_owned());
                    } else {
                        b.act(BackupAction::OpenImage { path, rename: true });
                        self.backup_source_chosen();
                    }
                }
                None => {}
            }
            return true;
        }
        if b.ctl.remote_dialog_open() {
            if b.remote_key(code) {
                self.backup_source_chosen();
            }
            return true;
        }
        if b.ctl.vhd_open() {
            b.vhd_key(code);
            if !b.ctl.vhd_open() && b.busy() {
                self.backup_run_started();
            }
            return true;
        }

        // Physical-disk chooser sub-mode.
        if b.device_pick {
            let ndisks = self.shell.devices().len();
            match code {
                KeyCode::Up | KeyCode::Char('k') => b.device_sel = b.device_sel.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    b.device_sel = (b.device_sel + 1).min(ndisks.saturating_sub(1))
                }
                KeyCode::Esc => b.device_pick = false,
                KeyCode::Enter => {
                    let sel = b.device_sel;
                    if let Some(device) = self.shell.devices().get(sel).cloned() {
                        let b = self.backup.as_mut().unwrap();
                        b.device_pick = false;
                        b.act(BackupAction::SelectDevice { slot: sel, device });
                        self.backup_source_chosen();
                    }
                }
                _ => {}
            }
            return true;
        }

        match b.step {
            BackupStep::Source => self.backup_source_key(code),
            BackupStep::Config => self.backup_config_key(code),
            BackupStep::Run => {
                if code == KeyCode::Esc {
                    if b.result().is_none() {
                        // Request cancel; the bar clears via cancel_progress.
                        self.cancel_progress();
                        if let Some(b) = self.backup.as_mut() {
                            if !b.busy() && b.result().is_none() {
                                b.step = BackupStep::Config;
                            }
                        }
                    } else {
                        // Done: clear and return to the options.
                        self.progress = None;
                        b.cancel_note = false;
                        b.step = BackupStep::Config;
                    }
                }
                true // swallow keys while a backup is on screen
            }
        }
    }

    fn backup_source_key(&mut self, code: KeyCode) -> bool {
        let b = self.backup.as_mut().unwrap();
        match code {
            KeyCode::Enter if b.ctl.has_source() => {
                self.backup_source_chosen();
                true
            }
            KeyCode::Enter | KeyCode::Char('o') => {
                let recent = b.recent.list(RecentMode::Backup);
                b.picker = Some(
                    FilePicker::new(PickKind::File, "Choose source image").with_recent(recent),
                );
                true
            }
            KeyCode::Char('d') => {
                b.device_pick = true;
                b.device_sel = b.ctl.device_slot().unwrap_or(0);
                true
            }
            KeyCode::Char('r') => {
                b.act(BackupAction::RemoteOpen);
                b.remote_list = false;
                true
            }
            KeyCode::Char('c') => {
                if b.ctl.remote_active() {
                    b.act(BackupAction::RemoteClear);
                } else if b.ctl.has_local_source() {
                    b.act(BackupAction::CloseSource);
                }
                true
            }
            _ => false,
        }
    }

    fn backup_config_key(&mut self, code: KeyCode) -> bool {
        let b = self.backup.as_mut().unwrap();
        let focus = b.focus;
        match code {
            KeyCode::Esc => {
                if b.ctl.scanning().is_some() {
                    b.act(BackupAction::CancelScan);
                } else {
                    b.step = BackupStep::Source;
                }
            }
            KeyCode::Down => b.move_focus(1),
            KeyCode::Up | KeyCode::BackTab => b.move_focus(-1),
            KeyCode::Enter if focus == Field::Start => self.start_backup(),
            KeyCode::Enter => b.move_focus(1),
            KeyCode::Tab if focus == Field::Output => {
                // Browse for the output folder; the confirm branch routes it by its PickKind.
                let mut p = FilePicker::new(PickKind::Dir, "Choose output folder");
                p.input = b.out_dir.clone();
                b.picker = Some(p);
            }
            KeyCode::Char(' ') if !focus.is_text() => {
                b.change(focus, true);
            }
            KeyCode::Left | KeyCode::Right => {
                // Rows without a choice leave Left/Right to tab switching.
                return b.change(focus, code == KeyCode::Right);
            }
            KeyCode::Backspace => b.backspace(),
            KeyCode::Char(c) if focus.is_text() && !c.is_control() => b.type_char(c),
            KeyCode::Char('a') => b.act(BackupAction::SelectAll),
            KeyCode::Char('n') => b.act(BackupAction::SelectNone),
            KeyCode::Char('c') => {
                if let Field::Partition(index) = focus {
                    b.act(BackupAction::CalcMin(index));
                }
            }
            KeyCode::Char('x') => {
                if let Field::Partition(index) = focus {
                    let row = b.ctl.rows().into_iter().find(|r| r.index == index);
                    if let Some(CompactCell::Toggle(on)) = row.map(|r| r.compact) {
                        b.act(BackupAction::SetCompact(index, !on));
                    }
                }
            }
            KeyCode::Char('p') => {
                if b.ctl.can_scan_device() {
                    b.act(BackupAction::ScanPartitions);
                }
            }
            KeyCode::Char('s') => self.start_backup(),
            _ => return false,
        }
        true
    }

    /// A source was picked: on to the options, defaulting the output folder to the image's.
    fn backup_source_chosen(&mut self) {
        let Some(b) = self.backup.as_mut() else {
            return;
        };
        if b.out_dir.is_empty() {
            if let Some(dir) = b.ctl.default_destination() {
                b.set_out_dir(dir.display().to_string());
            }
        }
        b.step = BackupStep::Config;
        b.focus = Field::Output;
    }

    /// Hand the form to the controller and press its Start; it validates and runs the backup.
    pub(super) fn start_backup(&mut self) {
        let Some(b) = self.backup.as_mut() else {
            return;
        };
        b.set_out_dir(b.out_dir.clone());
        let chd = match b.chd_choice() {
            Ok(c) => c,
            Err(e) => {
                b.note = Some(e);
                return;
            }
        };
        b.note = None;
        b.act(BackupAction::Start(chd));
        b.cancel_note = false;
        if b.ctl.vhd_open() {
            b.vhd_sel = 0;
            return;
        }
        // A VHD Start on an unread disk scans first and opens the window; stay on the form.
        if b.busy() || b.ctl.last_result().is_some() {
            self.backup_run_started();
        }
    }

    /// On to the Run step, with the bottom bar while the run is live.
    fn backup_run_started(&mut self) {
        let Some(b) = self.backup.as_mut() else {
            return;
        };
        b.op = "Starting...".to_string();
        b.step = BackupStep::Run;
        if b.busy() {
            self.progress = Some(Progress {
                shared: Arc::new(Mutex::new(ProgressShared::default())),
                tracker: RateTracker::default(),
                label: "Backing up".to_string(),
            });
        }
    }

    /// Drive the Backup controller and mirror its run into the visual bar.
    pub(super) fn backup_tick(&mut self) {
        let Some(b) = self.backup.as_mut() else {
            return;
        };
        // A device refresh (Inspect's `r`) may have moved or dropped the source disk.
        b.ctl.reconcile_devices(self.shell.devices());
        let _ = b.ctl.tick();
        b.after();
        // A finished connect moves the focus to the drives it listed.
        if b.remote_connecting && !b.ctl.remote_busy() {
            b.remote_connecting = false;
            if !b.ctl.remote_drives().is_empty() {
                b.remote_list = true;
                b.remote_sel = 0;
            }
        }
        if b.vhd_sel > b.vhd_len() + 1 {
            b.vhd_sel = 0;
        }
        let bar = b.ctl.progress();
        if let Some(bar) = &bar {
            b.op = bar.operation.clone();
        } else if b.ctl.start_pending() {
            b.op = "Computing minimum sizes...".to_string();
        }
        if b.step == BackupStep::Run {
            let done = !b.busy();
            if let Some(prog) = self.progress.as_ref() {
                if let Ok(mut sh) = prog.shared.lock() {
                    if let Some(bar) = &bar {
                        sh.current = bar.current_bytes;
                        sh.total = bar.total_bytes;
                    }
                    sh.done = done;
                }
            }
        }
    }

    pub(super) fn draw_backup(&self, frame: &mut Frame, area: Rect, b: &BackupState) {
        let title = match b.step {
            BackupStep::Source => "Backup  -  step 1/3: source",
            BackupStep::Config => "Backup  -  step 2/3: options",
            BackupStep::Run => "Backup  -  step 3/3: run",
        };
        let mut lines: Vec<Line<'static>> = Vec::new();
        let mut focus_line = 0usize;
        match b.step {
            BackupStep::Source => self.backup_source_lines(b, &mut lines),
            BackupStep::Config => focus_line = self.backup_config_lines(b, &mut lines),
            BackupStep::Run => self.backup_run_lines(b, &mut lines),
        }
        // Keep the focused row on screen when the form is taller than the pane.
        let inner = area.height.saturating_sub(2) as usize;
        let scroll = if inner > 2 && focus_line + 2 > inner {
            (focus_line + 2 - inner) as u16
        } else {
            0
        };
        let para = Paragraph::new(Text::from(lines)).block(self.pane_block(title, true));
        let para = if b.step == BackupStep::Config {
            para.scroll((scroll, 0))
        } else {
            para.wrap(Wrap { trim: false })
        };
        frame.render_widget(para, area);

        if let Some(picker) = &b.picker {
            picker.draw(frame, area, self.palette, self.border);
        }
        if b.device_pick {
            self.draw_device_pick(frame, area, b.device_sel, "Pick a physical disk");
        }
        if b.ctl.remote_dialog_open() {
            self.draw_backup_remote(frame, area, b);
        }
        if b.ctl.vhd_open() {
            self.draw_backup_vhd(frame, area, b);
        }
    }

    fn backup_status_lines(&self, b: &BackupState, lines: &mut Vec<Line<'static>>) {
        if let Some(note) = &b.note {
            lines.push(Line::styled(format!("  {note}"), self.palette.warn()));
        }
        if let Some(last) = b.log.last() {
            lines.push(Line::styled(format!("  {last}"), self.palette.dim()));
        }
    }

    fn backup_source_lines(&self, b: &BackupState, lines: &mut Vec<Line<'static>>) {
        lines.push(Line::styled(
            "Choose what to back up:",
            self.palette.accent(),
        ));
        lines.push(Line::raw(""));
        let key = |k: &str, text: Span<'static>| {
            Line::from(vec![
                Span::styled(format!("  {k:<11}"), self.palette.accent()),
                text,
            ])
        };
        lines.push(key("Enter / o", Span::raw("choose a source image file")));
        let dev = if self.elevated {
            Span::raw("choose a physical disk")
        } else {
            Span::styled(
                "choose a physical disk (may prompt for admin)",
                self.palette.dim(),
            )
        };
        lines.push(key("d", dev));
        let remote = if BackupController::remote_available() {
            Span::raw("a drive on a remote rb-cli serve daemon")
        } else {
            Span::styled(
                "a remote drive (not compiled into this build)",
                self.palette.dim(),
            )
        };
        lines.push(key("r", remote));
        let source = b.source_text();
        if !source.is_empty() {
            lines.push(key("c", Span::raw("close the source")));
            lines.push(key("Enter", Span::raw("continue to the options")));
            lines.push(Line::raw(""));
            lines.push(Line::from(vec![
                Span::styled("Current source: ", self.palette.dim()),
                Span::raw(source),
            ]));
        }
        lines.push(Line::raw(""));
        if let Some(note) = self.device_note() {
            lines.push(note);
        }
        self.backup_status_lines(b, lines);
    }

    /// The options form; returns the focused row's line.
    fn backup_config_lines(&self, b: &BackupState, lines: &mut Vec<Line<'static>>) -> usize {
        let opts = b.ctl.options().clone();
        lines.push(Line::from(vec![
            Span::styled("Source: ", self.palette.dim()),
            Span::raw(b.source_text()),
            Span::styled(
                if b.ctl.device_slot().is_some() {
                    "  [device]"
                } else {
                    ""
                },
                self.palette.dim(),
            ),
        ]));
        if let Some(snap) = b.ctl.decoding().or_else(|| b.ctl.scanning()) {
            let what = if snap.phase.is_empty() {
                snap.label
            } else {
                snap.phase
            };
            lines.push(Line::styled(
                format!("  {what}  (Esc stops a scan)"),
                self.palette.dim(),
            ));
        } else if b.ctl.remote_busy() {
            lines.push(Line::styled(
                "  Loading the remote drive...",
                self.palette.dim(),
            ));
        } else if let Some(table) = b.ctl.table_desc() {
            lines.push(Line::styled(
                format!(
                    "  Partition table: {table} ({} partition(s))",
                    b.ctl.partition_count()
                ),
                self.palette.dim(),
            ));
        } else if let Some(err) = b.ctl.load_error() {
            lines.push(Line::styled(format!("  {err}"), self.palette.warn()));
        }
        if b.ctl.can_scan_device() {
            let what = if b.ctl.device_scanned() {
                "p  Reload partition info"
            } else {
                "p  Load partition info (may prompt for administrator credentials)"
            };
            lines.push(Line::styled(format!("  {what}"), self.palette.accent()));
        }
        lines.push(Line::raw(""));

        let mut focus_line = 0;
        let marker = |f: Field| -> (&'static str, ratatui::style::Style) {
            if b.focus == f {
                ("> ", self.palette.accent().add_modifier(Modifier::BOLD))
            } else {
                ("  ", self.palette.accent())
            }
        };

        let rows = b.ctl.rows();
        if !rows.is_empty() {
            lines.push(Line::styled(
                "Partitions  (Space include, a all, n none, c calc min, x compact)",
                self.palette.accent().add_modifier(Modifier::BOLD),
            ));
            lines.push(Line::styled(
                format!(
                    "      Inc  {:>2}  {:<18} {:>10}  {:<14} {:>5}  Compact",
                    "#", "Type", "Size", "Min size", "Frag"
                ),
                self.palette.dim(),
            ));
            for r in &rows {
                let f = Field::Partition(r.index);
                if b.focus == f {
                    focus_line = lines.len();
                }
                let (m, style) = marker(f);
                let min = match &r.min {
                    MinCell::Size(s) => format_size(*s),
                    MinCell::Pending(phase) => format!("({phase})"),
                    MinCell::CanCalc => "c = calc min".to_string(),
                    MinCell::Empty => String::new(),
                };
                let frag = r
                    .fragmentation
                    .map(|p| format!("{p:.0}%"))
                    .unwrap_or_default();
                let compact = match r.compact {
                    CompactCell::Hidden => "",
                    CompactCell::NotImplemented => "n/a",
                    CompactCell::Toggle(true) => "[x]",
                    CompactCell::Toggle(false) => "[ ]",
                };
                let mut type_name = r.type_name.clone();
                if type_name.chars().count() > 18 {
                    type_name = type_name.chars().take(17).collect::<String>() + "~";
                }
                lines.push(Line::styled(
                    format!(
                        "{m}    {}  {:>2}  {:<18} {:>10}  {:<14} {:>5}  {}",
                        if r.selected { "[x]" } else { "[ ]" },
                        r.position,
                        type_name,
                        format_size(r.size_bytes),
                        min,
                        frag,
                        compact,
                    ),
                    style,
                ));
            }
            lines.push(Line::raw(""));
        }

        let check = |on: bool| if on { "[x]" } else { "[ ]" };
        let (dest, name, format) = (
            format!("{}:", labels::DESTINATION),
            format!("{}:", labels::NAME),
            format!("{}:", labels::FORMAT),
        );
        for f in b.fields() {
            if matches!(f, Field::Partition(_)) {
                continue;
            }
            if b.focus == f {
                focus_line = lines.len();
            }
            let (m, style) = marker(f);
            let cursor = if b.focus == f && f.is_text() {
                Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED))
            } else {
                Span::raw("")
            };
            let (label, value): (&str, String) = match f {
                Field::Partition(_) => continue,
                Field::Output => (
                    dest.as_str(),
                    if b.out_dir.is_empty() && b.focus != f {
                        "(output folder; Tab browses)".to_string()
                    } else {
                        b.out_dir.clone()
                    },
                ),
                Field::Name => (name.as_str(), b.ctl.name().to_string()),
                Field::Format => (
                    format.as_str(),
                    format!("< {} >", BACKUP_FORMATS[b.format_index()].0),
                ),
                Field::ChdMode | Field::ChdHunk | Field::ChdCodec(_) => self.chd_row(b, f),
                Field::SectorBySector => (
                    "Copy:",
                    format!(
                        "{} Sector-by-sector (includes all blank space)",
                        check(opts.sector_by_sector)
                    ),
                ),
                Field::Resize => (
                    "Resize:",
                    format!(
                        "{} Resize partitions to minimum size{}",
                        check(opts.resize_effective()),
                        if opts.size_options_editable() {
                            ""
                        } else {
                            "  (not with sector-by-sector)"
                        }
                    ),
                ),
                Field::KeepSwap => (
                    "Swap:",
                    format!(
                        "{} Keep swap/page-file content{}",
                        check(opts.keep_swap),
                        if opts.size_options_editable() {
                            ""
                        } else {
                            "  (not with sector-by-sector)"
                        }
                    ),
                ),
                Field::Split => (
                    "Split:",
                    format!(
                        "{} Split archives{}",
                        check(opts.split && opts.split_editable()),
                        if opts.split_editable() {
                            ""
                        } else {
                            "  (not for VHD)"
                        }
                    ),
                ),
                Field::SplitMib => {
                    let eff = opts.split_mib_effective();
                    let note = if eff == opts.split_mib {
                        format!("  ({SPLIT_MIB_MIN}-{SPLIT_MIB_MAX})")
                    } else {
                        format!("  ({SPLIT_MIB_MIN}-{SPLIT_MIB_MAX}; {eff} is used)")
                    };
                    ("Split MiB:", format!("{}{note}", opts.split_mib))
                }
                Field::Checksum => (
                    "Checksum:",
                    format!("< {} >", BACKUP_CHECKSUMS[b.checksum_index()].0),
                ),
                Field::Start => {
                    let sel = b.focus == f;
                    let style = if sel {
                        self.palette
                            .accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                    } else {
                        self.palette.accent()
                    };
                    lines.push(Line::raw(""));
                    if sel {
                        focus_line = lines.len();
                    }
                    let why = b.ctl.validate().err().unwrap_or_default();
                    lines.push(Line::from(vec![
                        Span::styled(format!("{m}[ {} ]", labels::START), style),
                        Span::styled(format!("  {why}"), self.palette.dim()),
                    ]));
                    continue;
                }
            };
            lines.push(Line::from(vec![
                Span::styled(format!("{m}{label:<15}"), style),
                Span::raw(value),
                cursor,
            ]));
        }
        lines.push(Line::raw(""));
        self.backup_status_lines(b, lines);
        lines.push(Line::styled(
            "Up/Down row  Space toggle  Left/Right change  Tab browse  s start  Esc back  ? keys",
            self.palette.dim(),
        ));
        focus_line
    }

    #[cfg(feature = "chd")]
    fn chd_row(&self, b: &BackupState, f: Field) -> (&'static str, String) {
        match f {
            Field::ChdMode => ("CHD:", format!("< {} >", b.chd.mode.label())),
            Field::ChdHunk => {
                let h = b.chd.custom.hunk_size;
                let unit = unit_size_for(ChdProfile::Hd);
                let err = b
                    .chd
                    .validate(ChdProfile::Hd)
                    .err()
                    .map(|e| format!("  {e}"))
                    .unwrap_or_default();
                ("  Hunk:", format!("< {h} >  (unit {unit} bytes){err}"))
            }
            Field::ChdCodec(slot) => {
                let label = ["  Codec 1:", "  Codec 2:", "  Codec 3:", "  Codec 4:"][slot.min(3)];
                (
                    label,
                    format!("< {} >", codec_label(b.chd.custom.codecs[slot])),
                )
            }
            _ => ("", String::new()),
        }
    }

    #[cfg(not(feature = "chd"))]
    fn chd_row(&self, _b: &BackupState, _f: Field) -> (&'static str, String) {
        ("", String::new())
    }

    fn backup_run_lines(&self, b: &BackupState, lines: &mut Vec<Line<'static>>) {
        lines.push(Line::from(vec![
            Span::styled("Source: ", self.palette.dim()),
            Span::raw(b.source_text()),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Output: ", self.palette.dim()),
            Span::raw(format!(
                "{}/{}  ({}, {})",
                b.out_dir,
                b.ctl.name(),
                BACKUP_FORMATS[b.format_index()].0,
                BACKUP_CHECKSUMS[b.checksum_index()].0
            )),
        ]));
        lines.push(Line::raw(""));
        if b.ctl.start_pending() {
            lines.push(Line::styled(
                "Waiting for the minimum sizes before starting (Esc stops waiting).",
                self.palette.accent(),
            ));
        } else if !b.op.is_empty() {
            lines.push(Line::from(vec![
                Span::styled("Step: ", self.palette.dim()),
                Span::raw(b.op.clone()),
            ]));
        }
        for msg in &b.log {
            lines.push(Line::styled(format!("  {msg}"), self.palette.dim()));
        }
        if let Some((r, is_error)) = b.result() {
            lines.push(Line::raw(""));
            let style = if is_error {
                self.palette.warn()
            } else {
                self.palette.accent()
            };
            lines.push(Line::styled(format!("  {r}"), style));
            lines.push(Line::styled(
                "  Esc to return to options.",
                self.palette.dim(),
            ));
        }
    }

    fn draw_backup_remote(&self, frame: &mut Frame, area: Rect, b: &BackupState) {
        let mut lines: Vec<Line<'static>> = Vec::new();
        lines.push(Line::styled(
            format!("  {}", crate::controller::backup::hints::REMOTE_DRIVE_INTRO),
            self.palette.dim(),
        ));
        lines.push(Line::raw(""));
        let addr = b.ctl.remote_address().to_string();
        let host_focus = !b.remote_list;
        let field_style = if host_focus {
            self.palette.accent().add_modifier(Modifier::BOLD)
        } else {
            self.palette.accent()
        };
        let shown = if addr.is_empty() && !host_focus {
            "192.168.1.50:7341".to_string()
        } else {
            addr
        };
        lines.push(Line::from(vec![
            Span::styled(
                format!("{}Host: ", if host_focus { "> " } else { "  " }),
                field_style,
            ),
            Span::raw(shown),
            if host_focus {
                Span::styled(" ", self.palette.accent().add_modifier(Modifier::REVERSED))
            } else {
                Span::raw("")
            },
        ]));
        if b.ctl.remote_busy() {
            lines.push(Line::styled("  Connecting...", self.palette.dim()));
        }
        if let Some(err) = b.ctl.remote_error() {
            lines.push(Line::styled(format!("  {err}"), self.palette.warn()));
        }
        if b.ctl.remote_listed() {
            lines.push(Line::raw(""));
            let drives = b.ctl.remote_drives();
            if drives.is_empty() {
                lines.push(Line::raw(
                    "  No physical drives reported. The daemon may need to run as root.",
                ));
            } else {
                lines.push(Line::styled(
                    "  Pick a drive to back up:",
                    self.palette.dim(),
                ));
                for (i, d) in drives.iter().enumerate() {
                    let sel = b.remote_list && i == b.remote_sel;
                    let style = if sel {
                        self.palette
                            .accent()
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
                    } else {
                        self.palette.accent()
                    };
                    lines.push(Line::styled(
                        format!("{}{}", if sel { "> " } else { "  " }, d.label),
                        style,
                    ));
                }
            }
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Enter connect / pick   Tab host / drives   Esc close",
            self.palette.dim(),
        ));
        let h = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
        let popup = centered_rect(76.min(area.width), h, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .wrap(Wrap { trim: false })
                .block(self.pane_block("Remote Drive", true)),
            popup,
        );
    }

    fn draw_backup_vhd(&self, frame: &mut Frame, area: Rect, b: &BackupState) {
        let mut lines: Vec<Line<'static>> = Vec::new();
        let whole = b.ctl.vhd_whole_disk();
        let rows = b.ctl.vhd_rows();
        let style_for = |sel: bool| {
            if sel {
                self.palette
                    .accent()
                    .add_modifier(Modifier::REVERSED | Modifier::BOLD)
            } else {
                self.palette.accent()
            }
        };
        let mark = |sel: bool| if sel { "> " } else { "  " };
        lines.push(Line::styled(
            format!(
                "{}Layout: < {} >",
                mark(b.vhd_sel == 0),
                if whole {
                    "Whole Disk (single .vhd file)"
                } else {
                    "Per Partition (one .vhd per partition)"
                }
            ),
            style_for(b.vhd_sel == 0),
        ));
        lines.push(Line::raw(""));
        if !rows.is_empty() {
            lines.push(Line::styled(
                format!(
                    "    {:>3}  {:<20} {:<12} {:>10}  {}",
                    "#", "Type", "Size mode", "Size (MiB)", "Minimum"
                ),
                self.palette.dim(),
            ));
        }
        for (i, row) in rows.iter().enumerate() {
            let sel = b.vhd_sel == i + 1;
            let mode = match row.mode {
                SizeMode::Original => "Original",
                SizeMode::Minimum => "Minimum",
                SizeMode::MinPlus20 => "Min+20%",
                SizeMode::Custom => "Custom",
                SizeMode::FillRemaining => "Fill",
            };
            let size = if row.mode == SizeMode::Custom {
                let (lo, hi) = row.custom_bounds_mib();
                format!("{} ({lo}-{hi})", row.custom_mib)
            } else {
                format!("{}", row.effective_size() / (1024 * 1024))
            };
            let min = match (b.ctl.min_phase(row.index), row.deferred_fs) {
                (Some(phase), _) => format!("({phase})"),
                (None, Some(_)) => "c = calc min".to_string(),
                (None, None) if row.minimum_size < row.original_size => {
                    format_size(row.minimum_size)
                }
                (None, None) => String::new(),
            };
            let mut type_name = row.type_name.clone();
            if type_name.chars().count() > 20 {
                type_name = type_name.chars().take(19).collect::<String>() + "~";
            }
            lines.push(Line::styled(
                format!(
                    "{}  {:>3}  {:<20} < {:<8} > {:>10}  {}",
                    mark(sel),
                    row.index,
                    type_name,
                    mode,
                    size,
                    min
                ),
                style_for(sel),
            ));
        }
        lines.push(Line::raw(""));
        let start_sel = b.vhd_sel == rows.len() + 1;
        lines.push(Line::styled(
            format!("{}[ Start VHD backup ]", mark(start_sel)),
            style_for(start_sel),
        ));
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Up/Down row  Left/Right change  0-9 Custom MiB  c calc min  Enter start  Esc close",
            self.palette.dim(),
        ));
        let h = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
        let popup = centered_rect(90.min(area.width), h, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(self.pane_block("VHD Backup Options", true))
                .wrap(Wrap { trim: false }),
            popup,
        );
    }
}
