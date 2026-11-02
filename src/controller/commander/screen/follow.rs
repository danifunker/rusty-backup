//! File Info that follows a pane's cursor: the quick view (the active pane's File Info drawn in
//! the other pane) and a profile's side panel (`DetailPlacement::SidePanel`).
//!
//! Both build the same [`FileDetail`] as the File Info window. The entry's metadata shows at
//! once; the preview, xattrs and names are read by the slice-9 `entry_detail` job, started only
//! once the cursor has rested on a row for [`FOLLOW_DELAY`], and cancelled when it moves on.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{CommanderAction, CommanderController, FileDetail, Side, MAX_PREVIEW_SIZE};
use crate::controller::browser::profile::DetailPlacement;
use crate::controller::browser::{BrowserPaneAction, ListingStyle};
use crate::controller::keys::Command;
use crate::model::entry_detail::{self, DetailExtras, DetailSource};
use crate::model::job::{Job, JobCtx};
use disk_core::id_names::IdNameMap;

/// How long the cursor rests on a row before its preview is read.
pub const FOLLOW_DELAY: Duration = Duration::from_millis(150);

/// One pane's following detail.
#[derive(Default)]
pub(super) struct Follow {
    /// The row it shows, by node id.
    key: Option<String>,
    detail: Option<FileDetail>,
    /// The read waiting for the cursor to rest, and when it may start.
    pending: Option<(DetailSource, bool, Instant)>,
    job: Option<Job<DetailExtras>>,
}

impl Follow {
    fn reset(&mut self) {
        if let Some(job) = self.job.take() {
            job.cancel();
        }
        *self = Follow::default();
    }
}

/// One entry of the View menu; every view draws the same list.
#[derive(Debug, Clone)]
pub struct ViewMenuItem {
    pub label: String,
    /// The chord that does the same, or "".
    pub hint: String,
    pub kind: ViewItemKind,
    pub checked: bool,
    pub enabled: bool,
    pub action: CommanderAction,
}

/// How a View menu entry is drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewItemKind {
    /// One of the listing styles (radio buttons).
    Style,
    Toggle,
    Command,
}

impl CommanderController {
    /// True while the active pane's File Info shows in the other pane.
    pub fn quick_view(&self) -> bool {
        self.quick_view
    }

    /// The quick view's File Info (it shows in the pane other than `detail.side`).
    pub fn quick_view_detail(&self) -> Option<&FileDetail> {
        let slot = &self.follow[self.active.idx()];
        slot.detail.as_ref().filter(|_| self.quick_view)
    }

    /// The side panel's File Info for a pane whose profile places details beside the listing.
    pub fn side_detail(&self, side: Side) -> Option<&FileDetail> {
        let panel = self.pane(side).profile().detail == DetailPlacement::SidePanel;
        self.follow[side.idx()].detail.as_ref().filter(|_| panel)
    }

    /// True while a following detail waits for the cursor to rest, so a view keeps ticking.
    pub fn follow_pending(&self) -> bool {
        self.follow
            .iter()
            .any(|f| f.pending.is_some() || f.job.is_some())
    }

    pub fn view_menu_shown(&self) -> bool {
        self.view_menu
    }

    /// The View menu for the active pane: its listing styles, quick view, the log and help.
    pub fn view_menu(&self) -> Vec<ViewMenuItem> {
        let side = self.active;
        let pane = self.pane(side);
        let mut items: Vec<ViewMenuItem> = pane
            .profile()
            .styles
            .iter()
            .map(|s| ViewMenuItem {
                label: s.label().to_string(),
                hint: String::new(),
                kind: ViewItemKind::Style,
                checked: pane.listing_style() == *s,
                enabled: true,
                action: CommanderAction::Pane(side, BrowserPaneAction::SetListingStyle(*s)),
            })
            .collect();
        let toggle = |label: &str, cmd, checked, enabled, action| ViewMenuItem {
            label: label.to_string(),
            hint: self.key_hint(cmd),
            kind: ViewItemKind::Toggle,
            checked,
            enabled,
            action,
        };
        if !self.single {
            items.push(toggle(
                "Quick view",
                Command::QuickView,
                self.quick_view,
                true,
                CommanderAction::ToggleQuickView,
            ));
        }
        items.push(toggle(
            "Session log",
            Command::Log,
            self.show_log,
            true,
            CommanderAction::ShowLog(!self.show_log),
        ));
        items.push(ViewMenuItem {
            label: "Keyboard shortcuts".into(),
            hint: self.key_hint(Command::Help),
            kind: ViewItemKind::Command,
            checked: false,
            enabled: true,
            action: CommanderAction::ShowHelp(true),
        });
        items
    }

    pub(super) fn toggle_quick_view(&mut self) {
        if self.single {
            self.status = "Quick view needs a second pane.".into();
            return;
        }
        self.quick_view = !self.quick_view;
        self.status = if self.quick_view {
            "Quick view: the other pane shows File Info for the cursor row.".into()
        } else {
            "Quick view off.".into()
        };
    }

    /// The commands that do something only when a pane shows a tree.
    pub(super) fn tree_command_idle(&self, cmd: Command) -> bool {
        let flat = self.pane(self.active).listing_style() == ListingStyle::FlatList;
        flat && matches!(cmd, Command::Expand | Command::Collapse)
    }

    fn follows(&self, side: Side) -> bool {
        let panel = self.pane(side).profile().detail == DetailPlacement::SidePanel;
        panel || (self.quick_view && !self.single && side == self.active)
    }

    /// Point each following detail at its pane's cursor row, start reads that are due, take results.
    pub(super) fn sync_follow(&mut self) {
        let now = Instant::now();
        for side in [Side::Left, Side::Right] {
            if !self.follows(side) {
                self.follow[side.idx()].reset();
                continue;
            }
            self.retarget(side, now);
            self.start_follow_read(side, now);
            self.poll_follow(side);
        }
    }

    fn retarget(&mut self, side: Side, now: Instant) {
        let target = self.pane_mut(side).cursor_detail();
        // The entry's size and date are in the key, so a row rewritten by an apply is read again.
        let key = target
            .as_ref()
            .map(|t| format!("{}\0{}\0{:?}", t.node_id, t.entry.size, t.entry.modified));
        if key == self.follow[side.idx()].key {
            return;
        }
        self.follow[side.idx()].reset();
        let Some(t) = target else {
            return;
        };
        self.detail_serial += 1;
        let detail = FileDetail {
            side,
            entry: t.entry,
            editable: t.editable && self.pane(side).profile().detail == DetailPlacement::SidePanel,
            fs_type: t.fs_type,
            loading: t.source.is_some(),
            preview: None,
            rsrc_preview: None,
            xattrs: Vec::new(),
            id_names: IdNameMap::default(),
            size_plan: None,
            result: None,
            serial: self.detail_serial,
        };
        let slot = &mut self.follow[side.idx()];
        slot.key = key;
        slot.detail = Some(detail);
        slot.pending = t.source.map(|s| (s, t.at_root, now + FOLLOW_DELAY));
    }

    fn start_follow_read(&mut self, side: Side, now: Instant) {
        let slot = &self.follow[side.idx()];
        let due = slot.pending.as_ref().is_some_and(|(_, _, at)| *at <= now);
        if !due || slot.job.is_some() {
            return;
        }
        let Some(entry) = slot.detail.as_ref().map(|d| d.entry.clone()) else {
            return;
        };
        let Some((source, at_root, at)) = self.follow[side.idx()].pending.take() else {
            return;
        };
        let retry = (source.clone(), at_root, at);
        let body =
            move |_: &JobCtx| entry_detail::read_detail(source, &entry, MAX_PREVIEW_SIZE, at_root);
        let reads = self.reads(side);
        let notify = Arc::clone(&self.notify);
        match self.jobs.spawn("Reading File Info", reads, notify, body) {
            Ok(job) => self.follow[side.idx()].job = Some(job),
            // Another job holds the source; the next tick tries again.
            Err(_) => self.follow[side.idx()].pending = Some(retry),
        }
    }

    fn poll_follow(&mut self, side: Side) {
        let slot = &mut self.follow[side.idx()];
        let Some(job) = slot.job.as_ref().filter(|j| !j.is_running()) else {
            return;
        };
        let extras = job.take_result();
        slot.job = None;
        let Some(d) = slot.detail.as_mut() else {
            return;
        };
        d.loading = false;
        if let Some(x) = extras {
            d.preview = x.preview;
            d.rsrc_preview = x.rsrc_preview;
            d.xattrs = x.xattrs;
            d.id_names = x.id_names;
            d.size_plan = x.size_plan;
        }
    }

    /// Where a metadata edit goes: File Info's entry, else the active pane's side panel.
    pub(super) fn metadata_target(&self) -> Option<(Side, bool)> {
        if let Some(d) = &self.detail {
            return Some((d.side, d.editable));
        }
        let d = self.side_detail(self.active)?;
        Some((d.side, d.editable))
    }

    /// Record a staging result on whichever detail the edit came from.
    pub(super) fn set_metadata_result(&mut self, result: String) {
        if let Some(d) = self.detail.as_mut() {
            d.result = Some(result);
        } else if let Some(d) = self.follow[self.active.idx()].detail.as_mut() {
            d.result = Some(result);
        }
    }
}
