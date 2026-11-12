//! Expand HFS Volume over `controller::expand_hfs`; the Win32 shell's `expand_hfs.rs` is the
//! reference. Inspect opens it on a classic HFS row; it lives while the controller has a source.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::app::Notify;
use rusty_backup::controller::expand_hfs::{
    ids, labels, BlockChoice, ExpandHfsAction, ExpandHfsController, ExpandSource,
};
use rusty_backup::controller::{Controller, Effect};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::ui::{self, CloseDelegate};

const MIB: u64 = 1024 * 1024;
const HFV_NOTE: &str = "Bare classic-HFS volume with no partition table: BasiliskII / \
    SheepShaver mount it as .hfv. Capped at 4063 MB (classic HFS limit).";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    SourceHead,
    Facts,
    TargetHead,
    FormatCaption,
    Apm,
    Flat,
    Note,
    BlockCaption,
    Block,
    SizeCaption,
    Size,
    OutCaption,
    Output,
    Save,
    Phase,
    Outcome,
    Lines,
    Expand,
    Close,
}

pub struct ExpandHfsWindow {
    ctrl: RefCell<ExpandHfsController>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<ExpandHfsWindow>,
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    blocks: RefCell<Vec<BlockChoice>>,
}

impl ExpandHfsWindow {
    pub fn open(
        mtm: MainThreadMarker,
        notify: Arc<dyn Notify>,
        host: EffectHost,
        source: ExpandSource,
    ) -> Rc<ExpandHfsWindow> {
        let me = Rc::new_cyclic(|me: &Weak<ExpandHfsWindow>| {
            let close = me.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Expand HFS Volume",
                560.0,
                520.0,
                Box::new(move || {
                    if let Some(d) = close.upgrade() {
                        d.send(ExpandHfsAction::Close);
                    }
                }),
            );
            let resize = me.clone();
            delegate.on_resize(Box::new(move || {
                if let Some(d) = resize.upgrade() {
                    d.layout();
                }
            }));
            let body = window.contentView().expect("the window's content view");
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(d) = fire.upgrade() {
                        d.fired(k);
                    }
                }),
            );
            let f = &form;
            f.label(K::SourceHead, "Source");
            f.wrap(K::Facts, "");
            f.label(K::TargetHead, "Target");
            f.label(K::FormatCaption, "Output format:");
            f.radio(K::Apm, "APM disk (.hda)", ids::OUTPUT_FORMAT);
            f.radio(K::Flat, "Bare HFS image (.hfv / .img)", "");
            f.wrap(K::Note, HFV_NOTE);
            f.label(K::BlockCaption, "Allocation block size:");
            f.popup(K::Block, &[], ids::BLOCK_SIZE);
            f.label(K::SizeCaption, &format!("{}:", labels::TARGET_SIZE));
            f.field(K::Size, ids::TARGET_SIZE, labels::TARGET_SIZE, false);
            f.label(K::OutCaption, "Output file:");
            f.label(K::Output, "(not chosen)");
            f.button(K::Save, labels::SAVE_AS, ids::SAVE_AS);
            f.label(K::Phase, "");
            f.label(K::Outcome, "");
            f.wrap(K::Lines, "");
            f.button(K::Expand, labels::EXPAND, ids::EXPAND);
            f.button(K::Close, "Cancel", ids::CLOSE);
            ui::present(&window);
            ExpandHfsWindow {
                ctrl: RefCell::new(ExpandHfsController::new(notify)),
                host,
                sync: SyncGuard::default(),
                me: me.clone(),
                window,
                _delegate: delegate,
                form,
                blocks: RefCell::default(),
            }
        });
        me.layout();
        me.apply(ExpandHfsAction::Open(source));
        me
    }

    pub fn apply(&self, action: ExpandHfsAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    pub fn is_open(&self) -> bool {
        self.ctrl.borrow().is_open()
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// Send `action` once the current handler returns: the window may close with it.
    fn send(&self, action: ExpandHfsAction) {
        let me = self.me.clone();
        crate::app::later(Box::new(move || {
            if let Some(d) = me.upgrade() {
                d.apply(action);
            }
        }));
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(d) = me.upgrade() else { return };
            let action = match reply {
                Reply::Picked { purpose, path } => ExpandHfsAction::Picked { purpose, path },
                // Expand HFS asks only for its output file.
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            d.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        if self.ctrl.borrow().source().is_none() {
            self.window.orderOut(None);
            return;
        }
        self.sync.run(|| self.refresh());
        self.layout();
    }

    /// Read the controller, then set the controls; only changed values are set.
    fn refresh(&self) {
        let f = &self.form;
        let c = self.ctrl.borrow();
        if let Some(src) = c.source() {
            let block = match src.source_block_size % 1024 {
                0 => format!("{} KiB", src.source_block_size / 1024),
                _ => format!("{} bytes", src.source_block_size),
            };
            f.set_text(
                K::Facts,
                &format!(
                    "Volume: {}\nSize: {} MiB ({} bytes)\nAllocation block size: {block}\nUsed: {} MiB across {} file(s) in {} folder(s)",
                    src.volume_name,
                    src.partition_size / MIB,
                    src.partition_size,
                    src.used_bytes / MIB,
                    src.file_count,
                    src.dir_count
                ),
            );
        }
        let editable = c.editable();
        for k in [K::Apm, K::Flat, K::Block, K::Size, K::Save] {
            f.enable(k, editable);
        }
        let flat = c.flat_hfv();
        f.set_check(K::Apm, !flat);
        f.set_check(K::Flat, flat);
        f.show(K::Note, flat);
        let blocks = c.block_choices();
        let names: Vec<String> = blocks.iter().map(|b| b.label.clone()).collect();
        f.set_items(K::Block, &names);
        let at = blocks
            .iter()
            .position(|b| b.bytes == c.block_size())
            .unwrap_or(0);
        f.select(K::Block, at);
        *self.blocks.borrow_mut() = blocks;
        // The field keeps the user's words while they mean the controller's value.
        let typed: Option<u32> = f.text(K::Size).trim().parse().ok();
        if typed != Some(c.size_mib()) {
            f.set_text(K::Size, &c.size_mib().to_string());
        }
        let (lo, hi) = c.size_bounds();
        f.set_text(
            K::SizeCaption,
            &format!("{} ({lo}-{hi}):", labels::TARGET_SIZE),
        );
        let output = c
            .output()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(not chosen)".into());
        f.set_text(K::Output, &output);
        let phase = c.progress().map(|s| {
            if s.phase.is_empty() {
                "Starting...".to_string()
            } else {
                s.phase
            }
        });
        f.set_text(K::Phase, phase.as_deref().unwrap_or(""));
        let outcome = match c.outcome() {
            Some(Ok(())) => labels::DONE.to_string(),
            Some(Err(e)) => format!("Failed: {e}"),
            None => String::new(),
        };
        f.set_text(K::Outcome, &outcome);
        f.set_text(K::Lines, &c.lines().join("\n"));
        f.enable(K::Expand, c.can_start());
        f.enable(K::Close, !c.is_running());
        let close = if c.outcome().is_some() {
            "Close"
        } else {
            "Cancel"
        };
        f.set_title(K::Close, close);
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let text = |k: K, lines: i32| {
            Node::leaf(
                k,
                Size::Stretch(1),
                Size::Fixed((layout::ROW - 6) * lines + 4),
            )
        };
        let caption = |k: K| Node::leaf(k, Size::Fixed(170), Size::Fixed(layout::ROW - 4));
        let tree = Node::inset(
            Node::column(vec![
                text(K::SourceHead, 1),
                text(K::Facts, 4),
                text(K::TargetHead, 1),
                Node::row(vec![
                    caption(K::FormatCaption),
                    Node::leaf(K::Apm, Size::Fixed(150), Size::Fixed(layout::ROW)),
                    Node::leaf(K::Flat, Size::Stretch(1), Size::Fixed(layout::ROW)),
                ]),
                text(K::Note, 2),
                Node::row(vec![
                    caption(K::BlockCaption),
                    Node::leaf(K::Block, Size::Fixed(200), Size::Fixed(layout::ROW + 2)),
                ]),
                Node::row(vec![
                    caption(K::SizeCaption),
                    Node::leaf(K::Size, Size::Fixed(100), Size::Fixed(layout::ROW)),
                ]),
                Node::row(vec![
                    caption(K::OutCaption),
                    Node::leaf(K::Output, Size::Stretch(1), Size::Fixed(layout::ROW - 4)),
                    Node::button(K::Save, 95),
                ]),
                text(K::Phase, 1),
                text(K::Outcome, 1),
                Node::leaf(K::Lines, Size::Stretch(1), Size::Stretch(1)),
                Node::row(vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(K::Expand, 95),
                    Node::button(K::Close, 90),
                ]),
            ]),
            layout::MARGIN,
        );
        self.form.place_shown(tree, ui::area(&body));
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        let action = match k {
            K::Apm => ExpandHfsAction::SetFlatHfv(false),
            K::Flat => ExpandHfsAction::SetFlatHfv(true),
            K::Block => {
                let Some(b) = f
                    .picked(k)
                    .and_then(|i| self.blocks.borrow().get(i).map(|b| b.bytes))
                else {
                    return;
                };
                ExpandHfsAction::SetBlockSize(b)
            }
            K::Size => {
                let Ok(mib) = f.text(k).trim().parse::<u32>() else {
                    return;
                };
                ExpandHfsAction::SetSizeMib(mib)
            }
            K::Save => ExpandHfsAction::PickOutput,
            K::Expand => ExpandHfsAction::Start,
            K::Close => ExpandHfsAction::Close,
            _ => return,
        };
        self.send(action);
    }
}

impl Drop for ExpandHfsWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
