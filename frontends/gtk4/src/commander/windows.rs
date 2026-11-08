//! The windows only the two-pane screen opens: the rename notice and the partition chooser.

use adw::prelude::*;
use rusty_backup::controller::commander::{
    BrowserPaneAction as P, CommanderAction as A, CommanderController, RenameNotice, Side,
};

use crate::browser::dialogs::{buttons, listing, page, window, wrapped, Slot};
use crate::browser::SendFn;
use crate::effects::EffectHost;
use crate::widgets::plain;

pub struct WindowsShown {
    notice: Option<RenameNotice>,
    partitions: Option<(Side, Vec<String>, Option<usize>)>,
}

pub struct Windows {
    host: EffectHost,
    send: SendFn,
    notice: Slot,
    partitions: Slot,
}

impl Windows {
    pub fn new(host: EffectHost, send: SendFn) -> Windows {
        Windows {
            host,
            send,
            notice: Slot::default(),
            partitions: Slot::default(),
        }
    }

    pub fn read(ctl: &CommanderController) -> WindowsShown {
        let partitions = ctl.partitions_shown().map(|side| {
            let pane = ctl.pane(side);
            (side, pane.partition_labels(), pane.selected_partition())
        });
        WindowsShown {
            notice: ctl.rename_notice().cloned(),
            partitions,
        }
    }

    pub fn show(&self, s: WindowsShown) {
        let (host, send) = (&self.host, &self.send);
        let want = s.notice.as_ref().map(|n| format!("{n:?}"));
        self.notice
            .sync(host, send, want, Some(A::RenameNoticeOk), || {
                notice_dialog(s.notice.as_ref().expect("a notice to show"), send)
            });
        let want = s.partitions.as_ref().map(|p| format!("{p:?}"));
        self.partitions
            .sync(host, send, want, Some(A::ShowPartitions(false)), || {
                let (side, labels, selected) = s.partitions.clone().expect("a pane to choose in");
                partitions_dialog(side, &labels, selected, send)
            });
    }
}

fn notice_dialog(notice: &RenameNotice, send: &SendFn) -> adw::Dialog {
    let body = page();
    for (head, lines) in [notice.renamed(), notice.dropped()].into_iter().flatten() {
        body.append(&wrapped(&head));
        body.append(&listing(&lines));
    }
    body.append(&wrapped(RenameNotice::TIP));
    let row = buttons(
        send,
        &[
            ("OK", A::RenameNoticeOk),
            ("Undo copy", A::RenameNoticeUndo),
        ],
    );
    if let Some(undo) = row.last_child() {
        undo.set_tooltip_text(Some("Remove this copy from the staged edits"));
    }
    body.append(&row);
    window(&notice.title(), 520, -1, &body)
}

fn partitions_dialog(
    side: Side,
    labels: &[String],
    selected: Option<usize>,
    send: &SendFn,
) -> adw::Dialog {
    let body = page();
    for (i, label) in labels.iter().enumerate() {
        let b = gtk::Button::with_label(&plain(label));
        if selected == Some(i) {
            b.add_css_class("suggested-action");
        }
        let send = send.clone();
        b.connect_clicked(move |_| send(A::Pane(side, P::SelectPartition(i))));
        body.append(&b);
    }
    window(
        &format!("Choose partition ({})", side.label()),
        420,
        -1,
        &body,
    )
}
