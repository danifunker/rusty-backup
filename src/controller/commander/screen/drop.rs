//! What files dropped from the file manager onto a pane do: open there, copy in, or ask.
//!
//! A pane with nothing open, a host folder or a read-only source opens the first item as its
//! source. A pane holding a writable volume takes the items as a copy into its folder, staged
//! like any edit. Only a single disk image dropped on a writable volume is unclear (copy the
//! file in, or open it instead), and only then does the view ask.

use std::path::{Path, PathBuf};

use super::{BrowserPaneAction as P, CommanderAction as A, CommanderController, Side};
use crate::model::file_types::DISK_IMAGE_EXTS;

/// What a drop does; a view carries out `Do`, or asks `Ask` and carries out the answer's actions.
#[derive(Debug, Clone)]
pub enum DropPlan {
    Do(Vec<A>),
    Ask {
        title: String,
        question: String,
        /// Each answer's label and what it sends; the view adds Cancel.
        choices: Vec<(String, Vec<A>)>,
    },
}

/// The name looks like a disk image this app opens.
fn is_image(path: &Path) -> bool {
    let ext = path
        .extension()
        .map(|e| e.to_string_lossy().to_ascii_lowercase());
    ext.is_some_and(|e| DISK_IMAGE_EXTS.iter().any(|d| d.eq_ignore_ascii_case(&e)))
}

impl CommanderController {
    /// What dropping `paths` on `side`'s pane does; `None` for an empty drop.
    pub fn drop_plan(&self, side: Side, paths: &[PathBuf]) -> Option<DropPlan> {
        let first = paths.first()?.clone();
        let pane = self.pane(side);
        let open = vec![A::Focus(side), A::OpenDropped(first.clone())];
        let into = pane.can_receive() && !pane.listing.is_host();
        if !into {
            return Some(DropPlan::Do(open));
        }
        let add = vec![A::Focus(side), A::Pane(side, P::AddPicked(paths.to_vec()))];
        if paths.len() > 1 || !is_image(&first) {
            return Some(DropPlan::Do(add));
        }
        let name = first
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| first.display().to_string());
        Some(DropPlan::Ask {
            title: "Dropped disk image".into(),
            question: format!(
                "\"{name}\" is a disk image. Copy it into {} as a file, or open it in this pane \
                 instead?",
                pane.volume_title()
            ),
            choices: vec![("Copy it in".into(), add), ("Open it here".into(), open)],
        })
    }
}
