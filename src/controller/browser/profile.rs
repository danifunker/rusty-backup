//! What one use of the browser pane may do: its sources, its editing model and its tools.
//!
//! A profile is plain data the pane checks before acting; a capability that is off refuses
//! its action with a message (see [`PaneProfile::refusal`]). Views read it to decide what
//! to draw (a source bar, an Apply button, a detail panel), never to decide what an input
//! means. The design is in docs/phase2_commander_plan.md § "Slice 10: one browser pane".

/// The sources a user may open from the pane itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sources {
    /// Disk images, Mac archives and optical disc images.
    pub image: bool,
    pub host: bool,
    /// Native rusty-backup and Clonezilla backup folders.
    pub backup: bool,
    /// An `rb-cli serve` daemon (cfg `remote`).
    pub remote: bool,
}

impl Sources {
    pub const ALL: Sources = Sources {
        image: true,
        host: true,
        backup: true,
        remote: true,
    };
    /// The host screen chooses the source; the user cannot open another.
    pub const NONE: Sources = Sources {
        image: false,
        host: false,
        backup: false,
        remote: false,
    };

    pub fn any(self) -> bool {
        self.image || self.host || self.backup || self.remote
    }
}

/// Volume tools a pane may offer on its open filesystem, beyond browsing and staging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Tools {
    /// Check the filesystem (read-only).
    pub fsck: bool,
    /// Repair the filesystem in place.
    pub repair: bool,
    /// Bless a folder as the bootable System Folder (HFS / HFS+).
    pub bless: bool,
    /// Open a text file in an editor and save it back.
    pub text_edit: bool,
    /// Import a host file into the current folder.
    pub import: bool,
    /// Edit type / creator / dates / mode / owner of the cursor row.
    pub metadata: bool,
    /// Whole-image transforms handed to the shell's command line (convert, resize, ...).
    pub transforms: bool,
}

/// How a pane lists its volume; the profile picks the default and the user can switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListingStyle {
    /// One expandable tree of folders and files (the Inspect browse view).
    CombinedTree,
    /// The current folder's contents with `..` (a Commander pane).
    FlatList,
    /// A folder tree beside the current folder's list (the TUI Explorer).
    TreeAndList,
}

impl ListingStyle {
    pub const ALL: [ListingStyle; 3] = [
        ListingStyle::CombinedTree,
        ListingStyle::FlatList,
        ListingStyle::TreeAndList,
    ];

    /// The View menu's label.
    pub fn label(self) -> &'static str {
        match self {
            ListingStyle::CombinedTree => "Combined tree",
            ListingStyle::FlatList => "Flat list",
            ListingStyle::TreeAndList => "Folder tree + list",
        }
    }

    /// The name stored in config.json.
    pub fn key(self) -> &'static str {
        match self {
            ListingStyle::CombinedTree => "combined-tree",
            ListingStyle::FlatList => "flat-list",
            ListingStyle::TreeAndList => "tree-and-list",
        }
    }

    pub fn from_key(key: &str) -> Option<ListingStyle> {
        ListingStyle::ALL.iter().copied().find(|s| s.key() == key)
    }
}

/// Where a pane's File Info goes: beside the listing following the cursor, or a window on request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailPlacement {
    SidePanel,
    OnRequest,
}

/// One use of the browser pane: which sources, how edits land, which tools.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaneProfile {
    /// For messages and tests: `"commander"`, `"explorer"`, `"inspect"`.
    pub name: &'static str,
    pub sources: Sources,
    /// The host screen opened the source: closing it or picking another partition is refused.
    pub fixed_source: bool,
    /// Edits wait in the queue for Apply; when false each staged edit applies at once.
    pub staging: bool,
    /// Nothing may be staged or written (a view-only browser).
    pub read_only: bool,
    pub rename: bool,
    pub new_folder: bool,
    pub delete: bool,
    /// Ask before staging the delete of a folder that holds anything.
    pub confirm_folder_delete: bool,
    /// Archives and disk images expand in place under their row.
    pub wrappers: bool,
    /// The listing style a new pane starts in (a remembered choice replaces it).
    pub listing: ListingStyle,
    /// The styles the user may switch to.
    pub styles: &'static [ListingStyle],
    pub detail: DetailPlacement,
    pub tools: Tools,
}

impl PaneProfile {
    /// Each Commander pane: any source, staged edits, wrappers, no volume tools.
    pub const fn commander() -> Self {
        PaneProfile {
            name: "commander",
            sources: Sources::ALL,
            fixed_source: false,
            staging: true,
            read_only: false,
            rename: true,
            new_folder: true,
            delete: true,
            confirm_folder_delete: false,
            wrappers: true,
            listing: ListingStyle::FlatList,
            styles: &ListingStyle::ALL,
            detail: DetailPlacement::OnRequest,
            tools: Tools {
                fsck: false,
                repair: false,
                bless: false,
                text_edit: false,
                import: false,
                metadata: false,
                transforms: false,
            },
        }
    }

    /// The TUI Explorer: one partition chosen on the Inspect tab, edits written at once, every tool.
    pub const fn explorer() -> Self {
        PaneProfile {
            name: "explorer",
            sources: Sources::NONE,
            fixed_source: true,
            staging: false,
            read_only: false,
            rename: true,
            new_folder: true,
            delete: true,
            confirm_folder_delete: false,
            wrappers: true,
            listing: ListingStyle::TreeAndList,
            styles: &ListingStyle::ALL,
            detail: DetailPlacement::OnRequest,
            tools: Tools {
                fsck: true,
                repair: true,
                bless: true,
                text_edit: true,
                import: true,
                metadata: true,
                transforms: true,
            },
        }
    }

    /// The Inspect tab's browse view: the selected partition, staged edits applied by the user.
    pub const fn inspect() -> Self {
        PaneProfile {
            name: "inspect",
            sources: Sources::NONE,
            fixed_source: true,
            staging: true,
            read_only: false,
            rename: true,
            new_folder: true,
            delete: true,
            confirm_folder_delete: true,
            wrappers: true,
            listing: ListingStyle::CombinedTree,
            styles: &ListingStyle::ALL,
            detail: DetailPlacement::SidePanel,
            tools: Tools {
                fsck: false,
                repair: false,
                bless: true,
                text_edit: true,
                import: true,
                metadata: true,
                transforms: false,
            },
        }
    }

    /// The same profile with nothing writable, for a view-only browser.
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self.tools.repair = false;
        self.tools.bless = false;
        self.tools.text_edit = false;
        self.tools.import = false;
        self.tools.metadata = false;
        self
    }

    /// The message an action gets when this profile turns its capability off.
    pub fn refusal(&self, what: &str) -> String {
        format!("{what} is not available in this view.")
    }

    pub fn allows(&self, style: ListingStyle) -> bool {
        self.styles.contains(&style)
    }

    pub fn can_rename(&self) -> bool {
        self.rename && !self.read_only
    }

    pub fn can_new_folder(&self) -> bool {
        self.new_folder && !self.read_only
    }

    pub fn can_delete(&self) -> bool {
        self.delete && !self.read_only
    }
}

impl Default for PaneProfile {
    fn default() -> Self {
        PaneProfile::commander()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_commander_profile_offers_everything_commander_did() {
        let p = PaneProfile::commander();
        assert_eq!(p.sources, Sources::ALL);
        assert!(p.staging && !p.fixed_source && !p.read_only && p.wrappers);
        assert!(p.can_rename() && p.can_new_folder() && p.can_delete());
        assert_eq!(p.tools, Tools::default());
        assert_eq!(p.listing, ListingStyle::FlatList);
        assert_eq!(p.detail, DetailPlacement::OnRequest);
    }

    #[test]
    fn each_profile_keeps_its_look_by_default() {
        assert_eq!(PaneProfile::explorer().listing, ListingStyle::TreeAndList);
        let inspect = PaneProfile::inspect();
        assert_eq!(inspect.listing, ListingStyle::CombinedTree);
        assert_eq!(inspect.detail, DetailPlacement::SidePanel);
        for s in ListingStyle::ALL {
            assert_eq!(ListingStyle::from_key(s.key()), Some(s));
        }
    }

    #[test]
    fn read_only_turns_every_write_off() {
        let p = PaneProfile::explorer().read_only();
        assert!(!p.can_rename() && !p.can_new_folder() && !p.can_delete());
        assert!(p.tools.fsck && !p.tools.repair && !p.tools.import && !p.tools.text_edit);
    }
}
