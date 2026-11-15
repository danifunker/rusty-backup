//! The Mac shells' screens, shared by the AppKit shell (10.13+) and the vintage Cocoa shell
//! (10.4 to 10.12): see docs/cocoa_vintage_shell_plan.md § 2.
//!
//! This is not a crate. Each shell mounts it with `#[path = ".../mac-screens/src/mod.rs"] mod
//! screens;`, because the two link different engine packages under the one crate name
//! `rusty_backup`. Code here reaches its siblings through `super::` and the engine through
//! `rusty_backup::`, and stays on Rust 1.73 with no proc macros or newer std, so mrustc can take
//! it to PowerPC (let-else is fine: the engine has hundreds). The vintage 1.73 build is the check.

// Each shell uses part of this: what one leaves out is not dead in the other.
#![allow(dead_code)]

pub mod alto;
pub mod archives;
pub mod backup;
pub mod backup_remote;
pub mod backup_vhd;
pub mod browse;
pub mod build_disk;
pub mod build_rows;
// CHD and optical media need prebuilt libraries the vintage build cannot link; see the plan § 8.
#[cfg(feature = "chd")]
pub mod chd;
#[cfg(not(feature = "chd"))]
#[path = "chd_off.rs"]
pub mod chd;
pub mod commander;
pub mod convert;
pub mod disk_write;
pub mod editor;
pub mod effects;
pub mod expand_hfs;
pub mod export;
pub mod form;
pub mod host;
pub mod inspect;
pub mod jobs;
pub mod layout;
pub mod main_window;
pub mod model_window;
#[cfg(feature = "optical")]
pub mod optical;
#[cfg(feature = "optical")]
pub mod optical_remote;
pub mod pane_view;
pub mod partition_bar;
pub mod popup;
pub mod prompt;
pub mod remote_pick;
pub mod report;
pub mod resize;
pub mod resize_image;
pub mod restore;
pub mod settings;
pub mod sync;
