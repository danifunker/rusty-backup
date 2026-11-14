//! The Mac shells' screens, shared by the AppKit shell (10.13+) and the vintage Cocoa shell
//! (10.4 to 10.12): see docs/cocoa_vintage_shell_plan.md § 2.
//!
//! This is not a crate. Each shell mounts it with `#[path = ".../mac-screens/src/mod.rs"] mod
//! screens;`, because the two link different engine packages under the one crate name
//! `rusty_backup`. Code here reaches its siblings through `super::` and the engine through
//! `rusty_backup::`, and stays on Rust 1.73 without let-else, proc macros or newer std, so mrustc
//! can take it to PowerPC. The vintage build compiling it on 1.73 is the check.

// Each shell uses part of this: what one leaves out is not dead in the other.
#![allow(dead_code)]

pub mod form;
pub mod host;
pub mod layout;
