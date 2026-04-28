//! Model layer: non-GUI state and business logic.
//!
//! The eventual goal is for every GUI tab to be a renderer over an object in
//! this tree. See §5 / §10 of `docs/codecleanup.md`.

pub mod archive_edit;
pub mod backup_loader;
pub mod edit_queue;
pub mod fsck_runner;
