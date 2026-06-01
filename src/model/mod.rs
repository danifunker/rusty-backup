//! Model layer: non-GUI state and business logic.
//!
//! The eventual goal is for every GUI tab to be a renderer over an object in
//! this tree. See §5 / §10 of `docs/codecleanup.md`.

pub mod archive_edit;
pub mod backup_loader;
pub mod browse_session;
pub mod bulk_convert_runner;
pub mod chd_expand_runner;
pub mod edit_queue;
pub mod export_runner;
pub mod file_types;
pub mod fsck_runner;
pub mod hfs_expand_runner;
pub mod min_size_runner;
pub mod partition_editor;
pub mod physical_write_runner;
pub mod size_mode;
pub mod source_reader;
pub mod status;
pub mod update_runner;
pub mod volume_label_runner;
