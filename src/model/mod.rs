//! Model layer: non-GUI state and business logic.
//!
//! The eventual goal is for every GUI tab to be a renderer over an object in
//! this tree. See §5 / §10 of `docs/codecleanup.md`.

pub mod archive_edit;
pub mod backup_loader;
#[cfg(feature = "remote")]
pub mod backup_remote;
pub mod browse_session;
pub mod bulk_convert_runner;
pub mod cache_runner;
#[cfg(feature = "chd")]
pub mod chd_expand_runner;
pub mod checksum;
pub mod commander_descend;
pub mod commander_ops;
pub mod commander_source;
pub mod container_edit;
pub mod dir_listing;
pub mod edit_queue;
pub mod export_runner;
pub mod file_types;
pub mod fsck_runner;
pub mod hfs_expand_runner;
pub mod min_size_runner;
#[cfg(feature = "optical")]
pub mod optical_devices;
pub mod partition_editor;
pub mod physical_write_runner;
pub mod rate_tracker;
// The remote file-browser core depends on `crate::remote`, which is itself
// behind the `remote` feature.
#[cfg(feature = "remote")]
pub mod remote_browser;
pub mod repack_runner;
#[cfg(feature = "remote")]
pub mod resize_remote;
#[cfg(feature = "remote")]
pub mod restore_remote;
pub mod size_mode;
pub mod source_reader;
pub mod status;
// `update_runner` wraps the GUI's update-download/install flow on top of
// `src/update.rs` — both depend on `reqwest`, which is GUI-feature-only.
#[cfg(feature = "gui")]
pub mod update_runner;
pub mod volume_label_runner;
