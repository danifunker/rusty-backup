//! Network daemon + client for remote browsing / transfer (`rb-cli serve`,
//! `rb://host:port/img@N` refs).
//!
//! **Phase 0 — Family F, read-only.** The first, make-or-break slice of the
//! umbrella design in `docs/remote_transfer_plan.md`: a daemon that opens disk
//! images it holds and lets a remote `rb-cli` browse and read files inside them
//! at the operation level. The write path (stage→apply), Family B (backup
//! stream), the MiSTer TUI/service, and push-update all land in later phases.
//!
//! Gated behind the `remote` cargo feature (pure `std::net` + `serde_json`, no
//! new dependency), so the slim build can drop or keep it cheaply.

pub mod client;
pub mod fs;
pub mod protocol;
pub mod server;

pub use client::{OpenedImage, RemoteSession};
pub use fs::RemoteFilesystem;
pub use protocol::RemoteRef;
pub use server::{serve, serve_on, ServeConfig};
