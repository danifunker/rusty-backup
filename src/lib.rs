pub mod backup;
pub mod bulk_buf_reader;
pub mod cli;
pub mod clonezilla;
// Always-compiled std-API shims that differ between the modern toolchain and the
// Rust 1.73 vintage (macOS 10.7) build. Keeps lint-discouraged fallbacks out of
// the desktop tree — see src/compat.rs.
pub mod compat;
pub mod device;
pub mod error;
pub mod fs;
pub mod macarchive;
pub mod model;
#[cfg(feature = "optical")]
pub mod optical;
pub mod os;
pub mod partition;
pub mod privileged;
pub mod rbformats;
/// Theme-aware GUI colours, shared by the desktop app and the optical
/// browse view (which lives here in the lib rather than in the binary).
#[cfg(feature = "gui")]
pub mod theme;
// Network daemon + client (`rb-cli serve`, `rb://` refs). std::net + serde
// only — gated so the slim build can drop it, but cheap enough to keep on.
#[cfg(feature = "remote")]
pub mod remote;
pub mod restore;
// Std-API polyfills for the Rust 1.73 vintage (macOS 10.7) build. Compiled only
// under `rust173-polyfill`; the desktop build omits it and uses std directly.
#[cfg(feature = "rust173-polyfill")]
pub mod rust173_compat;
// Update checker uses reqwest, pulled in by either the GUI or the opt-in
// `tui-update` feature (the `rb-cli update` self-updater). The slim
// rb-cli-mini build has neither and skips it.
// The `tui` feature also needs this module for its MRU recent-files list and
// config persistence (load_recent / push_recent / UpdateConfig — pure std/serde,
// no network). The reqwest-backed update *check* inside is separately gated on
// `gui` / `tui-update`, so a plain-`tui` (e.g. vintage) build links no network.
#[cfg(any(feature = "gui", feature = "tui-update", feature = "tui"))]
pub mod update;
