pub mod backup;
pub mod bulk_buf_reader;
pub mod cli;
pub mod clonezilla;
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
// Network daemon + client (`rb-cli serve`, `rb://` refs). std::net + serde
// only — gated so the slim build can drop it, but cheap enough to keep on.
#[cfg(feature = "remote")]
pub mod remote;
pub mod restore;
// Update checker uses reqwest, pulled in by either the GUI or the opt-in
// `tui-update` feature (the `rb-cli update` self-updater). The slim
// rb-cli-mini build has neither and skips it.
#[cfg(any(feature = "gui", feature = "tui-update"))]
pub mod update;
