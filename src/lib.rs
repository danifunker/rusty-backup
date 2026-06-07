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
pub mod restore;
// Update checker uses reqwest, which is GUI-feature-only — the slim
// rb-cli-mini build skips both. The desktop binary still self-updates.
#[cfg(feature = "gui")]
pub mod update;
