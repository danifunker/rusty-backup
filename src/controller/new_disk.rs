//! The New Disk wizard's plain data. The TUI wizard still runs the `rb-cli new` verbs itself;
//! a controller owning the wizard (shared with a GUI one) is Phase 2 parity work.

pub use crate::partition::mac_cd_builder::MacCdFs;
pub use crate::partition::sgi_hdd_builder::{DEFAULT_HEADS, DEFAULT_SECTORS_PER_TRACK};
