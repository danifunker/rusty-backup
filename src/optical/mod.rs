// `browse_view` is an egui widget; it only exists in GUI builds. The CLI/mini
// build enables `optical` without `gui`, so gate it to keep that build green.
#[cfg(feature = "gui")]
pub mod browse_view;
pub mod convert;
pub mod rip;

pub use convert::ConvertProgress;
pub use rip::{run_rip, RipConfig, RipFormat, RipProgress};
