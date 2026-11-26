//! The CLI's view of partition resolution: the engine lives in [`crate::model::resolve`]; the
//! `--fs-type` / `--carve-full` flag group is CLI grammar, so it stays here.

pub use crate::model::resolve::*;

/// Flatten-able CLI flag group: an explicit filesystem-dispatch override.
///
/// CP/M floppies (Altair, Amstrad, PCW, Einstein, SVI328, MultiComp, ZX
/// +3) carry NO on-disk signature for their FS — the BIOS knows the
/// Disk Parameter Block (DPB) out-of-band. So `open_filesystem` can't
/// autodetect them, and every rb-cli verb operating on a CP/M image
/// needs the user to declare the DPB via `--fs-type cpm:<preset_name>`.
///
/// The same flag can in principle override dispatch for any other
/// `partition_type_string` the engine recognises (e.g. `human68k`,
/// `qdos`, `Apple_HFS`), but the headline use is the CP/M family.
///
/// Flatten into a verb's `Args` and call
/// [`FsDispatchOverride::apply`] right after `resolve_partition_*`.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct FsDispatchOverride {
    /// Force a specific filesystem dispatch. The main use is `cpm:<preset>`
    /// for CP/M images (which have no on-disk signature). Valid CP/M
    /// presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`,
    /// `svi328_cpm`, `altair_8in`, `ibm_3740` (8" with skew 6), `altair_cf`, `multicomp`, `zxplus3`.
    /// Other strings (e.g. `human68k`, `qdos`) are also accepted and
    /// forwarded to the partition_type_string dispatch.
    #[arg(long = "fs-type", value_name = "TYPE")]
    pub fs_type: Option<String>,

    /// Scan the **entire** image for recoverable text in the synthetic carve
    /// view (used for disks with no recognized filesystem — e.g. custom
    /// bootblock Amiga "NDOS" disks). By default the carve view only scans
    /// the first 10 MB. No effect on disks with a real filesystem.
    #[arg(long = "carve-full")]
    pub carve_full: bool,
}

impl FsDispatchOverride {
    /// Apply the override to a resolved [`PartitionContext`] in place.
    /// No-op when `--fs-type` wasn't passed.
    ///
    /// The override replaces `ctx.type_string` and clears `ctx.type_byte`
    /// so the string-dispatch branch in `open_filesystem` wins. Updates
    /// `ctx.label` so the user can confirm the override is in effect.
    ///
    /// Also installs the process-wide carve scan policy from `--carve-full`
    /// so a subsequent carve open scans the whole image instead of the
    /// default first-10-MB window.
    pub fn apply(&self, ctx: &mut PartitionContext) {
        crate::fs::carve::set_full_scan(self.carve_full);
        if let Some(t) = &self.fs_type {
            crate::model::resolve::apply_fs_type(ctx, t);
        }
    }
}
