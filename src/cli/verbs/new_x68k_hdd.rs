//! `rb-cli new-x68k-hdd IMG --size 16M [--scsi] [--system-disk PATH.DIM]`
//! — Build a self-bootable Sharp X68000 HDD image with an X68K partition
//! table, IPL stub, and a blank or donor-cloned Human68k partition.
//!
//! Wraps [`crate::partition::x68k_hdd_builder::build_x68k_hdd`] — same
//! engine the `build_x68k_hdd` example uses. Both surfaces stay in lock-
//! step with the shared module, per the CLAUDE.md GUI/CLI parity rule.

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::partition::x68k_hdd_builder::{build_x68k_hdd, HddVariant};
use crate::partition::x68k_ipl::IplStub;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CliVariant {
    /// 256-byte logical sectors. Byte 0 carries the IPL stub directly
    /// (no Sharp signature header). Table at byte 0x400. Matches Hudson
    /// Soft self-bootable game discs and BlueSCSI in SASI mode.
    Sasi,
    /// 1024-byte logical sectors. Byte 0 carries the `X68SCSI1` +
    /// Keisoku Giken signature header; IPL stub at byte 0x400; table
    /// at byte 0x800. Matches BlueSCSI / ZuluSCSI and modern dev setups.
    Scsi,
}

impl From<CliVariant> for HddVariant {
    fn from(v: CliVariant) -> Self {
        match v {
            CliVariant::Sasi => HddVariant::Sasi,
            CliVariant::Scsi => HddVariant::Scsi,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CliStub {
    /// Byte 0 is `BRA.S self` (2 bytes). IPL ROM chains in, code halts.
    /// Minimum-footprint stub — useful for engine validation when you
    /// don't need any visible output.
    Halt,
    /// Byte 0 is `BRA.S +2` followed by a clean-room 68000 stub that
    /// calls IOCS `B_PRINT` (function $21 via TRAP #15) to clear the
    /// screen and print a "Rusty Backup X68000 HDD — Boot Human68k from
    /// FDD0 to mount as C:" banner before halting.
    Print,
}

impl From<CliStub> for IplStub {
    fn from(s: CliStub) -> Self {
        match s {
            CliStub::Halt => IplStub::Halt,
            CliStub::Print => IplStub::Print,
        }
    }
}

#[derive(Debug, Args)]
pub struct NewX68kHddArgs {
    /// Image file to create. Overwritten if it already exists.
    pub image: PathBuf,

    /// Disk size, accepting plain bytes or `K`/`KiB`/`M`/`MiB`/`G`/`GiB`
    /// suffixes (e.g. `8M`, `16M`). Defaults to `16M` — large enough for
    /// a full Human68k system clone plus room for user files.
    #[arg(long, default_value = "16M")]
    pub size: String,

    /// Sharp HDD controller convention to emit.
    #[arg(long, value_enum, default_value = "sasi")]
    pub variant: CliVariant,

    /// Which byte-0 IPL stub to write. `print` (default) renders a
    /// status banner via IOCS; `halt` is the bare minimum 2-byte halt
    /// loop.
    #[arg(long, value_enum, default_value = "print")]
    pub stub: CliStub,

    /// Number of Human68k partitions to carve out (1-8). The disk's
    /// data area is split equally; partition 1 (slot 0) is the one
    /// that gets `--system-disk` files and the optional
    /// `--boot-sector-donor` overlay. Other partitions are formatted
    /// blank FAT12/16. Defaults to 1 — multi-partition only matters
    /// when you want separate volumes for system / games / scratch on
    /// the same HDD.
    #[arg(long, default_value = "1")]
    pub partitions: usize,

    /// Optional donor Human68k system floppy (flat `.img` or
    /// `.dim` / `.D88` / `.xdf` / `.hdm` container). When present, the
    /// builder recursively clones every file and subdirectory from the
    /// donor into the output partition. Without this flag, three seed
    /// text files (`HELLO.TXT`, `MISTER.TXT`, `README.TXT`) are written
    /// for engine validation.
    ///
    /// After building with `--system-disk`, boot the same donor floppy
    /// from FDD0 once and run `A:\BIN\SWITCH.X /HD` to install the
    /// partition's HDD boot sector. From then on the HDD self-boots
    /// straight to `C:>`. To skip the manual `SWITCH.X` step entirely,
    /// also pass `--boot-sector-donor`.
    #[arg(long = "system-disk")]
    pub system_disk: Option<PathBuf>,

    /// Optional donor *real* Sharp X68000 SCSI HDD whose Human68k
    /// partition boot sector (Sharp IPL Copyright 1990 SHARP) we'll
    /// extract and overlay onto the output partition. Eliminates the
    /// post-build `SWITCH.X /HD` step — the HDD self-boots straight
    /// to `C:>` on every power-on.
    ///
    /// **Well-known donor**: `hd0.hds` (100 MB Sharp / Keisoku Giken
    /// SCSI HDD, file size 104,857,600 bytes). Other widely-mirrored
    /// donors that match the same pattern: `HD0.HDS`, `system.hds`.
    /// Search for the literal filename `hd0.hds` on retro-archive
    /// sites.
    ///
    /// **No size constraint**: the builder reads the donor's boot
    /// CODE verbatim but rewrites the embedded Sharp/KG BPB with the
    /// output partition's actual FAT geometry, so any `--size` between
    /// 1 MiB and ~512 MiB works (the upper bound is the Sharp/KG BPB's
    /// u8 `sectors_per_fat` field at offset `0x1D`).
    ///
    /// **SCSI variant**: validated end-to-end against `hd0.hds`
    /// (Sharp/KG `1.00`) and `hero_soft_boot.bin` (community Hero Soft
    /// V1.10). The donor's BPB is patched with the output partition's
    /// FAT geometry, so any `--size` works regardless of donor size.
    /// Well-known OEMs (`SHARP/KG    `, `Hudson...`, `Hero...`,
    /// `SxSI...`) pass silently; unfamiliar OEMs log a warning and
    /// proceed — most community SCSI formatters share the same Sharp/KG
    /// BPB layout so they boot fine after patching.
    ///
    /// **Naked-sector mode**: passing a file that's exactly one sector
    /// long (1024 bytes for SCSI, 256 for SASI) with `0x60` BRA at byte
    /// 0 skips the X68SCSI1 signature + X68K table lookup and uses the
    /// bytes verbatim. Useful for keeping a tiny portable donor
    /// (`hero_soft_boot.bin` is 1 KB) instead of carrying around a
    /// full 100+ MB HDD image.
    ///
    /// **SASI variant**: shipped experimental — accepts donors whose
    /// byte 0 carries either the `\x82w68000W` Sharp empty-marker or a
    /// `0x60` BRA opcode (self-bootable IPL discs like `Bomberman.hdf`).
    /// OEM marker can be `SHARP/KG    ` or `Hudson...` — the latter
    /// only logs a warning rather than failing. The donor's BPB ships
    /// verbatim (no patching) since we don't have a validated Sharp/KG
    /// SASI donor to develop the patch against, so for the boot to
    /// succeed the output `--size` needs to match the donor's
    /// partition size and FAT geometry exactly. If the boot hangs,
    /// fall back to `--system-disk` + the SWITCH.X workflow.
    ///
    /// Sharp's boot-sector bytes never live in the rusty-backup repo
    /// — they flow from the user's donor file into the user's output
    /// file at build time. Same license footprint as `--system-disk`.
    #[arg(long = "boot-sector-donor")]
    pub boot_sector_donor: Option<PathBuf>,
}

pub fn run(args: NewX68kHddArgs) -> Result<()> {
    let size_bytes = parse_size(&args.size).context("parsing --size")?;
    anyhow::ensure!(
        size_bytes >= 1024 * 1024,
        "--size must be at least 1 MiB (got {size_bytes} bytes)"
    );
    let size_mib = size_bytes / (1024 * 1024);
    anyhow::ensure!(
        size_bytes == size_mib * 1024 * 1024,
        "--size must be an exact multiple of 1 MiB (got {size_bytes} bytes)"
    );

    let summary = build_x68k_hdd(
        &args.image,
        size_mib,
        args.variant.into(),
        args.stub.into(),
        args.partitions,
        args.system_disk.as_deref(),
        args.boot_sector_donor.as_deref(),
    )?;

    log_stderr(format!(
        "wrote {} ({} bytes, {} MiB, {} {}-byte sectors)",
        args.image.display(),
        summary.total_bytes,
        summary.total_bytes / (1024 * 1024),
        summary.disk_sectors,
        summary.sector_size,
    ));
    log_stderr(format!(
        "  {} boot block @ byte 0 ({} bytes), table inside it",
        summary.variant.name(),
        summary.boot_block_bytes,
    ));
    log_stderr(format!(
        "  {} Human partition{} starting @ sector {} (byte {}), {} sectors / {} MiB each",
        summary.partition_count,
        if summary.partition_count == 1 {
            ""
        } else {
            "s"
        },
        summary.partition_start_sector,
        summary.partition_start_byte,
        summary.partition_sectors,
        summary.partition_bytes / (1024 * 1024),
    ));
    if summary.from_donor {
        log_stderr(format!(
            "  wrote donor: {} files + {} dirs into Human partition",
            summary.files_written, summary.dirs_written
        ));
        if summary.boot_sector_donor_applied {
            log_stderr("  -> Sharp partition boot sector overlaid from --boot-sector-donor.");
            log_stderr("     HDD self-boots straight to C:> on every power-on, no FDD0 needed.");
        } else {
            log_stderr("  -> Boot from FDD0 with the same donor floppy once, then run");
            log_stderr("     `A:\\BIN\\SWITCH.X /HD` to install the HDD boot sector.");
            log_stderr("     After that, the HDD self-boots straight to C:>.");
        }
    } else if summary.boot_sector_donor_applied {
        // Unusual but allowed combo: bootable boot-sector with seed text
        // files only (no Human68k system installed). Boot would chain to
        // donor's IPL which would fail to find HUMAN.SYS — flag it.
        log_stderr("  seeded: HELLO.TXT, MISTER.TXT, README.TXT");
        log_stderr("  WARNING: --boot-sector-donor applied without --system-disk;");
        log_stderr("           the donor's boot code expects HUMAN.SYS in the partition root.");
        log_stderr("           Add --system-disk to make the HDD actually boot.");
    } else {
        log_stderr("  seeded: HELLO.TXT, MISTER.TXT, README.TXT");
    }
    Ok(())
}
