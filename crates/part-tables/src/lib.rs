//! Disk partition tables and labels: parse, serialise to JSON, and write back.
//!
//! | Feature | Module | Scheme |
//! |---|---|---|
//! | `mbr` | [`mbr`] | PC MBR, with EBR logical partitions |
//! | `gpt` | [`gpt`] | GUID Partition Table |
//! | `apm` | [`apm`] | Apple Partition Map |
//! | `bsd` | [`bsd`] | BSD disklabel |
//! | `sun` | [`sun`] | Sun (SPARC) VTOC label |
//! | `sgi` | [`sgi`], [`sgi_dklabel`] | SGI volume header, IRIS 3000 disk label |
//! | `next` | [`next`] | NeXT disk label |
//! | `rdb` | [`rdb`] | Amiga Rigid Disk Block |
//! | `atari` | [`atari`] | Atari AHDI |
//! | `x68k` | [`x68k`] | Sharp X68000 (Human68k) partition table |
//! | `solaris-x86` | [`solaris_x86`] | Solaris x86 VTOC inside an MBR partition |
//! | `lvm` | [`lvm`] | AIX and HP-UX LVM volume groups |
//!
//! [`PartitionError`], [`PartitionSizeOverride`] and the [`lif`] probe are always built.
//! Detection across schemes, resizing, restore patching and disk builders are the host
//! application's job.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod lif;
mod size_override;

#[cfg(feature = "apm")]
pub mod apm;
#[cfg(feature = "atari")]
pub mod atari;
#[cfg(feature = "bsd")]
pub mod bsd;
#[cfg(feature = "gpt")]
pub mod gpt;
#[cfg(feature = "lvm")]
pub mod lvm;
#[cfg(feature = "mbr")]
pub mod mbr;
#[cfg(feature = "next")]
pub mod next;
#[cfg(feature = "rdb")]
pub mod rdb;
#[cfg(feature = "sgi")]
pub mod sgi;
#[cfg(feature = "sgi")]
pub mod sgi_dklabel;
#[cfg(feature = "solaris-x86")]
pub mod solaris_x86;
#[cfg(feature = "sun")]
pub mod sun;
#[cfg(feature = "x68k")]
pub mod x68k;

pub use size_override::PartitionSizeOverride;

/// Why a partition table could not be read or written.
#[derive(thiserror::Error, Debug)]
pub enum PartitionError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid MBR: {0}")]
    InvalidMbr(String),

    #[error("Invalid GPT: {0}")]
    InvalidGpt(String),

    #[error("Invalid APM: {0}")]
    InvalidApm(String),

    #[error("Invalid SGI Volume Header: {0}")]
    InvalidSgi(String),

    #[error("Invalid RDB: {0}")]
    InvalidRdb(String),

    #[error("Invalid AHDI: {0}")]
    InvalidAhdi(String),
}
