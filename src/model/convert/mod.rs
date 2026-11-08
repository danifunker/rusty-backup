//! Convert: one image, or every image in a folder, to another format (docs/convert_tab.md).
//!
//! [`classify`] tells a hard disk from a floppy from a disc by its contents, then its extension.
//! Each kind has its own [`Format`] list ([`formats_for`]); [`Format::fits`] says whether one
//! source can go to a format. [`scan`] lists a folder, and [`run::convert_one`] converts one file,
//! shrinking a hard disk ([`Sizing`]) or refitting a floppy ([`FloppySize`]) on the way, then
//! optionally verifying it ([`verify`]).

pub mod classify;
pub mod refit;
pub mod run;
pub mod verify;

use std::path::{Path, PathBuf};

pub use classify::{classify, Classified, DiscSource};

use crate::rbformats::export::ExportFormat;

/// What a source image holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageKind {
    HardDisk,
    Floppy,
    Disc,
}

impl ImageKind {
    pub const ALL: [ImageKind; 3] = [ImageKind::HardDisk, ImageKind::Floppy, ImageKind::Disc];

    pub const fn label(self) -> &'static str {
        match self {
            ImageKind::HardDisk => "Hard disk",
            ImageKind::Floppy => "Floppy",
            ImageKind::Disc => "Disc",
        }
    }

    /// The format a new form starts on for this kind.
    pub const fn default_format(self) -> Format {
        match self {
            ImageKind::HardDisk => Format::Vhd,
            ImageKind::Floppy => Format::Raw,
            ImageKind::Disc => Format::ChdCd,
        }
    }
}

/// An output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    Raw,
    Vhd,
    VhdDynamic,
    Qcow2,
    VmdkFlat,
    VmdkSparse,
    TwoMg,
    Chd,
    Woz,
    Moof,
    Dc42,
    Xdf,
    Hdm,
    Dim,
    D88,
    ChdCd,
    ChdDvd,
    BinCue,
    Iso,
}

/// The hard disk formats, in list order.
pub const HARD_DISK_FORMATS: &[Format] = &[
    Format::Vhd,
    Format::VhdDynamic,
    Format::Qcow2,
    Format::VmdkFlat,
    Format::VmdkSparse,
    Format::Raw,
    Format::TwoMg,
    Format::Chd,
];

/// The floppy formats, in list order.
pub const FLOPPY_FORMATS: &[Format] = &[
    Format::Raw,
    Format::TwoMg,
    Format::Woz,
    Format::Moof,
    Format::Dc42,
    Format::Xdf,
    Format::Hdm,
    Format::Dim,
    Format::D88,
];

/// The disc formats, in list order.
pub const DISC_FORMATS: &[Format] = &[Format::ChdCd, Format::ChdDvd, Format::BinCue, Format::Iso];

/// The formats a kind can go to.
pub fn formats_for(kind: ImageKind) -> &'static [Format] {
    match kind {
        ImageKind::HardDisk => HARD_DISK_FORMATS,
        ImageKind::Floppy => FLOPPY_FORMATS,
        ImageKind::Disc => DISC_FORMATS,
    }
}

const NO_CHD: &str = "This copy was built without CHD support.";
const NO_OPTICAL: &str = "This copy was built without optical disc support.";

/// The standard floppy sizes Apple's bitstream formats take.
const APPLE_GCR_SIZES: &[u64] = &[143_360, 409_600, 819_200];

impl Format {
    pub const fn label(self) -> &'static str {
        match self {
            Format::Raw => "Raw (.img)",
            Format::Vhd => "VHD",
            Format::VhdDynamic => "VHD (Dynamic)",
            Format::Qcow2 => "QCOW2",
            Format::VmdkFlat => "VMDK (Flat)",
            Format::VmdkSparse => "VMDK (Sparse)",
            Format::TwoMg => "2MG (.2mg)",
            Format::Chd => "CHD (Hard Disk)",
            Format::Woz => "WOZ (.woz)",
            Format::Moof => "MOOF (.moof)",
            Format::Dc42 => "DiskCopy 4.2 (.dsk)",
            Format::Xdf => "XDF (.xdf)",
            Format::Hdm => "HDM (.hdm)",
            Format::Dim => "DIM (.dim)",
            Format::D88 => "D88 (.d88)",
            Format::ChdCd => "CD CHD",
            Format::ChdDvd => "DVD CHD",
            Format::BinCue => "BIN/CUE",
            Format::Iso => "ISO",
        }
    }

    /// The output extension a new form starts on.
    pub const fn extension(self) -> &'static str {
        match self {
            Format::Raw => "img",
            Format::Vhd | Format::VhdDynamic => "vhd",
            Format::Qcow2 => "qcow2",
            Format::VmdkFlat | Format::VmdkSparse => "vmdk",
            Format::TwoMg => "2mg",
            Format::Chd | Format::ChdCd | Format::ChdDvd => "chd",
            Format::Woz => "woz",
            Format::Moof => "moof",
            Format::Dc42 => "dsk",
            Format::Xdf => "xdf",
            Format::Hdm => "hdm",
            Format::Dim => "dim",
            Format::D88 => "d88",
            Format::BinCue => "cue",
            Format::Iso => "iso",
        }
    }

    /// What the format is for, in plain words.
    pub const fn hint(self) -> &'static str {
        match self {
            Format::Raw => "The disk's bytes and nothing else. Every tool and emulator reads it.",
            Format::Vhd => {
                "Raw bytes with a small footer. Windows, Hyper-V, VirtualBox and QEMU open it."
            }
            Format::VhdDynamic => {
                "A VHD that leaves out empty space, so it is smaller. Same programs as VHD."
            }
            Format::Qcow2 => {
                "QEMU's own format; leaves out empty space. UTM and virt-manager use it."
            }
            Format::VmdkFlat => "VMware's format as a small descriptor plus the raw bytes.",
            Format::VmdkSparse => "VMware's format in one file, leaving out empty space.",
            Format::TwoMg => "Apple II emulators' format: a short header plus the raw bytes.",
            Format::Chd => "MAME's compressed format for hard disks. Usually much smaller.",
            Format::Woz => "Apple II and Mac floppy bitstream: 140K, 400K and 800K disks.",
            Format::Moof => "Macintosh floppy bitstream (Applesauce, Snow): 400K and 800K disks.",
            Format::Dc42 => "Apple's DiskCopy 4.2: 400K, 720K, 800K and 1.44 MB disks.",
            Format::Xdf => "Sharp X68000 floppy dump: 640K, 720K, 1.2 MB and 1.44 MB disks.",
            Format::Hdm => "PC-98 floppy dump, the same layout as XDF.",
            Format::Dim => "DiskExplorer DIM: a header plus the disk. 720K, 1.2 MB and 1.44 MB.",
            Format::D88 => "Sharp D88 track table (X68000, PC-88, PC-98, FM-7 emulators).",
            Format::ChdCd => "MAME's compressed format for CDs. Made from an ISO or a BIN/CUE.",
            Format::ChdDvd => "MAME's compressed format for DVDs (MAME 0.287 and later).",
            Format::BinCue => "A raw track file plus a cue sheet. Most CD emulators read it.",
            Format::Iso => "The data track as plain 2048-byte sectors. Audio tracks are left out.",
        }
    }

    /// Why this build cannot write the format, if it cannot.
    pub fn unavailable(self) -> Option<&'static str> {
        let chd = cfg!(feature = "chd");
        let optical = cfg!(feature = "optical");
        match self {
            Format::Chd | Format::ChdCd | Format::ChdDvd if !chd => Some(NO_CHD),
            Format::Iso | Format::BinCue if !optical && !chd => Some(NO_OPTICAL),
            _ => None,
        }
    }

    /// The export engine's name for the format, where it writes it.
    pub fn export_format(self) -> Option<ExportFormat> {
        Some(match self {
            Format::Raw => ExportFormat::Raw,
            Format::Vhd => ExportFormat::Vhd,
            Format::VhdDynamic => ExportFormat::VhdDynamic,
            Format::Qcow2 => ExportFormat::Qcow2,
            Format::VmdkFlat => ExportFormat::VmdkFlat,
            Format::VmdkSparse => ExportFormat::VmdkSparse,
            Format::TwoMg => ExportFormat::TwoMg,
            Format::Chd => ExportFormat::Chd,
            Format::Woz => ExportFormat::Woz,
            Format::Moof => ExportFormat::Moof,
            Format::Dc42 => ExportFormat::Dc42,
            Format::Xdf => ExportFormat::Xdf,
            Format::Hdm => ExportFormat::Hdm,
            Format::Dim => ExportFormat::Dim,
            Format::D88 => ExportFormat::D88,
            Format::ChdCd => ExportFormat::ChdCd,
            Format::ChdDvd => ExportFormat::ChdDvd,
            Format::BinCue => ExportFormat::BinCue,
            Format::Iso => return None,
        })
    }

    /// The format's id for the command line (`--format vhd`).
    pub const fn id(self) -> &'static str {
        match self {
            Format::Raw => "raw",
            Format::Vhd => "vhd",
            Format::VhdDynamic => "vhd-dynamic",
            Format::Qcow2 => "qcow2",
            Format::VmdkFlat => "vmdk-flat",
            Format::VmdkSparse => "vmdk-sparse",
            Format::TwoMg => "2mg",
            Format::Chd => "chd",
            Format::Woz => "woz",
            Format::Moof => "moof",
            Format::Dc42 => "dc42",
            Format::Xdf => "xdf",
            Format::Hdm => "hdm",
            Format::Dim => "dim",
            Format::D88 => "d88",
            Format::ChdCd => "chd-cd",
            Format::ChdDvd => "chd-dvd",
            Format::BinCue => "bincue",
            Format::Iso => "iso",
        }
    }

    pub fn from_id(id: &str) -> Option<Format> {
        ALL_FORMATS
            .iter()
            .copied()
            .find(|f| f.id().eq_ignore_ascii_case(id))
    }

    /// Why `source` cannot go to this format; `size` is the floppy's size after any refit.
    pub fn fits(self, source: &Classified, size: u64) -> Result<(), String> {
        if let Some(why) = self.unavailable() {
            return Err(why.to_string());
        }
        if !formats_for(source.kind).contains(&self) {
            return Err(format!(
                "{} is not a format for a {}.",
                self.label(),
                source.kind.label().to_ascii_lowercase()
            ));
        }
        let kb = size / 1024;
        match self {
            Format::Woz if !APPLE_GCR_SIZES.contains(&size) => Err(format!(
                "WOZ takes 140K, 400K and 800K disks; this one is {kb}K."
            )),
            Format::Moof if size != 409_600 && size != 819_200 => Err(format!(
                "MOOF takes 400K and 800K disks; this one is {kb}K."
            )),
            Format::Dc42 if ![409_600, 737_280, 819_200, 1_474_560].contains(&size) => {
                Err(format!(
                    "DiskCopy 4.2 takes 400K, 720K, 800K and 1.44 MB disks; this one is {kb}K."
                ))
            }
            Format::Xdf | Format::Hdm | Format::D88
                if dsk_floppy::floppy_geom::infer_media_from_size(size as usize).is_none() =>
            {
                Err(format!(
                    "{} takes 640K, 720K, 1.2 MB and 1.44 MB disks; this one is {kb}K.",
                    self.label()
                ))
            }
            Format::Dim
                if !matches!(
                    dsk_floppy::floppy_geom::infer_media_from_size(size as usize),
                    Some(m) if m != dsk_floppy::floppy_geom::FloppyMedia::Dd640
                ) =>
            {
                Err(format!(
                    "DIM takes 720K, 1.2 MB and 1.44 MB disks; this one is {kb}K."
                ))
            }
            _ if source.kind == ImageKind::Disc => disc_fits(self, source.disc),
            _ => Ok(()),
        }
    }
}

/// Every format, for lookups by id.
pub const ALL_FORMATS: &[Format] = &[
    Format::Raw,
    Format::Vhd,
    Format::VhdDynamic,
    Format::Qcow2,
    Format::VmdkFlat,
    Format::VmdkSparse,
    Format::TwoMg,
    Format::Chd,
    Format::Woz,
    Format::Moof,
    Format::Dc42,
    Format::Xdf,
    Format::Hdm,
    Format::Dim,
    Format::D88,
    Format::ChdCd,
    Format::ChdDvd,
    Format::BinCue,
    Format::Iso,
];

fn disc_fits(format: Format, source: Option<DiscSource>) -> Result<(), String> {
    use DiscSource::*;
    let Some(source) = source else {
        return Err("The disc's format could not be read.".to_string());
    };
    // These three go through the optical disc converter.
    let needs_optical = matches!(
        (source, format),
        (Iso, Format::BinCue) | (Cue, Format::Iso) | (CdChd, Format::Iso)
    );
    let routed = needs_optical
        || matches!(
            (source, format),
            (Iso, Format::ChdCd | Format::ChdDvd)
                | (Cue, Format::ChdCd)
                | (CdChd, Format::BinCue)
                | (DvdChd, Format::Iso)
        );
    let ok = routed && (!needs_optical || cfg!(feature = "optical"));
    if ok {
        return Ok(());
    }
    let same = matches!(
        (source, format),
        (Iso, Format::Iso)
            | (Cue, Format::BinCue)
            | (CdChd, Format::ChdCd)
            | (DvdChd, Format::ChdDvd)
    );
    Err(if same {
        format!("It is already {}.", format.label())
    } else if format == Format::ChdDvd {
        "A DVD CHD is made from an ISO.".to_string()
    } else if matches!(format, Format::Iso | Format::BinCue) && !cfg!(feature = "optical") {
        NO_OPTICAL.to_string()
    } else {
        format!(
            "A {} cannot be made from a {}.",
            format.label(),
            source.label()
        )
    })
}

/// How a hard disk's partitions are sized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Sizing {
    #[default]
    Keep,
    Minimum,
    /// The minimum plus this much on every partition.
    Extra(Extra),
}

/// Room added to every partition's minimum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Extra {
    Mib(u32),
    Percent(u32),
}

impl Sizing {
    pub const fn resizes(self) -> bool {
        !matches!(self, Sizing::Keep)
    }

    /// A partition's new size: never below `minimum`, above `original` or above `cap`.
    pub fn size_for(self, original: u64, minimum: u64, cap: Option<u64>) -> u64 {
        let minimum = minimum.min(original);
        let wanted = match self {
            Sizing::Keep => return original,
            Sizing::Minimum => minimum,
            Sizing::Extra(Extra::Mib(n)) => minimum.saturating_add(n as u64 * 1024 * 1024),
            Sizing::Extra(Extra::Percent(p)) => {
                minimum.saturating_add(minimum / 100 * p as u64 + minimum % 100 * p as u64 / 100)
            }
        };
        let limit = cap.map_or(original, |c| c.min(original)).max(minimum);
        wanted.div_ceil(512).saturating_mul(512).min(limit)
    }

    /// The choice as the log and Review show it.
    pub fn describe(self) -> String {
        match self {
            Sizing::Keep => "keep size".to_string(),
            Sizing::Minimum => "minimum".to_string(),
            Sizing::Extra(Extra::Mib(n)) => format!("minimum + {n} MB"),
            Sizing::Extra(Extra::Percent(p)) => format!("minimum + {p}%"),
        }
    }

    /// Parse `keep`, `minimum`, `minimum+64MB` or `minimum+10%` (the command line's spelling).
    pub fn parse(text: &str) -> Result<Sizing, String> {
        let t = text.trim().to_ascii_lowercase();
        match t.as_str() {
            "keep" | "same" => return Ok(Sizing::Keep),
            "minimum" | "min" => return Ok(Sizing::Minimum),
            _ => {}
        }
        let rest = t
            .strip_prefix("minimum+")
            .or_else(|| t.strip_prefix("min+"))
            .ok_or_else(|| format!("'{text}': use keep, minimum, minimum+64MB or minimum+10%"))?;
        let (num, pct) = match rest.strip_suffix('%') {
            Some(n) => (n, true),
            None => (rest.trim_end_matches("mb").trim_end_matches('m'), false),
        };
        let n: u32 = num
            .trim()
            .parse()
            .map_err(|_| format!("'{text}': the extra amount is not a whole number"))?;
        Ok(Sizing::Extra(if pct {
            Extra::Percent(n)
        } else {
            Extra::Mib(n)
        }))
    }
}

/// A floppy's output size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FloppySize {
    #[default]
    Same,
    /// Refit to this many bytes, one of [`refit::SIZES`].
    Refit(u64),
}

impl FloppySize {
    pub fn label(self) -> String {
        match self {
            FloppySize::Same => "Same size".to_string(),
            FloppySize::Refit(bytes) => refit::size_label(bytes).to_string(),
        }
    }

    /// Parse `same` or a size from [`refit::SIZES`] such as `720k` or `1.44m`.
    pub fn parse(text: &str) -> Result<FloppySize, String> {
        let t = text.trim().to_ascii_lowercase();
        if t == "same" || t == "keep" {
            return Ok(FloppySize::Same);
        }
        refit::SIZES
            .iter()
            .find(|s| s.ids.contains(&t.as_str()))
            .map(|s| FloppySize::Refit(s.bytes))
            .ok_or_else(|| {
                let ids: Vec<&str> = refit::SIZES.iter().map(|s| s.ids[0]).collect();
                format!("'{text}': use same or one of {}", ids.join(", "))
            })
    }
}

/// Everything a run needs besides the files.
#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    /// The format and output extension for each kind.
    pub hard_disk: (Format, String),
    pub floppy: (Format, String),
    pub disc: (Format, String),
    pub sizing: Sizing,
    pub measure_slow: bool,
    pub floppy_size: FloppySize,
    pub verify: bool,
    /// BIN/CUE only: one .bin per track.
    pub multi_bin: bool,
    pub chd: ChdChoices,
}

/// The CHD codec and hunk choices for each CHD format; `None` takes chdman's defaults.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChdChoices {
    pub hd: Option<crate::rbformats::chd_options::ChdOptions>,
    pub cd: Option<crate::rbformats::chd_options::ChdOptions>,
    pub dvd: Option<crate::rbformats::chd_options::ChdOptions>,
}

impl Default for Options {
    fn default() -> Self {
        let pick = |k: ImageKind| {
            (
                k.default_format(),
                k.default_format().extension().to_string(),
            )
        };
        Options {
            hard_disk: pick(ImageKind::HardDisk),
            floppy: pick(ImageKind::Floppy),
            disc: pick(ImageKind::Disc),
            sizing: Sizing::Keep,
            measure_slow: false,
            floppy_size: FloppySize::Same,
            verify: false,
            multi_bin: false,
            chd: ChdChoices::default(),
        }
    }
}

impl Options {
    /// The format and extension a kind goes to.
    pub fn target(&self, kind: ImageKind) -> (Format, &str) {
        let (f, e) = match kind {
            ImageKind::HardDisk => &self.hard_disk,
            ImageKind::Floppy => &self.floppy,
            ImageKind::Disc => &self.disc,
        };
        (*f, e.trim())
    }

    /// Why `source` will not be converted with these options, if it will not.
    pub fn refusal(&self, source: &Classified) -> Option<String> {
        let (format, _) = self.target(source.kind);
        let size = match (source.kind, self.floppy_size) {
            (ImageKind::Floppy, FloppySize::Refit(bytes)) => bytes,
            _ => source.size,
        };
        format.fits(source, size).err()
    }
}

/// A disk image decoded the way the exporter reads it (VHD, QCOW2, VMDK, CHD, floppy containers).
pub fn open_disk(path: &Path) -> anyhow::Result<(Box<dyn crate::rbformats::ReadSeek>, u64)> {
    crate::rbformats::export::open_source_reader(path)
}

/// An output extension is a bare word: no dot, slash or space.
pub fn extension_problem(ext: &str) -> Option<&'static str> {
    let ext = ext.trim();
    (ext.is_empty() || ext.contains(['/', '\\', '.', ' ']))
        .then_some("An output extension may not be empty or hold a dot, slash or space.")
}

/// Where `source` goes: under `output`, at its path below `root`, with the new extension.
pub fn output_path(source: &Path, root: &Path, output: &Path, extension: &str) -> PathBuf {
    let stem = source
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("disk");
    let rel_dir = source
        .parent()
        .and_then(|p| p.strip_prefix(root).ok())
        .map(Path::to_path_buf)
        .unwrap_or_default();
    output
        .join(rel_dir)
        .join(format!("{stem}.{}", extension.trim()))
}

/// The files a folder holds that may be images: dotfiles and a .bin beside its .cue are left out.
pub fn scan(folder: &Path, recursive: bool) -> std::io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    walk(folder, recursive, &mut out)?;
    out.sort();
    Ok(out)
}

fn walk(dir: &Path, recursive: bool, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for path in entries {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            if recursive {
                walk(path.as_path(), true, out)?;
            }
            continue;
        }
        if !path.is_file() || is_bin_of_a_cue(path.as_path()) || is_sidecar(path.as_path()) {
            continue;
        }
        out.push(path);
    }
    Ok(())
}

/// A `.bin` with a `.cue` of the same name beside it: the cue stands for both.
fn is_bin_of_a_cue(path: &Path) -> bool {
    let is_bin = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("bin"));
    is_bin
        && ["cue", "CUE", "Cue"]
            .iter()
            .any(|e| path.with_extension(e).is_file())
}

/// Files that describe an image rather than hold one: checksums, notes, a VMDK's flat extent.
fn is_sidecar(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    name.ends_with("-flat.vmdk")
        || matches!(
            ext.as_str(),
            "txt"
                | "md"
                | "nfo"
                | "json"
                | "sha256"
                | "crc32"
                | "md5"
                | "sfv"
                | "log"
                | "jpg"
                | "png"
                | "pdf"
                | "zip.part"
        )
}

#[cfg(test)]
mod tests;
