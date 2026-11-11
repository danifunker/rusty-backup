//! Build Disk: make a new disk, partitioned or a single bare volume, as an image file or onto a
//! physical disk (an SD / CF card, a USB stick, any disk the device list shows).
//!
//! [`BuildDiskController`] is a wizard. Its steps are controller state ([`Step`],
//! [`BuildDiskController::steps`], [`BuildDiskController::next_blocked`]), so the GUI tab and the
//! TUI screen walk the same steps and one scenario test drives both. The plan lives in
//! `model::disk_builder::DiskBuilder` (rows, layout, validation), the formatting in
//! `model::format`, and the build is one job over `model::provision_runner`. The CLI's `rb-cli
//! build` fills the same request without the steps. See docs/build_disk_plan.md § 7.

mod options;
#[cfg(test)]
mod tests;

pub use options::{OptionEdit, AFFS_VARIANTS, NTFS_VERSIONS, SECTOR_SIZES};

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::controller::log::LogLevel;
use crate::controller::partition_edit::{self, BuildAction, BuildView};
use crate::controller::{job_outcome, Choice, ChoiceSpec, Controller, Effect, PickSpec};
use crate::model::disk_builder::{DiskBuilder, RowFormat};
use crate::model::job::{Job, Notify, Snapshot};

pub use crate::device::DiskDevice;
pub use crate::model::format::{geometries, FormatOptions, FsCaps, FsKind, FsOption, NtfsVersion};
pub use crate::model::job::NoNotify;
pub use crate::partition::type_catalog::TableKind;
pub use crate::rbformats::export::ExportFormat;
pub use disk_core::display::format_size;

/// A label the user typed on `old`, kept when switching to `fs` if it fits there.
fn typed_label(old: &RowFormat, fs: FsKind) -> Option<String> {
    let label = old.opts.label.as_str();
    let max = fs.caps().label_max;
    let typed = label != RowFormat::new(old.kind).opts.label;
    (typed && max > 0 && label.len() <= max).then(|| label.to_string())
}

/// The containers an image output can be encoded into, raw first.
pub fn containers() -> Vec<ExportFormat> {
    let mut v = vec![
        ExportFormat::Raw,
        ExportFormat::Vhd,
        ExportFormat::VhdDynamic,
        ExportFormat::Qcow2,
        ExportFormat::VmdkSparse,
    ];
    if cfg!(feature = "chd") {
        v.push(ExportFormat::Chd);
    }
    v.extend([
        ExportFormat::TwoMg,
        ExportFormat::Dc42,
        ExportFormat::Woz,
        ExportFormat::Moof,
    ]);
    v
}

/// The container a file name's extension asks for, if it names one.
pub fn container_for_path(path: &Path) -> Option<ExportFormat> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    Some(match ext.as_str() {
        "img" | "raw" | "bin" | "hdf" | "dsk" | "hfv" | "iso" => ExportFormat::Raw,
        "vhd" => ExportFormat::Vhd,
        "qcow2" => ExportFormat::Qcow2,
        "vmdk" => ExportFormat::VmdkSparse,
        "chd" if cfg!(feature = "chd") => ExportFormat::Chd,
        "2mg" => ExportFormat::TwoMg,
        "dc42" | "image" => ExportFormat::Dc42,
        "woz" => ExportFormat::Woz,
        "moof" => ExportFormat::Moof,
        _ => return None,
    })
}

/// Why `fmt` cannot hold a disk of `size` bytes, for the containers fixed to floppy sizes.
fn container_refuses(fmt: ExportFormat, size: u64) -> Option<String> {
    let sizes: &[u64] = match fmt {
        ExportFormat::Woz => &[143_360, 409_600, 819_200],
        ExportFormat::Moof => &[409_600, 819_200],
        ExportFormat::Dc42 => &[409_600, 737_280, 819_200, 1_474_560],
        _ => return None,
    };
    (!sizes.contains(&size)).then(|| {
        let list: Vec<String> = sizes.iter().map(|s| format_size(*s)).collect();
        format!(
            "{} holds only floppies of {}, not {}",
            fmt.description(),
            list.join(" / "),
            format_size(size)
        )
    })
}

/// The CP/M disk formats a CP/M volume can take, by preset name.
pub fn cpm_presets() -> Vec<&'static str> {
    fs_cpm::cpm_diskdefs::ALL_PRESETS
        .iter()
        .map(|d| d.name)
        .collect()
}

/// A blank (auto) or sized field in the `4K` / `64M` grammar.
pub fn parse_size_field(text: &str) -> Result<Option<u64>, String> {
    let t = text.trim();
    if t.is_empty() {
        return Ok(None);
    }
    crate::partition::parse_size(t)
        .map(Some)
        .map_err(|e| format!("{e:#}"))
}

pub use crate::model::provision_runner::MacDriver;

/// The `purpose` of each picker this controller asks for.
pub mod purpose {
    pub const OUTPUT_IMAGE: &str = "build_disk.output_image";
    pub const ROW_SOURCE: &str = "build_disk.row_source";
    pub const RDB_HANDLER: &str = "build_disk.rdb_handler";
    pub const MAC_DRIVER: &str = "build_disk.mac_driver";
    pub const SYSTEM_DISK: &str = "build_disk.system_disk";
    pub const DONOR: &str = "build_disk.donor";
    pub const CD_FOLDER: &str = "build_disk.cd_folder";
    pub const NTFS_VERSION: &str = "build_disk.ntfs_version";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const STEP: &str = "build_disk.step";
    pub const KIND: &str = "build_disk.kind";
    pub const OUTPUT_KIND: &str = "build_disk.output_kind";
    pub const OUTPUT_DEVICE: &str = "build_disk.output_device";
    pub const OUTPUT_IMAGE: &str = "build_disk.output_image";
    pub const SIZE: &str = "build_disk.size";
    pub const TABLE: &str = "build_disk.table";
    pub const VOLUME_FS: &str = "build_disk.volume_fs";
    pub const ROW_CONTENT: &str = "build_disk.row_content";
    pub const LABEL: &str = "build_disk.label";
    pub const CONFIRM: &str = "build_disk.confirm";
    pub const BACK: &str = "build_disk.back";
    pub const NEXT: &str = "build_disk.next";
    pub const CREATE: &str = "build_disk.create";
    pub const CANCEL: &str = "build_disk.cancel";
    pub const BUILD_ANOTHER: &str = "build_disk.build_another";
    pub const OPEN_INSPECT: &str = "build_disk.open_inspect";
    pub const CONTAINER: &str = "build_disk.container";
    pub const PICK_IMAGE: &str = "build_disk.pick_image";
    pub const DONOR: &str = "build_disk.donor";
    pub const DONOR_BLANK: &str = "build_disk.donor_blank";
    pub const ALIGN: &str = "build_disk.align";
    pub const HEADS: &str = "build_disk.heads";
    pub const SECTORS: &str = "build_disk.sectors";
    pub const PRESET: &str = "build_disk.preset";
    pub const X68K_SCSI: &str = "build_disk.x68k_scsi";
    pub const X68K_PARTS: &str = "build_disk.x68k_parts";
    pub const SYSTEM_DISK: &str = "build_disk.system_disk";
    pub const SYSTEM_DISK_CLEAR: &str = "build_disk.system_disk_clear";
    pub const SGI_LABEL: &str = "build_disk.sgi_label";
    pub const ROW_SIZE: &str = "build_disk.row_size";
    pub const ROW_TYPE: &str = "build_disk.row_type";
    pub const ROW_PICK_TYPE: &str = "build_disk.row_pick_type";
    pub const ROW_NAME: &str = "build_disk.row_name";
    pub const ROW_BOOT: &str = "build_disk.row_boot";
    pub const ROW_IMAGE: &str = "build_disk.row_image";
    pub const ROW_CLEAR: &str = "build_disk.row_clear";
    pub const ROW_UP: &str = "build_disk.row_up";
    pub const ROW_DOWN: &str = "build_disk.row_down";
    pub const ROW_REMOVE: &str = "build_disk.row_remove";
    pub const ADD_ROW: &str = "build_disk.add_row";
    pub const ADD_HANDLER: &str = "build_disk.add_handler";
    pub const REMOVE_HANDLER: &str = "build_disk.remove_handler";
    pub const MAC_DRIVER: &str = "build_disk.mac_driver";
    pub const MAC_DRIVER_DISK: &str = "build_disk.mac_driver_disk";
    pub const CD_LAYOUT: &str = "build_disk.cd_layout";
    pub const CD_NAME: &str = "build_disk.cd_name";
    pub const CD_AUTO_SIZE: &str = "build_disk.cd_auto_size";
    pub const CD_FOLDER: &str = "build_disk.cd_folder";
    pub const CD_FOLDER_CLEAR: &str = "build_disk.cd_folder_clear";
    pub const CD_EXPAND_ARCHIVES: &str = "build_disk.cd_expand_archives";
    pub const CD_EXPAND_GUNZIP: &str = "build_disk.cd_expand_gunzip";
    pub const CD_EXPAND_IMAGES: &str = "build_disk.cd_expand_images";
    pub const CD_FLATTEN: &str = "build_disk.cd_flatten";
    pub const CD_NO_PERMISSIONS: &str = "build_disk.cd_no_permissions";
    pub const CD_FORCE: &str = "build_disk.cd_force";
    pub const CD_SIDECARS: &str = "build_disk.cd_sidecars";
    pub const OPTION: &str = "build_disk.option";
}

/// What every shell calls each control and outcome (D8); a shell adds only layout (a `:`, brackets).
pub mod labels {
    use super::ids;
    pub const KIND: &str = "Kind";
    pub const NEXT: &str = "Next";
    pub const CREATE: &str = "Create Disk";
    pub const BUILD_ANOTHER: &str = "Build Another";
    pub const PICK_IMAGE: &str = "Save As...";
    pub const CD_LAYOUT: &str = "Layout";
    pub const CD_AUTO_SIZE: &str = "Size to the folder";
    pub const CD_FOLDER: &str = "Choose Folder...";
    pub const DONE: &str = "Disk build complete.";
    /// Id to label, with `<task>.done` naming the outcome message, for drivers that find by label.
    pub const TABLE: &[(&str, &str)] = &[
        (ids::KIND, KIND),
        (ids::NEXT, NEXT),
        (ids::CREATE, CREATE),
        (ids::BUILD_ANOTHER, BUILD_ANOTHER),
        (ids::PICK_IMAGE, PICK_IMAGE),
        (ids::CD_LAYOUT, CD_LAYOUT),
        (ids::CD_AUTO_SIZE, CD_AUTO_SIZE),
        (ids::CD_FOLDER, CD_FOLDER),
        ("build_disk.done", DONE),
    ];
}

/// The text every view shows beside a control.
pub mod hints {
    pub const CONTAINER: &str = "The disk is built raw, then encoded into this container. The \
         file name's extension picks one when you save.";
    pub const DEVICE: &str = "An SD or CF card in a reader, a USB stick, or another disk";
    pub const DEVICE_WARNING: &str =
        "Building erases everything on the device. The disk is built at the device's own size.";
    pub const SIZE: &str = "Accepts K / M / G suffixes, e.g. 512M or 2G.";
    pub const CD_SIZE: &str = "A CD-R holds about 650-700 MiB; a larger image still works in an \
         emulator.";
    pub const CD_AUTO_SIZE: &str =
        "Size the disc to the folder plus filesystem overhead, for an image to mount rather than burn.";
    pub const CD_FOLDER: &str = "The folder's contents land at the disc's root; leave it blank \
         to fill the disc later in Inspect or Commander.";
    pub const CD_EXPAND_ARCHIVES: &str = "Archives in the folder (.tar, .tardist, .zip, and \
         classic Mac .sit, .cpt, .hqx) are opened and their contents put on the disc, in a folder \
         named after each archive. Leave it off to put the archive files on the disc unopened.";
    pub const CD_EXPAND_GUNZIP: &str = "Files compressed with gzip go on the disc uncompressed: \
         readme.gz lands as readme, and a .tgz or .tar.gz as a plain .tar. Handy for an old \
         system with no gunzip. With Unpack archives on too, a .tar.gz is unpacked all the way.";
    pub const CD_EXPAND_IMAGES: &str = "Disk images in the folder (.dsk, .img, .hfv, .dmg, \
         DiskCopy) are opened and the files inside them put on the disc, in a folder named after \
         each image, Mac resource forks included. Leave it off to put the image files themselves \
         on the disc.";
    pub const CD_FLATTEN: &str = "Instead of a folder per archive, every archive's contents go \
         straight into the top of the disc, merged together. This is the layout an IRIX \
         software CD needs, where inst is pointed at one folder holding every .tardist's \
         contents. A file found in more than one archive is kept once.";
    pub const CD_NO_PERMISSIONS: &str = "Normally each file keeps the owner and the read, write \
         and run permissions it has on this computer. Turn this on to give every file the \
         disc's standard owner and permissions instead, so the disc does not carry this \
         computer's user ids.";
    pub const CD_FORCE: &str = "When two things land at the same place on the disc (two \
         archives holding the same file, say), the later one replaces the earlier one. With \
         this off the build stops with an error instead, except when unpacking into the root, \
         where the first copy is kept.";
    pub const CD_SIDECARS: &str = "A modern Mac copying files to a disk that is not Mac \
         formatted stores each file's Mac data (its resource fork, type and creator) in a \
         hidden ._ file beside it.";
    pub const CD_SIDECARS_FORK: &str = "Each ._ file is put back into the file it belongs to, \
         as its resource fork, type and creator, the way a classic Mac kept it. The ._ files \
         themselves do not appear on the disc.";
    pub const CD_SIDECARS_KEEP: &str = "The ._ files go on the disc as ordinary files, just as \
         they are in the folder. On a Mac disc the file they belong to still gets its resource \
         fork as well.";
    pub const CD_SIDECARS_EXCLUDE: &str = "The ._ files are left off the disc and nothing is \
         taken from them. Pick this when they are only there because the folder was on a \
         modern Mac.";
    pub const FIXED_SIZE: &str = "This filesystem has a fixed size; no size to choose.";
    pub const DONOR: &str = "Take the table, its special partitions and their contents from \
         another disk image, at this disk's size. A label (SGI, Sun, NeXT, RDB, ...) keeps its \
         volume header or boot blocks verbatim.";
    pub const ALIGN: &str = "Partition starts are rounded up to this. Blank uses the table \
         default. Accepts 1M / 63s (sectors).";
    pub const SYSTEM_DISK: &str = "A Human68k system floppy cloned into partition 1";
    pub const NO_SYSTEM_DISK: &str = "(none: seed files)";
    pub const ROW_SIZE: &str = "20M / 1G / bytes, or `rest` for whatever is left";
    pub const ROW_FIXED_TYPE: &str = "From the donor's label; the label keeps it";
    pub const ROW_PICK_TYPE: &str = "Choose a well-known partition type";
    pub const ROW_COPY: &str = "The donor partition's bytes, verbatim";
    pub const ROW_IMAGE: &str = "Pour an image into this partition after the table is written";
    pub const BOOT_MBR: &str = "The MBR active flag: the BIOS boots this partition";
    pub const BOOT_RDB: &str = "The RDB bootable flag: Kickstart may boot this partition";
    pub const HANDLER: &str = "The AmigaDOS load file, e.g. L:PFS3 or L:SmartFilesystem; the \
         strap loads it from the RDB";
    pub const MAC_DRIVER: &str = "A Mac mounts an APM disk only through a driver on the disk. \
         Bundled uses a donor's own driver when it has one";
    pub const FAT32: &str = "An EFI System Partition is FAT32";
}

/// The wizard's steps, in the order they are walked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Partitioned disk or a single volume.
    Kind,
    /// Image file or physical disk; a disk fixes the size.
    Output,
    /// Partitioned: table, size, alignment. Volume: filesystem and size.
    Disk,
    /// Rows: size, type, name, content.
    Partitions,
    /// Label and filesystem options for every formatted row or the volume.
    Options,
    /// Everything chosen, the warnings, and for a device its typed path.
    Review,
    /// The job's progress and how it ended.
    Build,
}

impl Step {
    pub fn label(self) -> &'static str {
        match self {
            Step::Kind => "Kind",
            Step::Output => "Output",
            Step::Disk => "Disk",
            Step::Partitions => "Partitions",
            Step::Options => "Options",
            Step::Review => "Review",
            Step::Build => "Build",
        }
    }
}

/// What is being built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskKind {
    /// A partition table with rows.
    Partitioned,
    /// One filesystem filling the whole disk, no table (a superfloppy or a floppy).
    Volume,
    /// A fixed-shape bootable disk (X68000, SGI IRIX).
    Preset,
    /// A CD-ROM image in one of the native layouts `model::cd_build` masters.
    Cd,
}

impl DiskKind {
    pub const ALL: [DiskKind; 4] = [
        DiskKind::Partitioned,
        DiskKind::Volume,
        DiskKind::Preset,
        DiskKind::Cd,
    ];

    pub fn label(self) -> &'static str {
        match self {
            DiskKind::Partitioned => "Partitioned disk",
            DiskKind::Volume => "Single volume (no partition table)",
            DiskKind::Preset => "Bootable preset (X68000, SGI IRIX)",
            DiskKind::Cd => "CD-ROM image (IRIX EFS, Mac HFS / HFS+, NeXTSTEP UFS)",
        }
    }

    pub fn hint(self) -> &'static str {
        match self {
            DiskKind::Partitioned => {
                "A partition table (MBR, GPT, APM, RDB, SGI, Sun, NeXT, ...) with partitions you \
                 size, each formatted, filled from an image, or left empty."
            }
            DiskKind::Volume => {
                "One filesystem across the whole disk with no partition table: a floppy, a \
                 superfloppy, or a bare volume for an emulator."
            }
            DiskKind::Preset => {
                "A ready-to-boot disk in a fixed shape: a Sharp X68000 Human68k disk, or an SGI \
                 IRIX disk with its volume header and an EFS root."
            }
            DiskKind::Cd => {
                "A CD-ROM image (.iso) in a platform's own layout, blank or filled from a folder. \
                 There is no ISO 9660 side: each disc is read by the system it is made for."
            }
        }
    }
}

/// The presets, as the Disk step lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresetKind {
    X68k,
    SgiEfs,
}

impl PresetKind {
    pub const ALL: [PresetKind; 2] = [PresetKind::X68k, PresetKind::SgiEfs];

    pub fn label(self) -> &'static str {
        match self {
            PresetKind::X68k => "Sharp X68000 (IPL, X68000 table, Human68k)",
            PresetKind::SgiEfs => "SGI IRIX (volume header, EFS root)",
        }
    }
}

/// A preset's fields, for the Disk step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresetView {
    pub kind: PresetKind,
    /// X68000: SCSI (1024-byte sectors) rather than SASI (256-byte).
    pub x68k_scsi: bool,
    /// X68000: Human68k partitions, 1 to 8.
    pub x68k_partitions: usize,
    /// X68000: the Human68k system floppy cloned into partition 1.
    pub x68k_system_disk: Option<PathBuf>,
    /// SGI: the EFS volume label (6 bytes).
    pub sgi_label: String,
}

pub use crate::model::cd_build::{CdImport, CdLayout};

/// A CD-ROM's fields, for the Disk step; its size is the shared size text, or `auto`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdView {
    pub layout: CdLayout,
    pub name: String,
    /// The folder the disc is filled from; `None` leaves it blank.
    pub from_dir: Option<PathBuf>,
    pub import: CdImport,
}

/// One way a folder is copied onto a CD, as every shell lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdOption {
    ExpandArchives,
    FlattenFolders,
    ExpandGunzip,
    ExpandImages,
    Force,
    NoPermissions,
}

impl CdOption {
    pub const ALL: [CdOption; 6] = [
        CdOption::ExpandArchives,
        CdOption::FlattenFolders,
        CdOption::ExpandGunzip,
        CdOption::ExpandImages,
        CdOption::Force,
        CdOption::NoPermissions,
    ];

    pub fn label(self) -> &'static str {
        match self {
            CdOption::ExpandArchives => "Unpack archives",
            CdOption::FlattenFolders => "Unpack every archive into the root",
            CdOption::ExpandGunzip => "Decompress .gz files",
            CdOption::ExpandImages => "Copy files out of disk images",
            CdOption::Force => "Replace files with the same name",
            CdOption::NoPermissions => "Ignore this computer's permissions",
        }
    }

    pub fn hint(self) -> &'static str {
        match self {
            CdOption::ExpandArchives => hints::CD_EXPAND_ARCHIVES,
            CdOption::FlattenFolders => hints::CD_FLATTEN,
            CdOption::ExpandGunzip => hints::CD_EXPAND_GUNZIP,
            CdOption::ExpandImages => hints::CD_EXPAND_IMAGES,
            CdOption::Force => hints::CD_FORCE,
            CdOption::NoPermissions => hints::CD_NO_PERMISSIONS,
        }
    }

    pub fn id(self) -> &'static str {
        match self {
            CdOption::ExpandArchives => ids::CD_EXPAND_ARCHIVES,
            CdOption::FlattenFolders => ids::CD_FLATTEN,
            CdOption::ExpandGunzip => ids::CD_EXPAND_GUNZIP,
            CdOption::ExpandImages => ids::CD_EXPAND_IMAGES,
            CdOption::Force => ids::CD_FORCE,
            CdOption::NoPermissions => ids::CD_NO_PERMISSIONS,
        }
    }

    fn flag(self, i: &mut CdImport) -> &mut bool {
        match self {
            CdOption::ExpandArchives => &mut i.expand_archives,
            CdOption::FlattenFolders => &mut i.flatten_folders,
            CdOption::ExpandGunzip => &mut i.expand_gunzip,
            CdOption::ExpandImages => &mut i.expand_images,
            CdOption::Force => &mut i.force,
            CdOption::NoPermissions => &mut i.no_permissions,
        }
    }
}

/// What becomes of the macOS `._` files in the folder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdSidecars {
    /// Taken into their file as its resource fork (Mac discs only).
    ResourceFork,
    Keep,
    Exclude,
}

impl CdSidecars {
    pub fn label(self) -> &'static str {
        match self {
            CdSidecars::ResourceFork => "Use as resource forks",
            CdSidecars::Keep => "Leave as-is",
            CdSidecars::Exclude => "Exclude",
        }
    }

    pub fn hint(self) -> &'static str {
        match self {
            CdSidecars::ResourceFork => hints::CD_SIDECARS_FORK,
            CdSidecars::Keep => hints::CD_SIDECARS_KEEP,
            CdSidecars::Exclude => hints::CD_SIDECARS_EXCLUDE,
        }
    }

    /// The choices `layout` offers, its default first.
    pub fn choices(layout: CdLayout) -> &'static [CdSidecars] {
        if layout.keeps_forks() {
            &[
                CdSidecars::ResourceFork,
                CdSidecars::Keep,
                CdSidecars::Exclude,
            ]
        } else {
            &[CdSidecars::Exclude, CdSidecars::Keep]
        }
    }

    fn of(layout: CdLayout, i: &CdImport) -> CdSidecars {
        match (i.include_appledouble, i.ignore_appledouble) {
            (true, _) => CdSidecars::Keep,
            (_, true) => CdSidecars::Exclude,
            // Without a fork to take them into, a disc leaves them out.
            _ if layout.keeps_forks() => CdSidecars::ResourceFork,
            _ => CdSidecars::Exclude,
        }
    }

    fn apply(self, i: &mut CdImport) {
        i.include_appledouble = self == CdSidecars::Keep;
        i.ignore_appledouble = self == CdSidecars::Exclude;
    }
}

/// A CD option as the Disk step shows it: always listed, greyed out with the reason when it
/// does not apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdOptionRow {
    pub option: CdOption,
    pub on: bool,
    /// Why it does not apply now, or `None` when it can be changed.
    pub unavailable: Option<&'static str>,
}

/// The size text that sizes a CD to its folder.
pub const CD_AUTO_SIZE: &str = "auto";

/// What a row holds after the build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowContent {
    Empty,
    /// The donor partition's bytes, verbatim.
    Copy,
    /// Formatted blank with a filesystem.
    Format,
    /// A source image poured in.
    Image,
}

/// User intents.
#[derive(Debug, Clone)]
pub enum BuildDiskAction {
    Next,
    Back,
    /// Jump back to a step already passed.
    GoTo(Step),
    SetKind(DiskKind),
    SetOutputIsDevice(bool),
    /// The container an image output is encoded into.
    SetContainer(ExportFormat),
    SelectDevice {
        row: usize,
        device: DiskDevice,
    },
    ClearDevice,
    /// "Save As..." for the image file.
    PickOutputImage,
    /// The image-file size (or a volume's size), in the `20M` / `1G` grammar.
    SetSize(String),
    /// An edit of the table and its rows (table type, alignment, geometry, row size / type /
    /// name / source, add, remove, move).
    Table(BuildAction),
    /// A row formatted with `fs`, or `None` for empty.
    SetRowFs {
        row: usize,
        fs: Option<FsKind>,
    },
    SetRowOptions {
        row: usize,
        opts: FormatOptions,
    },
    SetVolumeFs(FsKind),
    SetVolumeOptions(FormatOptions),
    /// The preset and its fields, edited as one.
    SetPreset(PresetView),
    /// "Choose..." for the X68000 preset's system floppy.
    PickSystemDisk,
    /// The CD-ROM and its fields, edited as one.
    SetCd(CdView),
    /// "Choose..." for the folder a CD is filled from.
    PickCdFolder,
    /// One of the folder-copy switches; ignored while it does not apply.
    SetCdOption(CdOption, bool),
    /// Ignored when the layout does not offer it.
    SetCdSidecars(CdSidecars),
    /// "Start from an existing disk...": pick a donor image to lay the table out from.
    PickDonor,
    /// Start from a blank table again.
    ClearDonor,
    /// Copy the donor partition the row was laid out from.
    SetRowCopy(usize),
    SetConfirmText(String),
    /// "Add handler...": pick the AmigaDOS load file for this DosType (RDB only).
    AddHandler(String),
    RemoveHandler(usize),
    SetMacDriver(MacDriver),
    /// "From a disk...": pick the Apple-formatted disk whose driver to use (APM only).
    PickMacDriver,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    /// The answer to a `Choose` (which NTFS version a new NTFS target gets).
    Chose {
        purpose: &'static str,
        choice: usize,
    },
    Create,
    Cancel,
    /// Back to the start, keeping the choices but the output file, which the last build wrote.
    BuildAnother,
}

/// One formatted target (a row or the volume), for the Options step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatTarget {
    /// The row index, or `None` for the volume.
    pub row: Option<usize>,
    pub title: String,
    pub fs: FsKind,
    pub caps: FsCaps,
    pub opts: FormatOptions,
    /// Why its label or options are refused.
    pub error: Option<String>,
}

pub struct BuildDiskController {
    notify: Arc<dyn Notify>,
    log: Arc<Mutex<Vec<(LogLevel, String)>>>,
    step: Step,
    kind: DiskKind,
    builder: DiskBuilder,
    volume: RowFormat,
    preset: PresetView,
    cd: CdView,
    size_text: String,
    output_is_device: bool,
    device: Option<(usize, DiskDevice)>,
    image: Option<PathBuf>,
    container: ExportFormat,
    confirm_text: String,
    /// The target an open "which NTFS version" question is about: a row, or `None` for the volume.
    ntfs_question: Option<Option<usize>>,
    /// RDB filesystem handlers to embed, and the DosType a handler pick is for.
    handlers: Vec<(String, PathBuf)>,
    handler_for: Option<String>,
    mac_driver: MacDriver,
    source_row: Option<usize>,
    probe: Option<(usize, Job<(PathBuf, u64)>)>,
    donor_job: Option<Job<Result<crate::model::disk_builder::Donor, String>>>,
    donor_error: Option<String>,
    job: Option<Job<Result<(), String>>>,
    outcome: Option<Result<(), String>>,
    finished: Option<Snapshot>,
}

impl BuildDiskController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        let mut c = BuildDiskController {
            notify,
            log: Arc::new(Mutex::new(Vec::new())),
            step: Step::Kind,
            kind: DiskKind::Partitioned,
            builder: DiskBuilder::new(TableKind::Mbr, 0),
            volume: RowFormat::new(FsKind::Fat),
            preset: PresetView {
                kind: PresetKind::X68k,
                x68k_scsi: false,
                x68k_partitions: 1,
                x68k_system_disk: None,
                sgi_label: "irix".to_string(),
            },
            cd: CdView {
                layout: CdLayout::MacHfs,
                name: CdLayout::MacHfs.default_name().to_string(),
                from_dir: None,
                import: CdImport::default(),
            },
            size_text: "512M".to_string(),
            output_is_device: false,
            device: None,
            image: None,
            container: ExportFormat::Raw,
            confirm_text: String::new(),
            ntfs_question: None,
            handlers: Vec::new(),
            handler_for: None,
            mac_driver: MacDriver::Bundled,
            source_row: None,
            probe: None,
            donor_job: None,
            donor_error: None,
            job: None,
            outcome: None,
            finished: None,
        };
        c.sync_size();
        c
    }

    /// Lines for the log panel since the last call.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        self.log
            .lock()
            .map(|mut l| std::mem::take(&mut *l))
            .unwrap_or_default()
    }

    fn push(&self, level: LogLevel, msg: impl Into<String>) {
        if let Ok(mut l) = self.log.lock() {
            l.push((level, msg.into()));
        }
    }

    // --- reading -------------------------------------------------------------------------

    pub fn step(&self) -> Step {
        self.step
    }

    /// The steps for the current kind, in order.
    pub fn steps(&self) -> Vec<Step> {
        let mut v = vec![Step::Kind, Step::Output, Step::Disk];
        match self.kind {
            DiskKind::Partitioned => {
                v.push(Step::Partitions);
                if self.builder.rows.iter().any(|r| r.format.is_some()) {
                    v.push(Step::Options);
                }
            }
            DiskKind::Volume => v.push(Step::Options),
            DiskKind::Preset | DiskKind::Cd => {}
        }
        v.push(Step::Review);
        v.push(Step::Build);
        v
    }

    pub fn kind(&self) -> DiskKind {
        self.kind
    }

    pub fn output_is_device(&self) -> bool {
        self.output_is_device
    }

    pub fn device(&self) -> Option<&DiskDevice> {
        self.device.as_ref().map(|(_, d)| d)
    }

    pub fn device_row(&self) -> Option<usize> {
        self.device.as_ref().map(|(r, _)| *r)
    }

    /// "Refresh Devices" replaced the list: the target follows its own disk, and never another one.
    pub fn reconcile_devices(&mut self, devices: &[DiskDevice]) {
        if self.is_running() {
            return;
        }
        let Some((row, device)) = &self.device else {
            return;
        };
        let identity = device.identity();
        if devices.get(*row).map(DiskDevice::identity) == Some(identity.clone()) {
            return;
        }
        match devices.iter().position(|d| d.identity() == identity) {
            Some(moved) => self.device = Some((moved, devices[moved].clone())),
            None => {
                let gone = device.display_name();
                self.device = None;
                self.confirm_text.clear();
                self.push(
                    LogLevel::Warning,
                    format!("{gone} is no longer listed, so it is no longer the build target."),
                );
            }
        }
        self.sync_size();
    }

    pub fn image(&self) -> Option<&Path> {
        self.image.as_deref()
    }

    /// The container an image output is encoded into (a device is always raw).
    pub fn container(&self) -> ExportFormat {
        self.container
    }

    pub fn size_text(&self) -> &str {
        self.size_text.as_str()
    }

    /// The size of the disk or volume being built, or why it is unknown.
    pub fn disk_size(&self) -> Result<u64, String> {
        if self.output_is_device {
            return match &self.device {
                Some((_, d)) => Ok(d.size_bytes),
                None => Err("Choose a device first.".to_string()),
            };
        }
        if self.kind == DiskKind::Volume && self.volume.kind.caps().fixed_size {
            return self.fixed_volume_size();
        }
        if self.kind == DiskKind::Cd && self.cd_auto_size() {
            return Err("Sized to the folder when it is built.".to_string());
        }
        match crate::partition::parse_size(self.size_text.as_str()) {
            Ok(0) => Err("The size must be more than zero.".to_string()),
            Ok(n) => Ok(n),
            Err(e) => Err(format!("{e:#}")),
        }
    }

    /// A fixed-geometry volume's own size (CP/M's comes from its preset).
    fn fixed_volume_size(&self) -> Result<u64, String> {
        let caps = self.volume.kind.caps();
        if caps.min_size > 0 {
            return Ok(caps.min_size);
        }
        let mut cur = std::io::Cursor::new(Vec::new());
        crate::model::format::format_into(&mut cur, 0, self.volume.kind, &self.volume.opts)
            .map(|d| d.len)
            .map_err(|e| format!("{e:#}"))
    }

    /// The sizes a volume's filesystem takes, for the Disk step; a fixed size has none.
    pub fn volume_size_range(&self) -> Option<String> {
        let caps = self.volume.kind.caps();
        (!caps.fixed_size).then(|| {
            format!(
                "{} to {}",
                format_size(caps.min_size),
                format_size(caps.max_buildable())
            )
        })
    }

    /// The table and rows, as the partition editor draws them.
    pub fn table_view(&self) -> BuildView {
        partition_edit::build_view(&self.builder)
    }

    /// Each row's content choice.
    pub fn row_content(&self, row: usize) -> RowContent {
        match self.builder.rows.get(row) {
            Some(r) if r.source.is_some() => RowContent::Image,
            Some(r) if r.format.is_some() => RowContent::Format,
            Some(r) if r.copy_from.is_some() => RowContent::Copy,
            _ => RowContent::Empty,
        }
    }

    /// The filesystem a row is formatted with.
    pub fn row_fs(&self, row: usize) -> Option<FsKind> {
        self.builder
            .rows
            .get(row)
            .and_then(|r| r.format.as_ref())
            .map(|f| f.kind)
    }

    pub fn volume_fs(&self) -> FsKind {
        self.volume.kind
    }

    pub fn preset(&self) -> &PresetView {
        &self.preset
    }

    pub fn cd(&self) -> &CdView {
        &self.cd
    }

    /// Every way a folder is copied onto the CD, with why one does not apply now.
    pub fn cd_options(&self) -> Vec<CdOptionRow> {
        let cd = &self.cd;
        CdOption::ALL
            .iter()
            .map(|&option| {
                let mut i = cd.import.clone();
                let on = *option.flag(&mut i);
                let unavailable = if cd.from_dir.is_none() {
                    Some("Choose a folder to fill the disc from first.")
                } else {
                    match option {
                        CdOption::FlattenFolders if !cd.layout.can_flatten() => {
                            Some("Only the IRIX EFS and NeXTSTEP UFS discs unpack into one root.")
                        }
                        CdOption::FlattenFolders if !cd.import.expand_archives => {
                            Some("Turn on Unpack archives first.")
                        }
                        CdOption::NoPermissions if !cd.layout.keeps_permissions() => {
                            Some("HFS keeps no Unix permissions.")
                        }
                        _ => None,
                    }
                };
                CdOptionRow {
                    option,
                    on,
                    unavailable,
                }
            })
            .collect()
    }

    /// What becomes of the folder's macOS `._` files, as the layout reads the settings.
    pub fn cd_sidecars(&self) -> CdSidecars {
        CdSidecars::of(self.cd.layout, &self.cd.import)
    }

    /// Why the `._` choice cannot be changed now.
    pub fn cd_sidecars_unavailable(&self) -> Option<&'static str> {
        self.cd
            .from_dir
            .is_none()
            .then_some("Choose a folder to fill the disc from first.")
    }

    /// The CD is sized to its folder rather than to the size text.
    pub fn cd_auto_size(&self) -> bool {
        self.size_text.trim().eq_ignore_ascii_case(CD_AUTO_SIZE)
    }

    /// The donor the layout started from: its path and table name.
    pub fn donor(&self) -> Option<(&Path, &str)> {
        self.builder
            .donor
            .as_ref()
            .map(|d| (d.path.as_path(), d.table_type.as_str()))
    }

    /// The donor is being read, or why it was refused.
    pub fn donor_reading(&self) -> bool {
        self.donor_job.is_some()
    }

    pub fn donor_error(&self) -> Option<&str> {
        self.donor_error.as_deref()
    }

    /// A row was laid out from a donor partition, so Copy is one of its contents.
    pub fn row_can_copy(&self, row: usize) -> bool {
        self.builder
            .rows
            .get(row)
            .is_some_and(|r| r.donor_part.is_some())
    }

    /// The filesystems a volume or row can be formatted with, in a stable order.
    pub fn filesystems(&self) -> &'static [FsKind] {
        &FsKind::ALL
    }

    /// The source image of a row is being measured.
    pub fn probing(&self) -> bool {
        self.probe.is_some()
    }

    /// Every formatted row (or the volume), with its options, for the Options step.
    pub fn format_targets(&self) -> Vec<FormatTarget> {
        let target = |row: Option<usize>, title: String, f: &RowFormat| FormatTarget {
            row,
            title,
            fs: f.kind,
            caps: f.kind.caps(),
            opts: f.opts.clone(),
            error: f.check().err(),
        };
        match self.kind {
            DiskKind::Volume => vec![target(
                None,
                format!("Volume ({})", self.volume.kind.display_name()),
                &self.volume,
            )],
            DiskKind::Preset | DiskKind::Cd => Vec::new(),
            DiskKind::Partitioned => self
                .builder
                .rows
                .iter()
                .enumerate()
                .filter_map(|(i, r)| {
                    r.format.as_ref().map(|f| {
                        target(
                            Some(i),
                            format!("Partition {} ({})", i + 1, f.kind.display_name()),
                            f,
                        )
                    })
                })
                .collect(),
        }
    }

    /// The RDB filesystem handlers to embed: `(DosType, load file)`.
    pub fn handlers(&self) -> &[(String, PathBuf)] {
        self.handlers.as_slice()
    }

    /// Whether the table takes filesystem handlers (RDB).
    pub fn takes_handlers(&self) -> bool {
        self.kind == DiskKind::Partitioned
            && self.builder.kind == TableKind::Rdb
            && !self.builder.fixed_slots()
    }

    /// Whether the table takes a Mac driver (APM).
    pub fn takes_mac_driver(&self) -> bool {
        self.kind == DiskKind::Partitioned && self.builder.kind == TableKind::Apm
    }

    /// The driver an APM disk gets.
    pub fn mac_driver(&self) -> &MacDriver {
        &self.mac_driver
    }

    /// The DosTypes the rows use that no handler covers yet, ROM ones (`DOS\x`) left out.
    pub fn handlers_missing(&self) -> Vec<String> {
        if !self.takes_handlers() {
            return Vec::new();
        }
        let mut v: Vec<String> = Vec::new();
        for r in &self.builder.rows {
            let t = r.type_text.trim().to_string();
            let rom = t.starts_with("DOS");
            let covered = self
                .handlers
                .iter()
                .any(|(d, _)| d.eq_ignore_ascii_case(&t));
            if !rom && !covered && !t.is_empty() && !v.contains(&t) {
                v.push(t);
            }
        }
        v
    }

    pub fn confirm_text(&self) -> &str {
        self.confirm_text.as_str()
    }

    /// What a device build asks the user to type.
    pub fn confirm_expected(&self) -> Option<String> {
        if !self.output_is_device {
            return None;
        }
        self.device().map(|d| d.path.display().to_string())
    }

    pub fn is_running(&self) -> bool {
        self.job.is_some()
    }

    /// The build in progress, or the last one once it ended.
    pub fn progress(&self) -> Option<Snapshot> {
        self.job
            .as_ref()
            .map(|j| j.snapshot())
            .or_else(|| self.finished.clone())
    }

    pub fn outcome(&self) -> Option<&Result<(), String>> {
        self.outcome.as_ref()
    }

    /// The image file the last successful build wrote, for "Open in Inspect".
    pub fn built_image(&self) -> Option<&Path> {
        match (&self.outcome, self.output_is_device) {
            (Some(Ok(())), false) => self.image.as_deref(),
            _ => None,
        }
    }

    /// Why Next (or Create on Review) is refused on `step`, or `None`.
    pub fn next_blocked(&self) -> Option<String> {
        self.blocked_at(self.step)
    }

    fn blocked_at(&self, step: Step) -> Option<String> {
        match step {
            Step::Kind => None,
            Step::Output => self.output_blocked(),
            Step::Disk => self.disk_blocked(),
            Step::Partitions => self.partitions_blocked(),
            Step::Options => self
                .format_targets()
                .into_iter()
                .find_map(|t| t.error.map(|e| format!("{}: {e}", t.title))),
            Step::Review => self.review_blocked(),
            Step::Build => Some("The build is running.".to_string()),
        }
    }

    fn output_blocked(&self) -> Option<String> {
        if self.kind == DiskKind::Cd && self.output_is_device {
            return Some("A CD-ROM image is saved as a file; choose Image file.".to_string());
        }
        if !self.output_is_device {
            return match self.image {
                Some(_) => None,
                None => Some("Choose where to save the image file.".to_string()),
            };
        }
        match &self.device {
            None => Some("Choose a device.".to_string()),
            Some((_, d)) if d.is_system => {
                Some("That is the system disk; choose another device.".to_string())
            }
            Some((_, d)) if d.is_read_only => Some("That device is read-only.".to_string()),
            Some((_, d)) if d.size_bytes == 0 => {
                Some("That device reports no size; refresh the device list.".to_string())
            }
            Some(_) => None,
        }
    }

    fn disk_blocked(&self) -> Option<String> {
        if self.kind == DiskKind::Cd {
            return self.cd_blocked();
        }
        let size = match self.disk_size() {
            Ok(s) => s,
            Err(e) => return Some(e),
        };
        match self.kind {
            DiskKind::Partitioned => None,
            DiskKind::Volume => self.volume.fits(size).err(),
            DiskKind::Preset => self.preset_blocked(size),
            DiskKind::Cd => self.cd_blocked(),
        }
    }

    /// What the CD's fields lack: a size or a folder to measure, a folder that exists.
    fn cd_blocked(&self) -> Option<String> {
        if let Some(dir) = &self.cd.from_dir {
            if !dir.is_dir() {
                return Some(format!("{} is not a folder.", dir.display()));
            }
        }
        if self.cd_auto_size() {
            return self
                .cd
                .from_dir
                .is_none()
                .then(|| "An automatic size needs a folder to fill the disc from.".to_string());
        }
        let size = match self.disk_size() {
            Ok(s) => s,
            Err(e) => return Some(e),
        };
        if self.cd.layout == CdLayout::NextUfs {
            let opts = crate::partition::next_cd_builder::NextCdOptions::new(size, "");
            return crate::partition::next_cd_builder::plan_next_ufs_cd(&opts)
                .err()
                .map(|e| format!("{e:#}"));
        }
        None
    }

    /// The size the preset is built at: a device's size rounded down to what the preset takes.
    fn preset_size(&self, size: u64) -> u64 {
        match self.preset.kind {
            PresetKind::X68k => size / (1024 * 1024) * (1024 * 1024),
            PresetKind::SgiEfs => size,
        }
    }

    fn preset_blocked(&self, size: u64) -> Option<String> {
        let p = &self.preset;
        match p.kind {
            PresetKind::X68k => {
                if !(1..=8).contains(&p.x68k_partitions) {
                    return Some("An X68000 disk has 1 to 8 partitions.".to_string());
                }
                if !self.output_is_device && size % (1024 * 1024) != 0 {
                    return Some("An X68000 disk is a whole number of MiB.".to_string());
                }
                (self.preset_size(size) < 4 * 1024 * 1024)
                    .then(|| "An X68000 disk needs at least 4 MiB.".to_string())
            }
            PresetKind::SgiEfs => {
                let opts = crate::partition::sgi_hdd_builder::SgiHddOptions::new(
                    size,
                    p.sgi_label.as_str(),
                );
                crate::partition::sgi_hdd_builder::compute_sgi_layout(&opts)
                    .err()
                    .map(|e| format!("{e:#}"))
            }
        }
    }

    fn partitions_blocked(&self) -> Option<String> {
        let mut b = self.builder.clone();
        b.validate();
        b.errors.into_iter().find(|e| !e.starts_with("Warning:"))
    }

    fn review_blocked(&self) -> Option<String> {
        if self.job.is_some() {
            return Some("A build is running.".to_string());
        }
        for step in self.steps() {
            if matches!(step, Step::Review | Step::Build) {
                break;
            }
            if let Some(e) = self.blocked_at(step) {
                return Some(format!("{}: {e}", step.label()));
            }
        }
        if !self.output_is_device && self.kind != DiskKind::Cd {
            if let Some(why) = self
                .disk_size()
                .ok()
                .and_then(|size| container_refuses(self.container, size))
            {
                return Some(why);
            }
        }
        match self.confirm_expected() {
            Some(want) if self.confirm_text != want => {
                Some(format!("Type the device path `{want}` to confirm."))
            }
            _ => None,
        }
    }

    /// The Review step's summary lines.
    pub fn review_lines(&self) -> Vec<String> {
        let mut v = Vec::new();
        let size = match self.disk_size() {
            Ok(s) => format_size(s),
            Err(_) if self.kind == DiskKind::Cd => "sized to the folder".to_string(),
            Err(_) => String::new(),
        };
        v.push(match (&self.device, self.output_is_device, &self.image) {
            (Some((_, d)), true, _) => {
                format!("Write to {} ({size}), erasing it", d.display_name())
            }
            (_, false, Some(p))
                if self.container == ExportFormat::Raw || self.kind == DiskKind::Cd =>
            {
                format!("Save as {} ({size})", p.display())
            }
            (_, false, Some(p)) => format!(
                "Save as {} ({size}), encoded as {}",
                p.display(),
                self.container.description()
            ),
            _ => "No output chosen".to_string(),
        });
        match self.kind {
            DiskKind::Volume => v.push(format!("One {} volume", self.volume.describe())),
            DiskKind::Preset => {
                let p = &self.preset;
                v.push(p.kind.label().to_string());
                match p.kind {
                    PresetKind::X68k => {
                        v.push(format!(
                            "{} controller, {} Human68k partition(s)",
                            if p.x68k_scsi { "SCSI" } else { "SASI" },
                            p.x68k_partitions
                        ));
                        if let Some(d) = &p.x68k_system_disk {
                            v.push(format!("System disk cloned in: {}", d.display()));
                        }
                    }
                    PresetKind::SgiEfs => v.push(format!("EFS label {:?}", p.sgi_label)),
                }
            }
            DiskKind::Cd => {
                let c = &self.cd;
                v.push(format!("{}, volume name {:?}", c.layout.label(), c.name));
                v.push(match &c.from_dir {
                    Some(d) => format!("Filled from {}", d.display()),
                    None => "Left blank".to_string(),
                });
                let i = &c.import;
                let mut how = Vec::new();
                if i.expand_archives {
                    how.push(if i.flatten_folders && c.layout.can_flatten() {
                        "archives unpacked into the root"
                    } else {
                        "archives unpacked"
                    });
                }
                if i.expand_gunzip {
                    how.push(".gz files decompressed");
                }
                if i.expand_images {
                    how.push("disk images opened up");
                }
                if i.no_permissions && c.layout.keeps_permissions() {
                    how.push("host permissions ignored");
                }
                if i.force {
                    how.push("same-name entries overwritten");
                }
                if i.ignore_appledouble {
                    how.push("macOS ._ sidecars left out");
                } else if i.include_appledouble {
                    how.push("macOS ._ sidecars copied in as files");
                }
                if c.from_dir.is_some() && !how.is_empty() {
                    v.push(format!("While copying: {}", how.join(", ")));
                }
            }
            DiskKind::Partitioned => {
                match self.donor() {
                    Some((p, t)) if self.builder.fixed_slots() => v.push(format!(
                        "{t} label from {}, resized for this disk",
                        p.display()
                    )),
                    Some((p, _)) => v.push(format!(
                        "{} partition table, laid out from {}",
                        self.builder.kind.label(),
                        p.display()
                    )),
                    None => v.push(format!("{} partition table", self.builder.kind.label())),
                }
                for (t, p) in &self.handlers {
                    v.push(format!("Embedded {t} handler: {}", p.display()));
                }
                if self.takes_mac_driver() {
                    v.push(format!("Mac driver: {}", self.mac_driver.label()));
                }
                if let Ok(placed) = self.builder.plan() {
                    for (i, (row, p)) in self.builder.rows.iter().zip(placed.iter()).enumerate() {
                        let content = match (&row.source, &row.format, row.copy_from) {
                            (Some(path), _, _) => format!("from {}", path.display()),
                            (None, Some(f), _) => f.describe(),
                            (None, None, Some(_)) => {
                                "copied from the donor, grown to fit if it can".to_string()
                            }
                            (None, None, None) => "empty".to_string(),
                        };
                        v.push(format!(
                            "Partition {}: {} at LBA {}, type {}, {content}",
                            i + 1,
                            format_size(p.size_bytes),
                            p.start_lba,
                            p.type_text,
                        ));
                    }
                }
            }
        }
        v
    }

    /// What each volume being formatted asks of the machine reading it (model::fs_limits).
    fn format_caveats(&self) -> Vec<String> {
        use crate::model::fs_limits::{caveats, line, VolumeFacts};
        let lines = |f: &RowFormat, size: u64| -> Vec<String> {
            VolumeFacts::of_kind(f.kind, size, &f.opts)
                .map(|facts| caveats(&facts, size).iter().map(line).collect())
                .unwrap_or_default()
        };
        match self.kind {
            DiskKind::Volume => self
                .disk_size()
                .map(|size| lines(&self.volume, size))
                .unwrap_or_default(),
            DiskKind::Partitioned => {
                let placed = self.builder.plan().unwrap_or_default();
                let formats = self.builder.formats();
                placed
                    .iter()
                    .zip(formats.iter())
                    .filter_map(|(p, f)| f.as_ref().map(|f| lines(f, p.size_bytes)))
                    .flatten()
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    /// Warnings shown on Review that do not block the build.
    pub fn warnings(&self) -> Vec<String> {
        let mut v = Vec::new();
        if self.kind == DiskKind::Cd {
            let limit = self.cd.layout.name_limit();
            if self.cd.name.len() > limit {
                v.push(format!(
                    "Warning: this disc keeps the first {limit} bytes of the volume name"
                ));
            }
            if self
                .disk_size()
                .is_ok_and(|s| s > crate::model::cd_build::CD_R_BYTES)
            {
                v.push(
                    "Info: larger than a 700 MB CD-R - fine for an emulator, too big to burn"
                        .to_string(),
                );
            }
        }
        if self.kind == DiskKind::Partitioned {
            let mut b = self.builder.clone();
            b.validate();
            v.extend(b.errors.into_iter().filter(|e| e.starts_with("Warning:")));
        }
        v.extend(self.format_caveats());
        for t in self.handlers_missing() {
            v.push(format!(
                "Warning: {t} has no handler in the Kickstart ROM and none was added; the \
                 Amiga cannot mount it until its handler is installed"
            ));
        }
        if self.output_is_device
            && (self.kind == DiskKind::Volume
                || self.builder.rows.iter().any(|r| r.format.is_some()))
        {
            v.push(
                "Info: each formatted partition is written in full, so old data on the device \
                 cannot show through."
                    .to_string(),
            );
        }
        v
    }

    // --- acting --------------------------------------------------------------------------

    fn editable(&self) -> bool {
        self.job.is_none()
    }

    /// The disk size the plan lays out on follows the output.
    fn sync_size(&mut self) {
        self.builder.disk_size = self.disk_size().unwrap_or(0);
    }

    fn go(&mut self, forward: bool) {
        let steps = self.steps();
        let Some(at) = steps.iter().position(|s| *s == self.step) else {
            self.step = Step::Kind;
            return;
        };
        let next = if forward {
            at + 1
        } else {
            at.saturating_sub(1)
        };
        if let Some(s) = steps.get(next) {
            if *s != Step::Build {
                self.step = *s;
            }
        }
    }

    fn pick_output(&self) -> Vec<Effect> {
        let name = match self.kind {
            DiskKind::Volume => format!("{}.img", self.volume.kind.id()),
            DiskKind::Partitioned => "disk.img".to_string(),
            DiskKind::Preset => match self.preset.kind {
                PresetKind::X68k => "x68000.hds".to_string(),
                PresetKind::SgiEfs => "irix.img".to_string(),
            },
            DiskKind::Cd => "disc.iso".to_string(),
        };
        let (kind_name, ext) = match self.kind {
            DiskKind::Cd => ("CD-ROM images", "iso"),
            DiskKind::Partitioned | DiskKind::Volume | DiskKind::Preset => {
                ("Raw disk images", "img")
            }
        };
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::OUTPUT_IMAGE,
            title: "Save the new disk image as".to_string(),
            filters: vec![
                (kind_name.to_string(), vec![ext.to_string()]),
                ("All Files".to_string(), vec!["*".to_string()]),
            ],
            save: true,
            file_name: name,
        })]
    }

    fn table_edit(&mut self, action: BuildAction) -> Vec<Effect> {
        if let BuildAction::PickRowSource(row) = action {
            if row >= self.builder.rows.len() || self.probe.is_some() {
                return Vec::new();
            }
            self.source_row = Some(row);
            let mut exts: Vec<String> = crate::model::file_types::DISK_IMAGE_EXTS
                .iter()
                .map(|e| e.to_string())
                .collect();
            exts.dedup();
            return vec![Effect::PickFile(PickSpec {
                purpose: purpose::ROW_SOURCE,
                title: format!("Source image for partition {}", row + 1),
                filters: vec![
                    ("Disk images".to_string(), exts),
                    ("All Files".to_string(), vec!["*".to_string()]),
                ],
                save: false,
                file_name: String::new(),
            })];
        }
        if let BuildAction::SetKind(kind) = action {
            let auto: Vec<bool> = (0..self.builder.rows.len())
                .map(|i| self.row_type_is_suggested(i))
                .collect();
            let sizes = self.planned_sizes();
            self.builder.set_kind(kind);
            for (i, r) in self.builder.rows.iter_mut().enumerate() {
                if let (true, Some(f)) = (auto[i], r.format.as_ref()) {
                    let size = sizes.get(i).copied().unwrap_or(0);
                    r.type_text = crate::model::disk_builder::suggested_type(kind, f.kind, size)
                        .unwrap_or_else(|| {
                            crate::partition::provision::default_type(kind).to_string()
                        });
                }
            }
            return Vec::new();
        }
        partition_edit::apply_build(&mut self.builder, action);
        Vec::new()
    }

    fn picked(&mut self, which: &'static str, path: Option<PathBuf>) {
        let Some(path) = path else {
            self.source_row = None;
            return;
        };
        match which {
            purpose::OUTPUT_IMAGE => {
                if let Some(c) = container_for_path(path.as_path()) {
                    self.container = c;
                }
                self.image = Some(path);
            }
            purpose::SYSTEM_DISK => self.preset.x68k_system_disk = Some(path),
            purpose::CD_FOLDER => self.cd.from_dir = Some(path),
            purpose::DONOR => {
                self.donor_error = None;
                self.donor_job = Some(Job::spawn(
                    "Reading the donor's layout...",
                    Arc::clone(&self.notify),
                    move |_| {
                        Ok(crate::model::disk_builder::Donor::read(path.as_path())
                            .map_err(|e| format!("{e:#}")))
                    },
                ));
            }
            purpose::RDB_HANDLER => {
                if let Some(t) = self.handler_for.take() {
                    self.handlers.retain(|(d, _)| !d.eq_ignore_ascii_case(&t));
                    self.handlers.push((t, path));
                }
            }
            purpose::MAC_DRIVER => self.mac_driver = MacDriver::FromDisk(path),
            purpose::ROW_SOURCE => {
                let Some(row) = self.source_row.take() else {
                    return;
                };
                let job = Job::spawn(
                    "Measuring the source image...",
                    Arc::clone(&self.notify),
                    move |_| {
                        let size = crate::model::source_reader::decoded_image_size(path.as_path())
                            .unwrap_or(0);
                        Ok((path, size))
                    },
                );
                self.probe = Some((row, job));
            }
            _ => {}
        }
    }

    fn planned_sizes(&self) -> Vec<u64> {
        let placed = self.builder.plan().unwrap_or_default();
        placed.iter().map(|p| p.size_bytes).collect()
    }

    /// The row's type is the one its filesystem proposed, so it is ours to change, not the user's.
    fn row_type_is_suggested(&self, row: usize) -> bool {
        let size = self.planned_sizes().get(row).copied().unwrap_or(0);
        let Some(r) = self.builder.rows.get(row) else {
            return false;
        };
        let fs = r.format.as_ref().map(|f| f.kind);
        let kind = self.builder.kind;
        fs.and_then(|fs| crate::model::disk_builder::suggested_type(kind, fs, size))
            .is_some_and(|t| t == r.type_text.trim())
    }

    /// Which Windows reads an NTFS volume decides its version, so ask rather than default.
    fn ask_ntfs_version(&mut self, row: Option<usize>) -> Vec<Effect> {
        self.ntfs_question = Some(row);
        let message = NTFS_VERSIONS
            .iter()
            .map(|(v, _)| format!("NTFS {}: {}.", v.label(), v.readers()))
            .collect::<Vec<_>>()
            .join("\n");
        let choices = NTFS_VERSIONS
            .iter()
            .zip(['3', '1'])
            .map(|((_, label), key)| Choice {
                label: format!("NTFS {label}"),
                key,
            })
            .collect();
        vec![Effect::Choose(ChoiceSpec {
            purpose: purpose::NTFS_VERSION,
            title: "Which Windows will use this NTFS volume?".into(),
            message,
            choices,
            dismiss: 0,
        })]
    }

    /// The answer to [`Self::ask_ntfs_version`], for the target it was asked about.
    fn chose_ntfs_version(&mut self, choice: usize) {
        let Some(row) = self.ntfs_question.take() else {
            return;
        };
        let Some((version, _)) = NTFS_VERSIONS.get(choice) else {
            return;
        };
        let format = match row {
            Some(row) => self
                .builder
                .rows
                .get_mut(row)
                .and_then(|r| r.format.as_mut()),
            None => Some(&mut self.volume),
        };
        if let Some(f) = format.filter(|f| f.kind == FsKind::Ntfs) {
            OptionEdit::NtfsVersion(*version).apply(&mut f.opts);
        }
    }

    fn set_row_fs(&mut self, row: usize, fs: Option<FsKind>) {
        let kind = self.builder.kind;
        let size = self.planned_sizes().get(row).copied().unwrap_or(0);
        let suggested = self.row_type_is_suggested(row);
        let Some(r) = self.builder.rows.get_mut(row) else {
            return;
        };
        r.copy_from = None;
        match fs {
            None => r.format = None,
            Some(fs) => {
                r.source = None;
                r.source_size = 0;
                let mut f = RowFormat::new(fs);
                if let Some(label) = r.format.as_ref().and_then(|old| typed_label(old, fs)) {
                    f.opts.label = label;
                }
                // A default, donor or proposed type follows the filesystem; one the user typed stays.
                if r.type_text.trim().is_empty()
                    || r.type_text.trim() == crate::partition::provision::default_type(kind)
                    || r.donor_part.is_some()
                    || suggested
                {
                    if let Some(t) = crate::model::disk_builder::suggested_type(kind, fs, size) {
                        r.type_text = t;
                    }
                }
                r.format = Some(f);
            }
        }
    }

    fn create(&mut self) {
        if self.step != Step::Review || self.job.is_some() {
            return;
        }
        if let Some(e) = self.review_blocked() {
            self.push(LogLevel::Warning, e);
            return;
        }
        let target = match (self.output_is_device, &self.device, &self.image) {
            (true, Some((_, d)), _) => d.path.clone(),
            (false, _, Some(p)) => p.clone(),
            _ => return,
        };
        let size = self.disk_size();
        if self.kind == DiskKind::Cd {
            let c = &self.cd;
            let request = crate::model::provision_runner::BuildRequest::Cd {
                target_path: target.clone(),
                request: crate::model::cd_build::CdRequest {
                    layout: c.layout,
                    size: size.as_ref().ok().copied(),
                    name: c.name.clone(),
                    from_dir: c.from_dir.clone(),
                    import: c.import.clone(),
                    mac_block_size: None,
                    bytes_per_inode: None,
                },
            };
            self.push(
                LogLevel::Info,
                format!("Building a CD-ROM image at {}", target.display()),
            );
            self.start_job(request, None);
            return;
        }
        let Ok(size) = size else {
            return;
        };
        let request = match self.kind {
            // Started above, before the size was required.
            DiskKind::Cd => return,
            DiskKind::Preset => {
                let p = &self.preset;
                crate::model::provision_runner::BuildRequest::Preset {
                    target_path: target.clone(),
                    size: self.preset_size(size),
                    preset: match p.kind {
                        PresetKind::X68k => crate::model::provision_runner::Preset::X68k {
                            scsi: p.x68k_scsi,
                            partitions: p.x68k_partitions,
                            system_disk: p.x68k_system_disk.clone(),
                        },
                        PresetKind::SgiEfs => crate::model::provision_runner::Preset::SgiEfs {
                            label: p.sgi_label.clone(),
                        },
                    },
                }
            }
            DiskKind::Volume => crate::model::provision_runner::BuildRequest::Volume {
                target_path: target.clone(),
                size,
                format: self.volume.clone(),
                full_write: self.output_is_device,
            },
            DiskKind::Partitioned => {
                let Some(placed) = self.builder.validate() else {
                    return;
                };
                crate::model::provision_runner::BuildRequest::Partitioned(
                    crate::model::provision_runner::ProvisionRequest {
                        target_path: target.clone(),
                        target_size_bytes: size,
                        kind: self.builder.kind,
                        geometry: self.builder.geometry,
                        sources: self.builder.sources(),
                        formats: self.builder.formats(),
                        full_write: self.output_is_device,
                        rdb_handlers: if self.takes_handlers() {
                            self.handlers.clone()
                        } else {
                            Vec::new()
                        },
                        head: match self.builder.patched_head() {
                            Ok(h) => h.map(|(head, _)| head),
                            Err(e) => {
                                self.push(LogLevel::Error, format!("{e:#}"));
                                return;
                            }
                        },
                        donor: self.builder.donor.as_ref().map(|d| d.path.clone()),
                        copies: self.builder.copies(placed.as_slice()),
                        partitions: placed,
                        mac_driver: self.mac_driver.clone(),
                    },
                )
            }
        };
        self.push(
            LogLevel::Info,
            format!("Building on {} ({})", target.display(), format_size(size)),
        );
        let container = (!self.output_is_device).then_some(self.container);
        self.start_job(request, container);
    }

    /// Run `request` as the build job, its log lines landing in the panel.
    fn start_job(
        &mut self,
        request: crate::model::provision_runner::BuildRequest,
        container: Option<ExportFormat>,
    ) {
        let sink = Arc::clone(&self.log);
        self.outcome = None;
        self.finished = None;
        self.step = Step::Build;
        self.job = Some(Job::spawn(
            "Building disk",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_phase("Writing the disk...");
                let status = crate::model::provision_runner::start_build(request, container);
                let mut logged = 0;
                crate::model::worker::wait_spawned(ctx, &status, |s, ctx| {
                    if let Ok(mut l) = sink.lock() {
                        for msg in s.log_messages.iter().skip(logged) {
                            l.push((LogLevel::Info, msg.clone()));
                        }
                    }
                    logged = s.log_messages.len();
                    if s.total_bytes > 0 {
                        ctx.set_phase("Building disk");
                        ctx.set_bytes(s.current_bytes, s.total_bytes);
                    }
                    if ctx.is_cancelled() {
                        s.cancel_requested = true;
                    }
                    s.finished
                });
                let err = crate::model::worker::lock_status(&*status).error.clone();
                Ok(match err {
                    Some(e) => Err(e),
                    None => Ok(()),
                })
            },
        ));
    }

    fn poll(&mut self) {
        if let Some(done) = self.donor_job.as_ref().and_then(job_outcome) {
            self.donor_job = None;
            match done.and_then(|r| r) {
                Ok(d) => {
                    let size = self.disk_size().unwrap_or(d.disk_size);
                    self.builder = DiskBuilder::from_donor(d, size);
                }
                Err(e) => self.donor_error = Some(e),
            }
        }
        if let Some(done) = self.probe.as_ref().and_then(|(_, j)| job_outcome(j)) {
            if let Some((row, _)) = self.probe.take() {
                if let Ok((path, size)) = done {
                    partition_edit::set_row_source(&mut self.builder, row, path, size);
                    if let Some(r) = self.builder.rows.get_mut(row) {
                        r.format = None;
                        r.copy_from = None;
                    }
                }
            }
        }
        if let Some(done) = self.job.as_ref().and_then(job_outcome) {
            self.finished = self.job.take().map(|j| j.snapshot());
            let outcome = done.and_then(|r| r);
            // The runner logs its own completion line.
            if let Err(e) = &outcome {
                self.push(LogLevel::Error, format!("Disk build failed: {e}"));
            }
            self.outcome = Some(outcome);
        }
    }

    /// Block until the running build ends; for a headless shell or a test.
    pub fn wait(&mut self) {
        if let Some(j) = self.probe.as_ref() {
            j.1.wait();
        }
        if let Some(j) = self.donor_job.as_ref() {
            j.wait();
        }
        if let Some(j) = self.job.as_ref() {
            j.wait();
        }
        self.poll();
    }
}

impl Controller for BuildDiskController {
    type Action = BuildDiskAction;

    fn apply(&mut self, action: BuildDiskAction) -> Vec<Effect> {
        let editable = self.editable();
        match action {
            BuildDiskAction::Next => {
                if self.step == Step::Review {
                    self.create();
                } else if self.next_blocked().is_none() {
                    self.go(true);
                }
            }
            BuildDiskAction::Back => {
                if editable {
                    self.go(false);
                }
            }
            BuildDiskAction::GoTo(step) => {
                let steps = self.steps();
                let here = steps.iter().position(|s| *s == self.step);
                let there = steps.iter().position(|s| *s == step);
                if editable && step != Step::Build && there < here {
                    self.step = step;
                }
            }
            BuildDiskAction::Cancel => {
                if let Some(j) = &self.job {
                    j.cancel();
                }
            }
            BuildDiskAction::BuildAnother => {
                if editable {
                    self.outcome = None;
                    self.finished = None;
                    self.confirm_text.clear();
                    // Asked for again: the save picker is what warns before an overwrite.
                    self.image = None;
                    self.step = Step::Kind;
                }
            }
            BuildDiskAction::Picked { purpose, path } => {
                if editable {
                    self.picked(purpose, path);
                    self.sync_size();
                }
            }
            _ if !editable => {}
            BuildDiskAction::SetKind(k) => {
                // A CD is a file, raw, with a CD's size.
                if k == DiskKind::Cd && self.kind != DiskKind::Cd {
                    self.output_is_device = false;
                    self.container = ExportFormat::Raw;
                    self.size_text = "600M".to_string();
                }
                self.kind = k;
            }
            BuildDiskAction::SetOutputIsDevice(on) => self.output_is_device = on,
            BuildDiskAction::SetContainer(c) => self.container = c,
            BuildDiskAction::SelectDevice { row, device } => {
                self.device = Some((row, device));
                self.confirm_text.clear();
            }
            BuildDiskAction::ClearDevice => {
                self.device = None;
                self.confirm_text.clear();
            }
            BuildDiskAction::PickOutputImage => return self.pick_output(),
            BuildDiskAction::SetSize(t) => self.size_text = t,
            BuildDiskAction::Table(a) => return self.table_edit(a),
            BuildDiskAction::SetRowFs { row, fs } => {
                self.set_row_fs(row, fs);
                if fs == Some(FsKind::Ntfs) {
                    return self.ask_ntfs_version(Some(row));
                }
            }
            BuildDiskAction::SetRowOptions { row, opts } => {
                if let Some(f) = self
                    .builder
                    .rows
                    .get_mut(row)
                    .and_then(|r| r.format.as_mut())
                {
                    f.opts = opts;
                }
            }
            BuildDiskAction::SetVolumeFs(fs) => {
                let label = typed_label(&self.volume, fs);
                self.volume = RowFormat::new(fs);
                if let Some(label) = label {
                    self.volume.opts.label = label;
                }
                if fs == FsKind::Ntfs {
                    return self.ask_ntfs_version(None);
                }
            }
            BuildDiskAction::Chose { purpose, choice } => {
                if purpose == purpose::NTFS_VERSION {
                    self.chose_ntfs_version(choice);
                }
            }
            BuildDiskAction::SetVolumeOptions(opts) => self.volume.opts = opts,
            BuildDiskAction::SetPreset(p) => self.preset = p,
            BuildDiskAction::SetCd(c) => {
                // A layout change takes that layout's default name, unless one was typed.
                if c.layout != self.cd.layout && self.cd.name == self.cd.layout.default_name() {
                    self.cd = CdView {
                        name: c.layout.default_name().to_string(),
                        ..c
                    };
                } else {
                    self.cd = c;
                }
            }
            BuildDiskAction::SetCdOption(option, on) => {
                let applies = self
                    .cd_options()
                    .iter()
                    .any(|r| r.option == option && r.unavailable.is_none());
                if applies {
                    *option.flag(&mut self.cd.import) = on;
                }
            }
            BuildDiskAction::SetCdSidecars(choice) => {
                let offered = CdSidecars::choices(self.cd.layout).contains(&choice);
                if offered && self.cd_sidecars_unavailable().is_none() {
                    choice.apply(&mut self.cd.import);
                }
            }
            BuildDiskAction::PickCdFolder => {
                return vec![Effect::PickFolder(PickSpec {
                    purpose: purpose::CD_FOLDER,
                    title: "Fill the disc from this folder".to_string(),
                    filters: Vec::new(),
                    save: false,
                    file_name: String::new(),
                })];
            }
            BuildDiskAction::PickDonor => {
                return vec![Effect::PickFile(PickSpec {
                    purpose: purpose::DONOR,
                    title: "A disk to take the layout from".to_string(),
                    filters: vec![("All Files".to_string(), vec!["*".to_string()])],
                    save: false,
                    file_name: String::new(),
                })];
            }
            BuildDiskAction::ClearDonor => {
                let kind = self.builder.kind;
                self.builder = DiskBuilder::new(kind, self.builder.disk_size);
                self.donor_error = None;
            }
            BuildDiskAction::SetRowCopy(row) => {
                if let Some(r) = self.builder.rows.get_mut(row) {
                    if r.donor_part.is_some() {
                        r.copy_from = r.donor_part;
                        r.format = None;
                        r.source = None;
                        r.source_size = 0;
                    }
                }
            }
            BuildDiskAction::PickSystemDisk => {
                return vec![Effect::PickFile(PickSpec {
                    purpose: purpose::SYSTEM_DISK,
                    title: "Human68k system floppy (.dim, .xdf, .hdm, .d88)".to_string(),
                    filters: vec![
                        (
                            "X68000 floppies".to_string(),
                            ["dim", "xdf", "hdm", "d88"]
                                .iter()
                                .map(|e| e.to_string())
                                .collect(),
                        ),
                        ("All Files".to_string(), vec!["*".to_string()]),
                    ],
                    save: false,
                    file_name: String::new(),
                })];
            }
            BuildDiskAction::SetConfirmText(t) => self.confirm_text = t,
            BuildDiskAction::AddHandler(dostype) => {
                if !self.takes_handlers() {
                    return Vec::new();
                }
                self.handler_for = Some(dostype.clone());
                return vec![Effect::PickFile(PickSpec {
                    purpose: purpose::RDB_HANDLER,
                    title: format!("AmigaDOS handler for {dostype} (e.g. L:SmartFilesystem)"),
                    filters: vec![("All Files".to_string(), vec!["*".to_string()])],
                    save: false,
                    file_name: String::new(),
                })];
            }
            BuildDiskAction::RemoveHandler(i) => {
                if i < self.handlers.len() {
                    self.handlers.remove(i);
                }
            }
            BuildDiskAction::SetMacDriver(d) => self.mac_driver = d,
            BuildDiskAction::PickMacDriver => {
                if !self.takes_mac_driver() {
                    return Vec::new();
                }
                return vec![Effect::PickFile(PickSpec {
                    purpose: purpose::MAC_DRIVER,
                    title: "An Apple-formatted disk to take the driver from".to_string(),
                    filters: vec![("All Files".to_string(), vec!["*".to_string()])],
                    save: false,
                    file_name: String::new(),
                })];
            }
            BuildDiskAction::Create => self.create(),
        }
        self.sync_size();
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll();
        Vec::new()
    }
}
