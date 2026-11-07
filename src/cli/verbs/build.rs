//! `rb-cli build TARGET ...` — Build Disk from the command line: one request, the same
//! `BuildDiskController` the GUI tab and the TUI screen walk.
//!
//! The flags fill the wizard's steps (kind, output, size, table, rows, formats, sources); the
//! controller's own checks decide what is refused, and the build runs as its job. A device
//! target goes through the same safety preflight as `rb-cli write` and needs `--yes`.
//! See docs/build_disk_plan.md.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, ValueEnum};

use crate::cli::logging::log_stderr;
use crate::controller::build_disk::{
    containers, purpose, BuildDiskAction as A, BuildDiskController, DiskKind, ExportFormat, FsKind,
    MacDriver, NoNotify, PresetKind, Step, TableKind,
};
use crate::controller::partition_edit::BuildAction;
use crate::controller::Controller as _;

/// The partition tables `build` writes, by the names `new hd` uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BuildTable {
    Mbr,
    Gpt,
    Apm,
    Sgi,
    #[value(name = "sgi-dklabel")]
    SgiDklabel,
    #[value(name = "x68k")]
    X68k,
    Rdb,
    Sun,
    Next,
    #[value(name = "solaris-x86")]
    SolarisX86,
    Atari,
}

impl BuildTable {
    fn kind(self) -> TableKind {
        match self {
            BuildTable::Mbr => TableKind::Mbr,
            BuildTable::Gpt => TableKind::Gpt,
            BuildTable::Apm => TableKind::Apm,
            BuildTable::Sgi => TableKind::Sgi,
            BuildTable::SgiDklabel => TableKind::SgiDkLabel,
            BuildTable::X68k => TableKind::X68k,
            BuildTable::Rdb => TableKind::Rdb,
            BuildTable::Sun => TableKind::Sun,
            BuildTable::Next => TableKind::Next,
            BuildTable::SolarisX86 => TableKind::SolarisX86,
            BuildTable::Atari => TableKind::Atari,
        }
    }
}

/// The bootable presets `build` makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BuildPreset {
    X68k,
    #[value(name = "sgi-efs")]
    SgiEfs,
}

/// The containers `build` can encode an image into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BuildContainer {
    Raw,
    Vhd,
    #[value(name = "vhd-dynamic")]
    VhdDynamic,
    Qcow2,
    Vmdk,
    Chd,
    #[value(name = "2mg")]
    TwoMg,
    Dc42,
    Woz,
    Moof,
}

impl BuildContainer {
    fn format(self) -> ExportFormat {
        match self {
            BuildContainer::Raw => ExportFormat::Raw,
            BuildContainer::Vhd => ExportFormat::Vhd,
            BuildContainer::VhdDynamic => ExportFormat::VhdDynamic,
            BuildContainer::Qcow2 => ExportFormat::Qcow2,
            BuildContainer::Vmdk => ExportFormat::VmdkSparse,
            BuildContainer::Chd => ExportFormat::Chd,
            BuildContainer::TwoMg => ExportFormat::TwoMg,
            BuildContainer::Dc42 => ExportFormat::Dc42,
            BuildContainer::Woz => ExportFormat::Woz,
            BuildContainer::Moof => ExportFormat::Moof,
        }
    }
}

#[derive(Debug, Args)]
pub struct BuildArgs {
    /// The image file to create, or a device path (`/dev/sdX`, `/dev/diskN`,
    /// `\\.\PhysicalDriveN`) to build on. A device needs `--yes` and is
    /// built at its own size.
    pub target: PathBuf,

    /// Disk (or volume) size for an image target, e.g. `64M`, `2G`. Ignored
    /// for a device and for a fixed-geometry volume.
    #[arg(long, default_value = "512M")]
    pub size: String,

    /// Partition table to write. Leave it out for one bare volume (give
    /// `--fs`).
    #[arg(long, value_enum)]
    pub table: Option<BuildTable>,

    /// A partition, in disk order: `SIZE[:TYPE[:NAME]]`, where SIZE is
    /// `20M` / `1G` / bytes or `rest`, and TYPE is the table's own type
    /// value (MBR byte, GPT GUID, `Apple_HFS`, `DOS\3`, ...). Repeat for
    /// each partition; the default is one `rest` partition.
    #[arg(long = "partition", value_name = "SPEC")]
    pub partitions: Vec<String>,

    /// Format partition N blank: `N=FS[:LABEL]`, where FS is a `new volume`
    /// / `new floppy` filesystem name (`fat`, `hfs`, `pfs3`, `ufs-solaris`,
    /// ...). The partition's type follows the filesystem unless the
    /// `--partition` spec gave one.
    #[arg(long = "format", value_name = "N=FS[:LABEL]")]
    pub formats: Vec<String>,

    /// Pour an image into partition N after the table is written:
    /// `N=PATH`. Any image Rusty Backup can read works.
    #[arg(long = "fill", value_name = "N=PATH")]
    pub fills: Vec<String>,

    /// Mark partition N bootable: the MBR active flag (one partition) or the
    /// RDB bootable flag. Repeatable on RDB.
    #[arg(long = "bootable", value_name = "N")]
    pub bootable: Vec<usize>,

    /// Embed an Amiga filesystem handler in the RDB, `DOSTYPE=PATH`
    /// (repeatable). PATH is the handler's AmigaDOS load file (`L:PFS3`,
    /// `L:SmartFilesystem`); without one, a DosType the ROM lacks cannot
    /// mount unaided.
    #[arg(long = "filesystem", value_name = "DOSTYPE=PATH")]
    pub filesystems: Vec<String>,

    /// The Mac driver on an APM disk: `bundled` (the default: a donor's own
    /// driver if it has one, otherwise Apple SCSI 7.3.5), `none`, or the path
    /// of an Apple-formatted disk to take the driver from. The driver serves
    /// SCSI; a Mac with the disk on IDE won't mount it.
    #[arg(long = "mac-driver", value_name = "bundled|none|DISK")]
    pub mac_driver: Option<String>,

    /// Partition-start alignment, e.g. `1M` or `63s`; blank uses the
    /// table's default.
    #[arg(long, default_value = "")]
    pub align: String,

    /// Heads for the cylinder-aligned tables (SGI, RDB, Sun, NeXT,
    /// Solaris x86).
    #[arg(long)]
    pub heads: Option<u16>,

    /// Sectors per track for the cylinder-aligned tables.
    #[arg(long)]
    pub sectors: Option<u16>,

    /// With no `--table`: the filesystem of the one bare volume.
    #[arg(long)]
    pub fs: Option<String>,

    /// Start from this disk image's layout: its table, special partitions
    /// and their contents, at the new `--size`. Each partition is copied
    /// unless `--format N=FS` or `--empty N` says otherwise; `--partition
    /// SIZE` sets the sizes in order. A copied filesystem that can resize
    /// grows to its partition; one that can't keeps its size. A label (SGI,
    /// Sun, NeXT, RDB, AHDI, X68000) keeps its head region verbatim, resized
    /// for the new disk.
    #[arg(long = "from", value_name = "DONOR", conflicts_with_all = ["table", "fs", "preset"])]
    pub from: Option<PathBuf>,

    /// With `--from`: leave partition N empty instead of copying it.
    #[arg(long = "empty", value_name = "N")]
    pub empty: Vec<usize>,

    /// Build a fixed bootable preset instead of a table or a volume:
    /// `x68k` (IPL, X68000 table, Human68k) or `sgi-efs` (volume header, EFS
    /// root). The same disks `new hd x68k` / `new hd sgi-efs` make.
    #[arg(long, value_enum, conflicts_with_all = ["table", "fs"])]
    pub preset: Option<BuildPreset>,

    /// `--preset x68k`: write the SCSI form (1024-byte sectors) rather than SASI.
    #[arg(long)]
    pub scsi: bool,

    /// `--preset x68k`: how many Human68k partitions, 1 to 8.
    #[arg(long, default_value_t = 1)]
    pub x68k_partitions: usize,

    /// `--preset x68k`: a Human68k system floppy cloned into partition 1.
    #[arg(long)]
    pub system_disk: Option<PathBuf>,

    /// With no `--table`: the volume label.
    #[arg(long)]
    pub label: Option<String>,

    /// With no `--table`: the fixed geometry of a floppy format, e.g. `d71`
    /// for a 1571 disk (`cbm`), `80` tracks (`dfs`, `rsdos`), `80ds`
    /// (`dragondos`). The default is the format's first.
    #[arg(long)]
    pub geometry: Option<String>,

    /// Container to encode an image target into: raw, vhd, vhd-dynamic,
    /// qcow2, vmdk, chd, 2mg, dc42, woz or moof. Defaults to the one the
    /// target's extension names (`.chd`, `.vhd`, ...), else raw.
    #[arg(long, value_enum)]
    pub container: Option<BuildContainer>,

    /// Overwrite an existing image file.
    #[arg(long)]
    pub force: bool,

    /// Required to build on a device; everything on it is erased.
    #[arg(long)]
    pub yes: bool,
}

/// Split `N=VALUE` into a 0-based partition index and the value.
fn numbered(spec: &str, flag: &str, rows: usize) -> Result<(usize, String)> {
    let (n, v) = spec
        .split_once('=')
        .ok_or_else(|| anyhow!("{flag} wants N=VALUE, got '{spec}'"))?;
    let n: usize = n
        .trim()
        .parse()
        .with_context(|| format!("bad partition number in {flag} '{spec}'"))?;
    if n == 0 || n > rows {
        bail!("{flag} {n} names partition {n}, but only {rows} were defined");
    }
    Ok((n - 1, v.trim().to_string()))
}

fn fs_named(name: &str) -> Result<FsKind> {
    FsKind::from_id(name).ok_or_else(|| {
        let all: Vec<&str> = FsKind::ALL.iter().map(|k| k.id()).collect();
        anyhow!("unknown filesystem '{name}'; one of: {}", all.join(", "))
    })
}

pub fn run(args: BuildArgs) -> Result<()> {
    let mut c = BuildDiskController::new(Arc::new(NoNotify));
    let act = |c: &mut BuildDiskController, a: A| {
        c.apply(a);
    };

    // Kind and output.
    let device = crate::os::is_device_path(args.target.as_path());
    if device {
        if !args.yes {
            bail!(
                "building on {} erases it; pass --yes to confirm",
                args.target.display()
            );
        }
        let pre = crate::cli::device_safety::preflight(args.target.as_path(), false)?;
        let disk = pre
            .device
            .ok_or_else(|| anyhow!("{} is not in the device list", args.target.display()))?;
        act(&mut c, A::SetOutputIsDevice(true));
        act(
            &mut c,
            A::SelectDevice {
                row: 0,
                device: disk,
            },
        );
    } else {
        if args.target.exists() && !args.force {
            bail!(
                "{} already exists; pass --force to overwrite",
                args.target.display()
            );
        }
        act(
            &mut c,
            A::Picked {
                purpose: purpose::OUTPUT_IMAGE,
                path: Some(args.target.clone()),
            },
        );
    }
    act(&mut c, A::SetSize(args.size.clone()));
    if let Some(k) = args.container {
        if device {
            bail!("--container is for an image target; a device is written raw");
        }
        let fmt = k.format();
        if !containers().contains(&fmt) {
            bail!("this build has no {} support", fmt.description());
        }
        act(&mut c, A::SetContainer(fmt));
    }

    if let Some(donor) = &args.from {
        act(&mut c, A::SetKind(DiskKind::Partitioned));
        act(
            &mut c,
            A::Picked {
                purpose: purpose::DONOR,
                path: Some(donor.clone()),
            },
        );
        c.wait();
        c.tick();
        if let Some(e) = c.donor_error() {
            bail!("reading {}: {e}", donor.display());
        }
        let rows = c.table_view().rows.len();
        for (i, spec) in args.partitions.iter().enumerate() {
            if i >= rows {
                bail!("the donor has {rows} partitions; --partition gave more");
            }
            let size = spec.split(':').next().unwrap_or("").trim().to_string();
            c.apply(A::Table(BuildAction::SetRowSize { row: i, text: size }));
        }
        for &n in &args.empty {
            if n == 0 || n > rows {
                bail!("--empty {n} names partition {n}, but the donor has {rows}");
            }
            c.apply(A::SetRowFs {
                row: n - 1,
                fs: None,
            });
        }
        for spec in &args.formats {
            let (row, v) = numbered(spec.as_str(), "--format", rows)?;
            let (fs, label) = match v.split_once(':') {
                Some((f, l)) => (f.to_string(), Some(l.to_string())),
                None => (v, None),
            };
            c.apply(A::SetRowFs {
                row,
                fs: Some(fs_named(fs.as_str())?),
            });
            if let Some(label) = label {
                if let Some(t) = c.format_targets().into_iter().find(|t| t.row == Some(row)) {
                    let mut opts = t.opts;
                    opts.label = label;
                    c.apply(A::SetRowOptions { row, opts });
                }
            }
        }
        return finish(c);
    }
    if let Some(p) = args.preset {
        act(&mut c, A::SetKind(DiskKind::Preset));
        let mut view = c.preset().clone();
        view.kind = match p {
            BuildPreset::X68k => PresetKind::X68k,
            BuildPreset::SgiEfs => PresetKind::SgiEfs,
        };
        view.x68k_scsi = args.scsi;
        view.x68k_partitions = args.x68k_partitions;
        view.x68k_system_disk = args.system_disk.clone();
        if let Some(label) = &args.label {
            view.sgi_label = label.clone();
        }
        act(&mut c, A::SetPreset(view));
        return finish(c);
    }
    match args.table {
        None => {
            let fs = args.fs.as_deref().ok_or_else(|| {
                anyhow!("give --table for a partitioned disk, or --fs for a volume")
            })?;
            if !args.partitions.is_empty() || !args.formats.is_empty() || !args.fills.is_empty() {
                bail!("--partition / --format / --fill need --table");
            }
            act(&mut c, A::SetKind(DiskKind::Volume));
            act(&mut c, A::SetVolumeFs(fs_named(fs)?));
            if args.label.is_some() || args.geometry.is_some() {
                let mut opts = c.format_targets()[0].opts.clone();
                if let Some(label) = &args.label {
                    opts.label = label.clone();
                }
                opts.geometry = args.geometry.clone();
                act(&mut c, A::SetVolumeOptions(opts));
            }
        }
        Some(table) => {
            if args.fs.is_some() || args.label.is_some() || args.geometry.is_some() {
                bail!("--fs / --label are for a bare volume; use --format N=FS:LABEL with --table");
            }
            act(&mut c, A::SetKind(DiskKind::Partitioned));
            act(&mut c, A::Table(BuildAction::SetKind(table.kind())));
            act(&mut c, A::Table(BuildAction::SetAlign(args.align.clone())));
            if let Some(h) = args.heads {
                act(&mut c, A::Table(BuildAction::SetHeads(h)));
            }
            if let Some(s) = args.sectors {
                act(&mut c, A::Table(BuildAction::SetSectorsPerTrack(s)));
            }
            set_rows(&mut c, &args)?;
        }
    }

    finish(c)
}

/// Walk the steps the way a user would (the first refusal is the error), then build.
fn finish(mut c: BuildDiskController) -> Result<()> {
    let act = |c: &mut BuildDiskController, a: A| {
        c.apply(a);
    };
    while c.step() != Step::Review {
        if let Some(why) = c.next_blocked() {
            bail!("{}: {why}", c.step().label());
        }
        act(&mut c, A::Next);
    }
    if let Some(want) = c.confirm_expected() {
        act(&mut c, A::SetConfirmText(want));
    }
    if let Some(why) = c.next_blocked() {
        bail!("{why}");
    }
    for line in c.review_lines() {
        log_stderr(line);
    }
    for w in c.warnings() {
        log_stderr(w);
    }
    act(&mut c, A::Next);
    c.wait();
    for (_, msg) in c.take_log() {
        log_stderr(msg);
    }
    match c.outcome() {
        Some(Ok(())) => Ok(()),
        Some(Err(e)) => bail!("{e}"),
        None => bail!("the build did not start"),
    }
}

/// The `--partition`, `--format` and `--fill` rows.
fn set_rows(c: &mut BuildDiskController, args: &BuildArgs) -> Result<()> {
    let specs: Vec<String> = if args.partitions.is_empty() {
        vec!["rest".to_string()]
    } else {
        args.partitions.clone()
    };
    while c.table_view().rows.len() < specs.len() {
        let before = c.table_view().rows.len();
        c.apply(A::Table(BuildAction::AddRow));
        if c.table_view().rows.len() == before {
            bail!("this table holds at most {before} partitions");
        }
    }
    let mut typed = vec![false; specs.len()];
    for (i, spec) in specs.iter().enumerate() {
        let mut parts = spec.splitn(3, ':');
        let size = parts.next().unwrap_or("").trim();
        if size.is_empty() {
            bail!("empty --partition spec");
        }
        c.apply(A::Table(BuildAction::SetRowSize {
            row: i,
            text: size.to_string(),
        }));
        if let Some(t) = parts.next().map(str::trim).filter(|t| !t.is_empty()) {
            c.apply(A::Table(BuildAction::SetRowType {
                row: i,
                text: t.to_string(),
            }));
            typed[i] = true;
        }
        if let Some(n) = parts.next().map(str::trim).filter(|n| !n.is_empty()) {
            c.apply(A::Table(BuildAction::SetRowName {
                row: i,
                text: n.to_string(),
            }));
        }
    }
    for spec in &args.formats {
        let (row, v) = numbered(spec.as_str(), "--format", specs.len())?;
        let (fs, label) = match v.split_once(':') {
            Some((f, l)) => (f.to_string(), Some(l.to_string())),
            None => (v, None),
        };
        let typed_type = typed[row].then(|| c.table_view().rows[row].type_text.clone());
        c.apply(A::SetRowFs {
            row,
            fs: Some(fs_named(fs.as_str())?),
        });
        // A type the spec named wins over the one the filesystem suggests.
        if let Some(t) = typed_type {
            c.apply(A::Table(BuildAction::SetRowType { row, text: t }));
        }
        if let Some(label) = label {
            let t = c
                .format_targets()
                .into_iter()
                .find(|t| t.row == Some(row))
                .ok_or_else(|| anyhow!("partition {} does not format", row + 1))?;
            let mut opts = t.opts;
            opts.label = label;
            c.apply(A::SetRowOptions { row, opts });
        }
    }
    for &n in &args.bootable {
        if n == 0 || n > specs.len() {
            bail!(
                "--bootable {n} names partition {n}, but only {} were defined",
                specs.len()
            );
        }
        c.apply(A::Table(BuildAction::SetRowBootable {
            row: n - 1,
            on: true,
        }));
    }
    for spec in &args.filesystems {
        let (dostype, path) = spec
            .split_once('=')
            .ok_or_else(|| anyhow!("--filesystem wants DOSTYPE=PATH, got '{spec}'"))?;
        if !c.takes_handlers() {
            bail!("--filesystem is an Amiga RDB feature; give --table rdb");
        }
        c.apply(A::AddHandler(dostype.trim().to_string()));
        c.apply(A::Picked {
            purpose: purpose::RDB_HANDLER,
            path: Some(PathBuf::from(path.trim())),
        });
    }
    if let Some(spec) = &args.mac_driver {
        if !c.takes_mac_driver() {
            bail!("--mac-driver is an APM feature; give --table apm or an APM donor");
        }
        let (driver, path) = match spec.as_str() {
            "bundled" => (MacDriver::Bundled, None),
            "none" => (MacDriver::Off, None),
            path => (MacDriver::Bundled, Some(PathBuf::from(path))),
        };
        c.apply(A::SetMacDriver(driver));
        if path.is_some() {
            c.apply(A::PickMacDriver);
            c.apply(A::Picked {
                purpose: purpose::MAC_DRIVER,
                path,
            });
        }
    }
    for spec in &args.fills {
        let (row, path) = numbered(spec.as_str(), "--fill", specs.len())?;
        let path = PathBuf::from(path);
        if !path.exists() {
            bail!("--fill source {} does not exist", path.display());
        }
        c.apply(A::Table(BuildAction::PickRowSource(row)));
        c.apply(A::Picked {
            purpose: purpose::ROW_SOURCE,
            path: Some(path),
        });
        c.wait();
    }
    Ok(())
}
