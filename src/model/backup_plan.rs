//! Backup planning rules shared by every backup front end.
//!
//! [`MinSizes`] holds what the per-partition minimum-size walks found (in-place trim point,
//! defragmented floor, fragmentation) and the per-partition Compact Space toggle, with the rule
//! that turns them into the minimum a row shows and the auto-check of Compact. [`build_config`]
//! turns the user's options and the partition selection into the engine's [`BackupConfig`]: the
//! split rule, the single-file-CHD size policy and targets, the Compact opt-in set and the
//! precomputed minimums. `controller::backup` drives them; see docs/phase2_commander_plan.md
//! § "Slice 12".

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::backup::metadata::SizePolicy;
use crate::backup::{BackupConfig, ChecksumType, CompressionType};
use crate::model::size_mode::SizeMode;
use crate::partition::PartitionInfo;
use crate::rbformats::chd_options::ChdOptions;

/// Compact Space is auto-checked when compacting saves at least this share of the in-place size.
pub const COMPACT_AUTOCHECK_SAVINGS_PCT: f32 = 5.0;

/// The minimum-size walks' results and the Compact toggles, keyed by partition index.
#[derive(Debug, Default, Clone)]
pub struct MinSizes {
    pub in_place: HashMap<usize, u64>,
    pub defrag: HashMap<usize, u64>,
    pub fragmentation: HashMap<usize, f32>,
    compact: HashMap<usize, bool>,
    /// Partitions whose Compact default was applied once, so a re-run keeps the user's choice.
    compact_default_applied: HashSet<usize>,
}

impl MinSizes {
    pub fn compact_enabled(&self, idx: usize) -> bool {
        self.compact.get(&idx).copied().unwrap_or(false)
    }

    pub fn set_compact(&mut self, idx: usize, on: bool) {
        self.compact.insert(idx, on);
    }

    /// Compact is live with a defragmenting writer, or when a packed minimum beats the in-place trim.
    pub fn compact_available(&self, idx: usize, type_byte: u8, type_str: Option<&str>) -> bool {
        if crate::fs::has_defragmenting_writer(type_byte, type_str) {
            return true;
        }
        matches!(
            (self.defrag.get(&idx), self.in_place.get(&idx)),
            (Some(&d), Some(&p)) if d < p
        )
    }

    /// The minimum a row shows: the defragmented floor while Compact is on and possible, else in place.
    pub fn effective(&self, idx: usize, type_byte: u8, type_str: Option<&str>) -> Option<u64> {
        let in_place = self.in_place.get(&idx).copied();
        let defrag = self.defrag.get(&idx).copied();
        if self.compact_available(idx, type_byte, type_str) && self.compact_enabled(idx) {
            defrag.or(in_place)
        } else {
            in_place.or(defrag)
        }
    }

    /// Store a finished walk, clamped to the partition, and auto-check Compact once when it pays.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        idx: usize,
        in_place: Option<u64>,
        defragmented: Option<u64>,
        fragmentation_percent: Option<f32>,
        partition_size: u64,
        type_byte: u8,
        type_str: Option<&str>,
    ) {
        let in_place = in_place.map(|v| v.min(partition_size));
        let defrag = defragmented.map(|v| v.min(partition_size));
        if let Some(v) = in_place {
            self.in_place.insert(idx, v);
        }
        if let Some(v) = defrag {
            self.defrag.insert(idx, v);
        }
        if let Some(pct) = fragmentation_percent {
            self.fragmentation.insert(idx, pct);
        }
        // Space saved decides, not file fragmentation: unfragmented files can still leave holes.
        if !self.compact_default_applied.contains(&idx)
            && self.compact_available(idx, type_byte, type_str)
        {
            if let (Some(ip), Some(df)) = (in_place, defrag) {
                if ip > 0 && df < ip {
                    let savings_pct = 100.0 * (ip - df) as f32 / ip as f32;
                    if savings_pct >= COMPACT_AUTOCHECK_SAVINGS_PCT {
                        self.compact.insert(idx, true);
                    }
                }
            }
        }
        self.compact_default_applied.insert(idx);
    }

    /// The log line for a computed minimum: " (space compacted via clone)" and the fragmentation.
    pub fn result_suffix(&self, idx: usize, fragmentation_percent: Option<f32>) -> String {
        let compact = if self.compact_enabled(idx) {
            " (space compacted via clone)"
        } else {
            ""
        };
        let frag = fragmentation_percent
            .map(|p| format!(", fragmentation {:.1}%", p))
            .unwrap_or_default();
        format!("{compact}{frag}")
    }
}

/// The Compact column shows a toggle only for a layout-preserving filesystem or a live Compact.
pub fn shows_compact(mins: &MinSizes, part: &PartitionInfo) -> bool {
    let ts = part.partition_type_string.as_deref();
    crate::fs::is_layout_preserving_fs(part.gate_type_byte(), ts)
        || mins.compact_available(part.index, part.gate_type_byte(), ts)
}

/// What the user chose on the options form.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupOptions {
    pub compression: CompressionType,
    pub checksum: ChecksumType,
    pub sector_by_sector: bool,
    pub resize: bool,
    pub keep_swap: bool,
    pub split: bool,
    pub split_mib: u32,
    pub chd: Option<ChdOptions>,
}

impl Default for BackupOptions {
    fn default() -> Self {
        BackupOptions {
            compression: CompressionType::Zstd,
            checksum: ChecksumType::Sha256,
            sector_by_sector: false,
            resize: true,
            keep_swap: false,
            split: true,
            split_mib: 4000,
            chd: None,
        }
    }
}

impl BackupOptions {
    /// Resizing preserves nothing a sector-by-sector copy keeps, so it is off while that is on.
    pub fn resize_effective(&self) -> bool {
        self.resize && !self.sector_by_sector
    }

    /// The split size the engine gets: VHD and CHD are always one file.
    pub fn split_size(&self) -> Option<u32> {
        let single_file = matches!(
            self.compression,
            CompressionType::Vhd | CompressionType::Chd | CompressionType::Dvd
        );
        (self.split && !single_file).then_some(self.split_mib)
    }

    /// The split option is offered at all (chdman and MAME do not read split CHDs).
    pub fn split_shown(&self) -> bool {
        !matches!(self.compression, CompressionType::Chd | CompressionType::Dvd)
    }

    /// The split option can be changed (a VHD is always one file).
    pub fn split_editable(&self) -> bool {
        self.split_shown() && self.compression != CompressionType::Vhd
    }

    /// A CHD resize-to-minimum backup needs every selected partition's minimum first.
    pub fn needs_minimums(&self) -> bool {
        self.compression == CompressionType::Chd && self.resize_effective()
    }
}

/// The per-partition sizing a backup config carries.
#[derive(Debug, Clone, PartialEq)]
pub struct Sizing {
    pub size_policy: Option<SizePolicy>,
    pub targets: Option<Vec<(usize, u64)>>,
    pub defrag_set: HashSet<usize>,
    pub precomputed: Option<Vec<(usize, u64)>>,
}

/// Single-file-CHD sizes, the Compact opt-in set and the minimums the walks already found.
pub fn sizing(parts: &[PartitionInfo], mins: &MinSizes, opts: &BackupOptions) -> Sizing {
    let volumes = || parts.iter().filter(|p| !p.is_extended_container);
    let has_clone = |p: &PartitionInfo| {
        crate::fs::has_defragmenting_writer(p.gate_type_byte(), p.partition_type_string.as_deref())
    };
    let effective = |p: &PartitionInfo| {
        mins.effective(
            p.index,
            p.gate_type_byte(),
            p.partition_type_string.as_deref(),
        )
    };
    // A Compact partition is sized by its clone, so it gets no CHD target of its own.
    let (size_policy, targets) = if opts.needs_minimums() {
        let mut targets: Vec<(usize, u64)> = Vec::new();
        let mut used_min_plus_20 = false;
        for part in volumes() {
            if mins.compact_enabled(part.index) && has_clone(part) {
                continue;
            }
            let target = match effective(part) {
                Some(min) if min < part.size_bytes => {
                    used_min_plus_20 = true;
                    SizeMode::MinPlus20.effective_size(part.size_bytes, min, 0)
                }
                _ => part.size_bytes,
            };
            targets.push((part.index, target));
        }
        let policy = if used_min_plus_20 {
            SizePolicy::MinPlus20
        } else {
            SizePolicy::Original
        };
        (Some(policy), (!targets.is_empty()).then_some(targets))
    } else {
        (None, None)
    };
    let defrag_set: HashSet<usize> = volumes()
        .filter(|p| mins.compact_enabled(p.index) && has_clone(p))
        .map(|p| p.index)
        .collect();
    let precomputed: Vec<(usize, u64)> = volumes()
        .filter_map(|p| effective(p).map(|sz| (p.index, sz)))
        .collect();
    Sizing {
        size_policy,
        targets,
        defrag_set,
        precomputed: (!precomputed.is_empty()).then_some(precomputed),
    }
}

/// `None` when every volume is selected (the engine then backs up all of them).
pub fn partition_filter(parts: &[PartitionInfo], selected: &HashSet<usize>) -> Option<Vec<usize>> {
    let volumes = parts.iter().filter(|p| !p.is_extended_container).count();
    if selected.len() < volumes {
        let mut list: Vec<usize> = selected.iter().copied().collect();
        list.sort_unstable();
        Some(list)
    } else {
        None
    }
}

/// The engine's config for a backup of `source` into `destination/name`.
pub fn build_config(
    source: PathBuf,
    destination: PathBuf,
    name: String,
    opts: &BackupOptions,
    parts: &[PartitionInfo],
    selected: &HashSet<usize>,
    mins: &MinSizes,
) -> BackupConfig {
    let sizing = sizing(parts, mins, opts);
    BackupConfig {
        source_path: source,
        destination_dir: destination,
        backup_name: name,
        compression: opts.compression,
        checksum: opts.checksum,
        split_size_mib: opts.split_size(),
        sector_by_sector: opts.sector_by_sector,
        partition_filter: partition_filter(parts, selected),
        chd_options: if matches!(
            opts.compression,
            CompressionType::Chd | CompressionType::Dvd
        ) {
            opts.chd.clone()
        } else {
            None
        },
        size_policy: sizing.size_policy,
        partition_target_sizes: sizing.targets,
        // Turns on the HFS+ / PFS3 clone, narrowed by `defrag_partition_indices`.
        shrink_to_minimum: opts.resize_effective(),
        precomputed_minimum_sizes: sizing.precomputed,
        defrag_partition_indices: Some(sizing.defrag_set),
        defrag_fat: false,
        keep_swap: opts.keep_swap,
    }
}

/// The selected volumes whose minimum still needs a walk before a resize-to-minimum CHD backup.
pub fn deferred_selected(
    parts: &[PartitionInfo],
    selected: &HashSet<usize>,
    mins: &MinSizes,
    deferred: &HashMap<usize, &'static str>,
) -> Vec<usize> {
    parts
        .iter()
        .filter(|p| {
            !p.is_extended_container
                && selected.contains(&p.index)
                && !mins.in_place.contains_key(&p.index)
                && deferred.contains_key(&p.index)
        })
        .map(|p| p.index)
        .collect()
}

/// For a remote drive: walks to defer behind Calc min (expensive) and walks to run now (cheap).
pub fn remote_min_plan(
    parts: &[PartitionInfo],
    superfloppy: bool,
) -> (Vec<(usize, &'static str)>, Vec<usize>) {
    let mut defer = Vec::new();
    let mut run = Vec::new();
    for part in parts.iter().filter(|p| !p.is_extended_container) {
        let type_byte = part.gate_type_byte();
        let type_str = part.partition_type_string.as_deref();
        let name = crate::fs::fs_name_for(type_byte, type_str);
        // A superfloppy's one volume carries its filesystem under a type byte of 0.
        if !superfloppy && name == "unknown" {
            continue;
        }
        if crate::fs::is_expensive_minimum(type_byte, type_str) {
            defer.push((part.index, name));
        } else {
            run.push(part.index);
        }
    }
    (defer, run)
}

/// A device node rather than an image file (the scan elevates and the preview skips it).
pub fn is_device_path(path: &std::path::Path) -> bool {
    let s = path.to_string_lossy();
    s.starts_with("/dev/") || s.starts_with("\\\\.\\")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(index: usize, size: u64, type_byte: u8) -> PartitionInfo {
        PartitionInfo {
            index,
            type_name: "FAT32".into(),
            partition_type_byte: type_byte,
            start_lba: 2048,
            start_byte: None,
            size_bytes: size,
            bootable: false,
            is_logical: false,
            is_extended_container: false,
            partition_type_string: None,
            hfs_block_size: None,
            rdb_part_block: None,
            drv_name: None,
        }
    }

    #[test]
    fn split_follows_the_format() {
        let mut o = BackupOptions::default();
        assert_eq!(o.split_size(), Some(4000));
        o.compression = CompressionType::Vhd;
        assert_eq!(o.split_size(), None);
        assert!(o.split_shown() && !o.split_editable());
        o.compression = CompressionType::Chd;
        assert_eq!(o.split_size(), None);
        assert!(!o.split_shown());
        o.compression = CompressionType::None;
        o.split = false;
        assert_eq!(o.split_size(), None);
    }

    #[test]
    fn sector_by_sector_turns_resize_off() {
        let o = BackupOptions {
            sector_by_sector: true,
            ..Default::default()
        };
        assert!(!o.resize_effective());
        let cfg = build_config(
            "a".into(),
            "b".into(),
            "c".into(),
            &o,
            &[],
            &HashSet::new(),
            &MinSizes::default(),
        );
        assert!(!cfg.shrink_to_minimum);
    }

    #[test]
    fn chd_resize_targets_min_plus_20() {
        let parts = vec![part(0, 100 << 20, 0x0C), part(1, 50 << 20, 0x0C)];
        let mut mins = MinSizes::default();
        mins.record(0, Some(10 << 20), None, None, 100 << 20, 0x0C, None);
        let opts = BackupOptions {
            compression: CompressionType::Chd,
            ..Default::default()
        };
        let s = sizing(&parts, &mins, &opts);
        assert_eq!(s.size_policy, Some(SizePolicy::MinPlus20));
        let targets = s.targets.unwrap();
        assert_eq!(targets[1], (1, 50 << 20));
        assert!(targets[0].1 >= 10 << 20 && targets[0].1 < 100 << 20);
        assert_eq!(s.precomputed, Some(vec![(0, 10 << 20)]));
    }

    #[test]
    fn filter_is_none_when_everything_is_selected() {
        let parts = vec![part(0, 1, 0x0C), part(1, 1, 0x0C)];
        let all: HashSet<usize> = [0, 1].into_iter().collect();
        assert_eq!(partition_filter(&parts, &all), None);
        let one: HashSet<usize> = [1].into_iter().collect();
        assert_eq!(partition_filter(&parts, &one), Some(vec![1]));
    }

    #[test]
    fn compact_auto_checks_on_savings_once() {
        let mut mins = MinSizes::default();
        mins.record(0, Some(100), Some(50), Some(3.0), 1000, 0x0C, None);
        assert!(mins.compact_enabled(0));
        assert_eq!(mins.effective(0, 0x0C, None), Some(50));
        mins.set_compact(0, false);
        mins.record(0, Some(100), Some(50), None, 1000, 0x0C, None);
        assert!(!mins.compact_enabled(0));
        assert_eq!(mins.effective(0, 0x0C, None), Some(100));
    }
}
