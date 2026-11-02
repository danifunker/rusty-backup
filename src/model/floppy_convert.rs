//! Convert Floppy Container: one XDF / HDM / DIM / D88 image to another of the four.
//!
//! The rules the Inspect tab's dialog used: what counts as a convertible source, the target it
//! defaults to, and the summary of a finished conversion. The conversion itself is
//! `rbformats::containers::convert_floppy_container`, as for `rb-cli floppy convert`.

use std::path::Path;

use crate::rbformats::containers::{self, ContainerKind};

/// One of the four floppy formats the dialog offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloppyTarget {
    Xdf,
    Hdm,
    Dim,
    D88,
}

impl FloppyTarget {
    pub const ALL: [FloppyTarget; 4] = [
        FloppyTarget::Xdf,
        FloppyTarget::Hdm,
        FloppyTarget::Dim,
        FloppyTarget::D88,
    ];

    pub const fn extension(self) -> &'static str {
        match self {
            FloppyTarget::Xdf => "xdf",
            FloppyTarget::Hdm => "hdm",
            FloppyTarget::Dim => "dim",
            FloppyTarget::D88 => "d88",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            FloppyTarget::Xdf => "X68000 XDF (.xdf)",
            FloppyTarget::Hdm => "PC-98 HDM (.hdm)",
            FloppyTarget::Dim => "DiskExplorer DIM (.dim)",
            FloppyTarget::D88 => "Sharp D88 (.d88)",
        }
    }

    const fn container(self) -> ContainerKind {
        match self {
            FloppyTarget::Xdf => ContainerKind::Xdf,
            FloppyTarget::Hdm => ContainerKind::Hdm,
            FloppyTarget::Dim => ContainerKind::Dim,
            FloppyTarget::D88 => ContainerKind::D88,
        }
    }

    fn from_container(c: ContainerKind) -> Option<Self> {
        match c {
            ContainerKind::Xdf => Some(FloppyTarget::Xdf),
            ContainerKind::Hdm => Some(FloppyTarget::Hdm),
            ContainerKind::Dim => Some(FloppyTarget::Dim),
            ContainerKind::D88 => Some(FloppyTarget::D88),
            _ => None,
        }
    }
}

/// A source the dialog accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloppySource {
    /// "Detected: ..." in the dialog.
    pub kind_name: &'static str,
    pub format: FloppyTarget,
    /// The target the dialog starts on: D88 for anything else, XDF for a D88.
    pub suggested: FloppyTarget,
}

/// Sniff `path`: a floppy container no larger than `FLOPPY_CONTAINER_MAX`, or why not.
pub fn probe_source(path: &Path) -> Result<FloppySource, String> {
    let head =
        crate::compat::read_head(path, 256).map_err(|e| format!("Cannot read source: {e}"))?;
    let kind = containers::detect_container_kind(&head, Some(path));
    let too_big =
        std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) > containers::FLOPPY_CONTAINER_MAX;
    match FloppyTarget::from_container(kind) {
        Some(format) if containers::is_floppy_container(kind) && !too_big => Ok(FloppySource {
            kind_name: kind.display_name(),
            format,
            suggested: if format == FloppyTarget::D88 {
                FloppyTarget::Xdf
            } else {
                FloppyTarget::D88
            },
        }),
        _ => Err(format!(
            "{} is not a recognised floppy container (detected: {})",
            path.display(),
            kind.display_name()
        )),
    }
}

/// A finished conversion: the dialog's summary and its log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloppyConverted {
    pub summary: String,
    pub line: String,
}

/// Convert `source` into `output` as `target`.
pub fn convert(
    source: &Path,
    output: &Path,
    target: FloppyTarget,
) -> Result<FloppyConverted, String> {
    let report = containers::convert_floppy_container(source, output, target.container())
        .map_err(|e| format!("{e:#}"))?;
    Ok(FloppyConverted {
        summary: format!(
            "Converted to {} ({}, {} bytes)",
            target.label(),
            report.media.display_label(),
            report.bytes_written
        ),
        line: format!(
            "Wrote {} bytes to {}",
            report.bytes_written,
            output.display()
        ),
    })
}
