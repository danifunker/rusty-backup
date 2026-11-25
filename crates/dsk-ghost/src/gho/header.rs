//! The container header: compression, image type, description, and the info summary.

use super::*;

/// Length of the fixed container header (excluding the optional password
/// verifier and description string).
pub const GHO_HEADER_PREFIX_LEN: usize = 12;

/// Length of the password verifier appended after the header when
/// `password_flag == 1`.
pub const GHO_PASSWORD_VERIFIER_LEN: usize = 16;

/// Container magic at file offset 0.
pub const GHO_MAGIC: [u8; 2] = [0xFE, 0xEF];

/// Offset where Ghost stores the Pascal-style description string
/// (`length_byte + ASCII`). Present in some 7.5 backups, absent in 11.5
/// ones. Capped to 1 sector so we never read past the in-file metadata
/// region.
pub const GHO_DESCRIPTION_OFFSET: u64 = 0xFF;

/// Maximum description string length we'll accept. Ghost's UI capped this
/// well below the sector boundary in practice (the longest string in our
/// corpus is "PartitionBackup no compression", 30 bytes).
pub const GHO_DESCRIPTION_MAX: u8 = 254;

/// Compression byte at header offset 0x03.
///
/// Cross-referenced against the inner record stream's per-block compression
/// indicator once 5.5b lands — the container byte is the "default" for the
/// whole image, but block-level overrides exist in the wild.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GhoCompression {
    /// `0x00` — no compression.
    None,
    /// `0x02` — Fast-LZ (custom LZ77 variant; spec in `nyarime/gho`).
    Fast,
    /// `0x03` — zlib (deflate).
    High,
    /// Anything else — Ghost shipped a handful of intermediate levels
    /// (`Z1`, `Z4`-`Z9`) that map to the same algorithms internally; we
    /// store the raw byte so 5.5b can decide whether to treat it as
    /// Fast or High.
    Other(u8),
}

impl GhoCompression {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0x00 => Self::None,
            0x02 => Self::Fast,
            0x03 => Self::High,
            other => Self::Other(other),
        }
    }

    /// Raw byte value as it appears in the header.
    pub fn as_byte(self) -> u8 {
        match self {
            Self::None => 0x00,
            Self::Fast => 0x02,
            Self::High => 0x03,
            Self::Other(b) => b,
        }
    }
}

/// Image-type byte at header offset 0x0A.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GhoImageType {
    /// `0x00` — file-aware ("truncated full backup"). Ghost walks the
    /// filesystem and stores only used clusters. The inner record stream
    /// targets this mode; it's session 5.5b's focus.
    FileAware,
    /// `0x01` — SECTOR (raw sector-by-sector). Deferred to session 5.6b.
    Sector,
    /// Anything else — surface raw so a future Ghost version doesn't
    /// silently misparse.
    Other(u8),
}

impl GhoImageType {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0x00 => Self::FileAware,
            0x01 => Self::Sector,
            other => Self::Other(other),
        }
    }

    pub fn as_byte(self) -> u8 {
        match self {
            Self::FileAware => 0x00,
            Self::Sector => 0x01,
            Self::Other(b) => b,
        }
    }
}

/// Parsed Norton Ghost container header.
#[derive(Debug, Clone)]
pub struct GhoContainerHeader {
    /// Container version byte at offset 0x02. Always `0x01` across every
    /// Ghost 7.5 + 11.5 fixture we've inspected — we surface it so we can
    /// reject unknown versions cleanly when they appear.
    pub container_version: u8,
    /// Compression scheme (offset 0x03).
    pub compression: GhoCompression,
    /// 32-bit serial / CRC (offset 0x04). Semantics not yet decoded; we
    /// expose it for diagnostics.
    pub serial: u32,
    /// Constant `[0x01, 0x01]` in our corpus (offset 0x08). Stored raw so
    /// future format variants don't silently misparse.
    pub flags: [u8; 2],
    /// Image type (offset 0x0A).
    pub image_type: GhoImageType,
    /// `true` if the backup is password-protected (offset 0x0B).
    pub password_protected: bool,
    /// 16-byte verifier following the prefix when `password_protected`,
    /// `None` otherwise. We never attempt to decrypt — the verifier is
    /// surfaced for diagnostics only.
    pub password_verifier: Option<[u8; GHO_PASSWORD_VERIFIER_LEN]>,
    /// Optional Pascal-style description string at offset 0xFF
    /// (length-prefixed ASCII). `None` if length byte is zero or the
    /// string would overflow the first sector.
    pub description: Option<String>,
}

impl GhoContainerHeader {
    /// Parse a container header from `reader`. Reader is left at an
    /// undefined position; callers should re-seek before continuing.
    pub fn parse<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        reader
            .seek(SeekFrom::Start(0))
            .context("seeking to start of GHO container")?;

        let mut prefix = [0u8; GHO_HEADER_PREFIX_LEN];
        reader
            .read_exact(&mut prefix)
            .context("reading GHO header prefix")?;

        if prefix[0..2] != GHO_MAGIC {
            bail!(
                "not a Norton Ghost container: magic at offset 0 is {:02x} {:02x}, expected FE EF",
                prefix[0],
                prefix[1]
            );
        }

        let container_version = prefix[2];
        let compression = GhoCompression::from_byte(prefix[3]);
        let serial = u32::from_le_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]);
        let flags = [prefix[8], prefix[9]];
        let image_type = GhoImageType::from_byte(prefix[10]);
        let password_protected = match prefix[11] {
            0x00 => false,
            0x01 => true,
            other => {
                bail!(
                    "GHO header password_flag byte at offset 0x0B is {:#04x}, expected 0x00 or 0x01",
                    other
                );
            }
        };

        let password_verifier = if password_protected {
            let mut verifier = [0u8; GHO_PASSWORD_VERIFIER_LEN];
            reader
                .read_exact(&mut verifier)
                .context("reading GHO password verifier")?;
            Some(verifier)
        } else {
            None
        };

        let description = read_description(reader).context("reading GHO description string")?;

        Ok(Self {
            container_version,
            compression,
            serial,
            flags,
            image_type,
            password_protected,
            password_verifier,
            description,
        })
    }
}

pub(super) fn read_description<R: Read + Seek>(reader: &mut R) -> Result<Option<String>> {
    reader.seek(SeekFrom::Start(GHO_DESCRIPTION_OFFSET))?;
    // The description is a NUL-terminated ASCII string starting at
    // offset 0xFF, bounded by the end of the first sector. (Ghost's
    // UI caps descriptions well before the sector boundary.)
    let max_len = 512u64.saturating_sub(GHO_DESCRIPTION_OFFSET) as usize;
    let mut buf = vec![0u8; max_len];
    let n = match read_fully_or_eof(reader, &mut buf) {
        Ok(n) => n,
        Err(_) => return Ok(None),
    };
    buf.truncate(n);
    // Trim at NUL terminator.
    if let Some(nul) = buf.iter().position(|&b| b == 0) {
        buf.truncate(nul);
    }
    // Strip trailing whitespace.
    while buf.last().map(|&b| b == b' ').unwrap_or(false) {
        buf.pop();
    }
    if buf.is_empty() {
        return Ok(None);
    }
    match std::str::from_utf8(&buf) {
        Ok(s) => Ok(Some(s.to_string())),
        Err(_) => Ok(Some(String::from_utf8_lossy(&buf).into_owned())),
    }
}

/// Format a human-readable summary of a GHO container's metadata.
/// Similar to `format_chd_info` for CHD files.
pub fn format_gho_info(path: &Path) -> Result<String> {
    let span_set = discover_gho_span_set(path)?;
    let mut f = File::open(&span_set[0])?;
    let header = GhoContainerHeader::parse(&mut f)?;

    let mut lines = Vec::new();
    lines.push("Norton Ghost Image".to_string());
    lines.push(String::new());

    if let Some(desc) = &header.description {
        lines.push(format!("Description:  {}", desc));
    }

    let comp_str = match header.compression {
        GhoCompression::None => "None",
        GhoCompression::Fast => "Fast (LZ)",
        GhoCompression::High => "High (zlib)",
        GhoCompression::Other(b) => {
            lines.push(format!("Compression:  Unknown (0x{:02x})", b));
            ""
        }
    };
    if !comp_str.is_empty() {
        lines.push(format!("Compression:  {}", comp_str));
    }

    let type_str = match header.image_type {
        GhoImageType::FileAware => "File-aware (truncated)",
        GhoImageType::Sector => "Sector-by-sector",
        GhoImageType::Other(b) => {
            lines.push(format!("Image type:   Unknown (0x{:02x})", b));
            ""
        }
    };
    if !type_str.is_empty() {
        lines.push(format!("Image type:   {}", type_str));
    }

    if header.password_protected {
        lines.push("Password:     Yes (encrypted)".to_string());
    }

    if span_set.len() > 1 {
        lines.push(format!("Span files:   {} files", span_set.len()));
    }

    let total_bytes: u64 = span_set
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
        .sum();
    lines.push(format!(
        "Archive size: {:.1} MB",
        total_bytes as f64 / (1024.0 * 1024.0)
    ));

    Ok(lines.join("\n"))
}

/// Materialize a Norton Ghost backup into a raw disk image temp file.
///
/// SECTOR and file-aware backups, single- or multi-partition, decode into a
/// fresh tempdir; an unknown container version or a missing password is an error.
pub fn materialize_gho_to_temp(path: &Path) -> Result<GhoMaterialized> {
    materialize_gho_to_temp_with_password(path, None)
}

/// Like [`materialize_gho_to_temp`] but accepts an optional password for
/// encrypted images. Password-protected GHOs are decoded through the streaming
/// [`GhoReader`], which derives + verifies the seed and applies the correct
/// decryption framing (per record body for file-aware, 32 KiB chunks for
/// uncompressed SECTOR), then the decoded disk is copied to a tempfile.
pub fn materialize_gho_to_temp_with_password(
    path: &Path,
    password: Option<&[u8]>,
) -> Result<GhoMaterialized> {
    let span_set = discover_gho_span_set(path)
        .with_context(|| format!("discovering span set for {}", path.display()))?;
    if span_set.len() > 1 {
        log::info!(
            "GHO span set: {} files discovered for {}",
            span_set.len(),
            path.display()
        );
        for (i, p) in span_set.iter().enumerate() {
            log::debug!("  span[{i}] -> {}", p.display());
        }
    }

    let primary = &span_set[0];
    let mut primary_file =
        File::open(primary).with_context(|| format!("opening GHO {}", primary.display()))?;
    let header = GhoContainerHeader::parse(&mut primary_file)
        .with_context(|| format!("parsing GHO container header from {}", primary.display()))?;
    drop(primary_file);

    if header.container_version != 0x01 {
        return Err(anyhow!(
            "GHO {} has unknown container version {:#04x}; this build supports 0x01 (Ghost 7.5 + 11.5)",
            path.display(),
            header.container_version
        ));
    }

    if header.password_protected {
        // Route encrypted images through the streaming GhoReader, which
        // derives + verifies the password seed and applies the right
        // decryption framing for both file-aware and SECTOR layouts, then
        // copy the decoded disk into a tempfile. (A missing/incorrect
        // password surfaces as a precise error from `open_with_password`.)
        let mut reader = GhoReader::open_with_password(path, password)
            .with_context(|| format!("opening password-protected GHO {}", path.display()))?;
        let logical = reader.logical_size();
        let partition_count = match &reader.mode {
            GhoReaderMode::FileAware { partitions, .. } => partitions.len(),
            _ => 1,
        };
        let guard =
            tempfile::tempdir().context("creating tempdir for encrypted GHO materialization")?;
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("gho");
        let out_path = guard.path().join(format!("{}.img", stem));
        let mut out =
            File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?;
        let bytes_written = std::io::copy(&mut reader, &mut out)
            .with_context(|| format!("materializing encrypted {} via GhoReader", path.display()))?;
        out.sync_all().ok();
        log::info!(
            "Materialized encrypted GHO {} -> {} ({} bytes via GhoReader)",
            path.display(),
            out_path.display(),
            bytes_written
        );
        return Ok(GhoMaterialized {
            temp_path: out_path,
            logical_size: logical,
            guard,
            partition_count,
        });
    }

    if let GhoCompression::Other(b) = header.compression {
        return Err(anyhow!(
            "GHO {} has unknown compression byte {:#04x} at offset 0x03; expected 0x00 (none), \
             0x02 (fast/LZ), or 0x03 (high/zlib)",
            path.display(),
            b
        ));
    }

    match header.image_type {
        GhoImageType::Sector => {
            let mut reader = SpanReader::open(&span_set)
                .with_context(|| format!("opening span set for {}", path.display()))?;
            decode_sector_mode_to_temp(&mut reader, path, &header)
        }
        GhoImageType::FileAware => {
            let mut reader = GhoReader::open(path)
                .with_context(|| format!("opening file-aware GHO {}", path.display()))?;
            let logical = reader.logical_size();
            let partition_count = match &reader.mode {
                GhoReaderMode::FileAware { partitions, .. } => partitions.len(),
                _ => 1,
            };
            let guard = tempfile::tempdir()
                .context("creating tempdir for GHO file-aware materialization")?;
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("gho");
            let out_path = guard.path().join(format!("{}.img", stem));
            let mut out = File::create(&out_path)
                .with_context(|| format!("creating {}", out_path.display()))?;
            let bytes_written = std::io::copy(&mut reader, &mut out).with_context(|| {
                format!("materializing file-aware {} via GhoReader", path.display())
            })?;
            out.sync_all().ok();
            log::info!(
                "Materialized GHO file-aware {} -> {} ({} bytes via streaming GhoReader, {} partitions)",
                path.display(),
                out_path.display(),
                bytes_written,
                partition_count
            );
            Ok(GhoMaterialized {
                temp_path: out_path,
                logical_size: logical,
                guard,
                partition_count,
            })
        }
        GhoImageType::Other(b) => Err(anyhow!(
            "GHO {} has unknown image_type byte {:#04x} at offset 0x0A; expected 0x00 \
             (file-aware) or 0x01 (SECTOR)",
            path.display(),
            b
        )),
    }
}

// SECTOR-mode decoder overview. Data starts at the sector after the last
// FE EF sub-header (sector 6 on Ghost 7.5, sector 11 on Ghost 11.5 in
// the corpus); from there it is either:
//   - compression == None → raw sectors, copied verbatim to `out`
//   - compression == Fast/High → stream of `[u16 stored_len][block_data]`
//     chunks where `stored_len` includes itself; each chunk decompresses
//     to one 32 KiB raw block.
// The decoder fn itself lives further down (`decode_sector_mode_to_temp`);
// the span-set discovery + multi-file reader machinery sits between.
