use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};

use super::ChecksumType;

const READ_BUF_SIZE: usize = 1024 * 1024; // 1 MB

/// Running-hash state shared between a [`ChecksumReader`] and the code
/// that finalises the digest after the reader has been consumed. Wrap
/// in `Arc<Mutex<>>` so the reader (often owned by a downstream
/// consumer like a stream builder) can update the hash while the
/// orchestrator retains a handle to extract the final digest at the
/// end of the operation.
pub enum RunningHasher {
    Sha256(sha2::Sha256),
    Crc32(crc32fast::Hasher),
}

impl RunningHasher {
    pub fn new(checksum_type: ChecksumType) -> Self {
        match checksum_type {
            ChecksumType::Sha256 => {
                use sha2::Digest;
                RunningHasher::Sha256(sha2::Sha256::new())
            }
            ChecksumType::Crc32 => RunningHasher::Crc32(crc32fast::Hasher::new()),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            RunningHasher::Sha256(h) => {
                use sha2::Digest;
                h.update(data);
            }
            RunningHasher::Crc32(h) => h.update(data),
        }
    }

    /// Consume the hasher and return the hex-encoded digest. After
    /// calling this the hasher must be replaced before further use;
    /// the caller usually drops it.
    pub fn finalize_hex(self) -> String {
        match self {
            RunningHasher::Sha256(h) => {
                use sha2::Digest;
                h.finalize().iter().map(|b| format!("{:02x}", b)).collect()
            }
            RunningHasher::Crc32(h) => format!("{:08x}", h.finalize()),
        }
    }
}

/// `Read` adapter that updates a shared [`RunningHasher`] on every
/// successful read. Used to "tee" bytes flowing through a compression
/// pipeline so the orchestrator can record the input bytes' checksum
/// without a second read pass after the encoder finishes.
///
/// The hasher is `Arc<Mutex<>>` because the reader is typically moved
/// into a downstream consumer (e.g. a stream builder), so the
/// orchestrator can't recover ownership directly — it shares the
/// hasher with the reader and finalises it after the consumer
/// returns.
pub struct ChecksumReader<R: Read> {
    inner: R,
    hasher: Arc<Mutex<Option<RunningHasher>>>,
}

impl<R: Read> ChecksumReader<R> {
    pub fn new(inner: R, hasher: Arc<Mutex<Option<RunningHasher>>>) -> Self {
        Self { inner, hasher }
    }
}

impl<R: Read> Read for ChecksumReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            if let Ok(mut guard) = self.hasher.lock() {
                if let Some(h) = guard.as_mut() {
                    h.update(&buf[..n]);
                }
            }
        }
        Ok(n)
    }
}

/// Finalise a shared hasher and return its hex digest. The hasher slot
/// is emptied; calling this twice on the same handle returns an empty
/// string the second time (so missing-tee bugs show up loudly in
/// tests). Used after the consumer that owned the corresponding
/// [`ChecksumReader`] / [`ChecksumWriter`] has been dropped or returned.
pub fn finalize_shared_hasher(handle: &Arc<Mutex<Option<RunningHasher>>>) -> String {
    let Ok(mut guard) = handle.lock() else {
        return String::new();
    };
    match guard.take() {
        Some(h) => h.finalize_hex(),
        None => String::new(),
    }
}

/// `Write` adapter that updates a shared [`RunningHasher`] on every
/// successful write. Mirror of [`ChecksumReader`] for the output side
/// of a compression pipeline — used to hash the bytes flowing into
/// the destination `.zst` / `.raw` / `.vhd` file as they're written,
/// avoiding a second read pass to compute the file's checksum after
/// the encode finishes.
pub struct ChecksumWriter<W: Write> {
    inner: W,
    hasher: Arc<Mutex<Option<RunningHasher>>>,
}

impl<W: Write> ChecksumWriter<W> {
    pub fn new(inner: W, hasher: Arc<Mutex<Option<RunningHasher>>>) -> Self {
        Self { inner, hasher }
    }
}

impl<W: Write> Write for ChecksumWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        if n > 0 {
            if let Ok(mut guard) = self.hasher.lock() {
                if let Some(h) = guard.as_mut() {
                    h.update(&buf[..n]);
                }
            }
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Compute a checksum over the file at `path`.
/// Returns the hex-encoded checksum string.
pub fn compute_checksum(path: &Path, checksum_type: ChecksumType) -> Result<String> {
    let file = File::open(path)
        .with_context(|| format!("failed to open {} for checksum", path.display()))?;
    let mut reader = BufReader::new(file);
    hash_reader_to_eof(&mut reader, checksum_type)
}

/// Compute a checksum by reading `reader` to EOF. Used for whole-file
/// hashing where the size is determined by the stream itself.
pub fn hash_reader_to_eof(reader: &mut dyn Read, checksum_type: ChecksumType) -> Result<String> {
    let mut buf = vec![0u8; READ_BUF_SIZE];
    match checksum_type {
        ChecksumType::Sha256 => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            loop {
                let n = reader.read(&mut buf).context("checksum read error")?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(hasher
                .finalize()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect())
        }
        ChecksumType::Crc32 => {
            let mut hasher = crc32fast::Hasher::new();
            loop {
                let n = reader.read(&mut buf).context("checksum read error")?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(format!("{:08x}", hasher.finalize()))
        }
    }
}

/// Compute a checksum over exactly `length` bytes read from `reader`,
/// optionally invoking `progress_cb` with the cumulative byte count after
/// each chunk. Used by single-file-CHD post-write hashing where each
/// partition is a sub-range of the container, not a whole file.
///
/// `progress_cb` receives the *delta* since the last invocation as the
/// `n` argument so the caller can blend per-partition progress into a
/// running total without bookkeeping.
pub fn hash_reader_range(
    reader: &mut dyn Read,
    length: u64,
    checksum_type: ChecksumType,
    progress_cb: &mut dyn FnMut(u64),
) -> Result<String> {
    let mut buf = vec![0u8; READ_BUF_SIZE];
    match checksum_type {
        ChecksumType::Sha256 => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            let mut remaining = length;
            while remaining > 0 {
                let want = (remaining as usize).min(buf.len());
                let n = reader
                    .read(&mut buf[..want])
                    .context("checksum read error")?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                remaining -= n as u64;
                progress_cb(n as u64);
            }
            Ok(hasher
                .finalize()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect())
        }
        ChecksumType::Crc32 => {
            let mut hasher = crc32fast::Hasher::new();
            let mut remaining = length;
            while remaining > 0 {
                let want = (remaining as usize).min(buf.len());
                let n = reader
                    .read(&mut buf[..want])
                    .context("checksum read error")?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                remaining -= n as u64;
                progress_cb(n as u64);
            }
            Ok(format!("{:08x}", hasher.finalize()))
        }
    }
}

/// Write a checksum sidecar file next to the data file.
///
/// For example, given `partition-0.zst` and SHA256, writes `partition-0.zst.sha256`
/// in the format: `{hash}  {filename}` (compatible with sha256sum/crc32).
pub fn write_checksum_file(
    checksum: &str,
    file_path: &Path,
    checksum_type: ChecksumType,
) -> Result<()> {
    let extension = match checksum_type {
        ChecksumType::Sha256 => "sha256",
        ChecksumType::Crc32 => "crc32",
    };
    let sidecar_path = file_path.with_extension(format!(
        "{}.{extension}",
        file_path.extension().unwrap_or_default().to_string_lossy()
    ));

    let file_name = file_path.file_name().unwrap_or_default().to_string_lossy();

    let mut f = File::create(&sidecar_path)
        .with_context(|| format!("failed to create {}", sidecar_path.display()))?;
    writeln!(f, "{checksum}  {file_name}")
        .with_context(|| format!("failed to write {}", sidecar_path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_sha256_known_value() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bin");
        // SHA-256 of empty file is well-known
        fs::write(&path, b"").unwrap();
        let checksum = compute_checksum(&path, ChecksumType::Sha256).unwrap();
        assert_eq!(
            checksum,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sha256_hello() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("hello.bin");
        fs::write(&path, b"hello").unwrap();
        let checksum = compute_checksum(&path, ChecksumType::Sha256).unwrap();
        assert_eq!(
            checksum,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_crc32_known_value() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bin");
        fs::write(&path, b"").unwrap();
        let checksum = compute_checksum(&path, ChecksumType::Crc32).unwrap();
        assert_eq!(checksum, "00000000");
    }

    #[test]
    fn test_crc32_hello() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("hello.bin");
        fs::write(&path, b"hello").unwrap();
        let checksum = compute_checksum(&path, ChecksumType::Crc32).unwrap();
        // CRC32 of "hello"
        assert_eq!(checksum, "3610a686");
    }

    #[test]
    fn test_write_checksum_file_sha256() {
        let tmp = TempDir::new().unwrap();
        let data_path = tmp.path().join("partition-0.zst");
        fs::write(&data_path, b"test data").unwrap();

        write_checksum_file("abcdef1234567890", &data_path, ChecksumType::Sha256).unwrap();

        let sidecar = tmp.path().join("partition-0.zst.sha256");
        assert!(sidecar.exists());
        let content = fs::read_to_string(&sidecar).unwrap();
        assert_eq!(content, "abcdef1234567890  partition-0.zst\n");
    }

    #[test]
    fn test_write_checksum_file_crc32() {
        let tmp = TempDir::new().unwrap();
        let data_path = tmp.path().join("partition-0.raw");
        fs::write(&data_path, b"test data").unwrap();

        write_checksum_file("deadbeef", &data_path, ChecksumType::Crc32).unwrap();

        let sidecar = tmp.path().join("partition-0.raw.crc32");
        assert!(sidecar.exists());
        let content = fs::read_to_string(&sidecar).unwrap();
        assert_eq!(content, "deadbeef  partition-0.raw\n");
    }

    /// ChecksumWriter must produce the same digest as if the written
    /// bytes were re-read and hashed in a separate pass. This is the
    /// invariant the tee-on-output backup pipeline relies on.
    #[test]
    fn checksum_writer_matches_post_write_hash() {
        use std::io::Cursor;

        let data: Vec<u8> = (0..65536u32).map(|i| (i & 0xff) as u8).collect();

        for algo in [ChecksumType::Sha256, ChecksumType::Crc32] {
            // Tee'd digest: write data through ChecksumWriter and finalise.
            let hasher = Arc::new(Mutex::new(Some(RunningHasher::new(algo))));
            let mut sink: Vec<u8> = Vec::new();
            {
                let mut writer = ChecksumWriter::new(Cursor::new(&mut sink), hasher.clone());
                writer.write_all(&data).unwrap();
                writer.flush().unwrap();
            }
            let tee_digest = finalize_shared_hasher(&hasher);

            // Independent digest: hash a fresh reader over the sink bytes.
            let mut reader = Cursor::new(&sink);
            let direct_digest = hash_reader_to_eof(&mut reader, algo).unwrap();

            assert_eq!(
                tee_digest,
                direct_digest,
                "tee'd {} digest must match post-write hash",
                match algo {
                    ChecksumType::Sha256 => "sha256",
                    ChecksumType::Crc32 => "crc32",
                }
            );
        }
    }
}
