//! Client side of the network protocol — the `RemoteSession` the CLI verbs
//! (`ls` / `get`) use to talk to a `rb-cli serve` daemon over an `rb://` ref.
//!
//! Phase 0 is read-only: connect + handshake, then `open_image` / `list_dir` /
//! `read_file`. Blocking, one request/reply at a time (no pipelining yet).

use anyhow::{anyhow, bail, Context, Result};
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::path::Path;

use crate::remote::protocol::{
    read_chunks, read_control, write_control, ChunkWriter, Request, Response, WireEntry,
    CAP_FAMILY_F, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION, RB_HELLO_MAGIC,
};

/// Result of opening a remote image: a handle + the filesystem's display
/// metadata.
#[derive(Debug, Clone)]
pub struct OpenedImage {
    pub handle: u64,
    pub label: String,
    pub fs_type: String,
    pub volume_label: Option<String>,
    pub total_size: u64,
    pub used_size: u64,
}

/// A live connection to a daemon, after a successful Family-F handshake.
pub struct RemoteSession {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl RemoteSession {
    /// Connect to `addr` (`host:port`) and complete the handshake, verifying
    /// the daemon offers Family F.
    pub fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).with_context(|| format!("connecting to {addr}"))?;
        stream.set_nodelay(true).ok();
        let reader = BufReader::new(stream.try_clone().context("cloning socket")?);
        let writer = BufWriter::new(stream);
        let mut session = Self { reader, writer };
        session.handshake()?;
        Ok(session)
    }

    fn handshake(&mut self) -> Result<()> {
        write_control(
            &mut self.writer,
            &Request::Hello {
                magic: RB_HELLO_MAGIC,
                version: PROTOCOL_VERSION,
            },
        )?;
        match self.read_response()? {
            Response::Hello {
                version,
                capabilities,
                ..
            } => {
                if version < MIN_PROTOCOL_VERSION {
                    bail!("remote daemon speaks protocol v{version}, too old for this client");
                }
                if capabilities & CAP_FAMILY_F == 0 {
                    bail!("remote daemon doesn't offer file transfer (Family F)");
                }
                Ok(())
            }
            Response::Error { message } => bail!("handshake refused: {message}"),
            other => bail!("unexpected handshake reply: {other:?}"),
        }
    }

    /// Open a disk image on the daemon. `label` mirrors the CLI's partition
    /// line; the rest is the opened filesystem's display metadata.
    pub fn open_image(&mut self, path: &str, partition: Option<u32>) -> Result<OpenedImage> {
        write_control(
            &mut self.writer,
            &Request::OpenImage {
                path: path.to_string(),
                partition,
            },
        )?;
        match self.read_response()? {
            Response::Opened {
                handle,
                label,
                fs_type,
                volume_label,
                total_size,
                used_size,
            } => Ok(OpenedImage {
                handle,
                label,
                fs_type,
                volume_label,
                total_size,
                used_size,
            }),
            Response::Error { message } => bail!("open {path}: {message}"),
            other => bail!("unexpected reply to OpenImage: {other:?}"),
        }
    }

    /// List a directory inside an opened image.
    pub fn list_dir(&mut self, handle: u64, path: &str) -> Result<Vec<WireEntry>> {
        write_control(
            &mut self.writer,
            &Request::ListDir {
                handle,
                path: path.to_string(),
            },
        )?;
        match self.read_response()? {
            Response::Dir { entries } => Ok(entries),
            Response::Error { message } => bail!("list {path}: {message}"),
            other => bail!("unexpected reply to ListDir: {other:?}"),
        }
    }

    /// Stream a single file's bytes into `sink`, returning the byte count.
    pub fn read_file(&mut self, handle: u64, path: &str, sink: &mut dyn Write) -> Result<u64> {
        write_control(
            &mut self.writer,
            &Request::ReadFile {
                handle,
                path: path.to_string(),
            },
        )?;
        match self.read_response()? {
            Response::FileBegin { .. } => {
                read_chunks(&mut self.reader, sink).map_err(|e| anyhow!("reading {path}: {e}"))
            }
            Response::Error { message } => bail!("read {path}: {message}"),
            other => bail!("unexpected reply to ReadFile: {other:?}"),
        }
    }

    /// Drop an opened-image handle on the daemon (the read-side `Close`). The
    /// daemon's handle table is per-connection, so closing a handle frees its
    /// open image without affecting other handles on the same session.
    pub fn close(&mut self, handle: u64) -> Result<()> {
        write_control(&mut self.writer, &Request::Close { handle })?;
        self.expect_ok("Close")
    }

    // --- write path (Phase 1: stage -> apply) ---

    /// Open a write session bound to a destination image. Returns the session
    /// id used by the `stage_*` / `apply` / `close_session` calls.
    pub fn open_session(&mut self, image_path: &str, partition: Option<u32>) -> Result<u64> {
        write_control(
            &mut self.writer,
            &Request::OpenSession {
                image_path: image_path.to_string(),
                partition,
            },
        )?;
        match self.read_response()? {
            Response::SessionOpened { session } => Ok(session),
            Response::Error { message } => bail!("open session for {image_path}: {message}"),
            other => bail!("unexpected reply to OpenSession: {other:?}"),
        }
    }

    /// Stage a host file into the session: upload its bytes to the daemon's
    /// staging area, queued as an AddFile applied at [`RemoteSession::apply`].
    #[allow(clippy::too_many_arguments)]
    pub fn stage_upload(
        &mut self,
        session: u64,
        dest_parent: &str,
        name: &str,
        host_file: &Path,
        force: bool,
        type_code: Option<String>,
        creator_code: Option<String>,
    ) -> Result<()> {
        let meta = std::fs::metadata(host_file)
            .with_context(|| format!("stat {}", host_file.display()))?;
        let size = meta.len();
        write_control(
            &mut self.writer,
            &Request::StageUpload {
                session,
                dest_parent: dest_parent.to_string(),
                name: name.to_string(),
                size,
                force,
                type_code,
                creator_code,
            },
        )?;
        // The body follows immediately as a chunk stream.
        let mut f = std::fs::File::open(host_file)
            .with_context(|| format!("open {}", host_file.display()))?;
        {
            let mut cw = ChunkWriter::new(&mut self.writer);
            std::io::copy(&mut f, &mut cw)
                .with_context(|| format!("uploading {}", host_file.display()))?;
            cw.finish()?;
        }
        self.expect_ok("StageUpload")
    }

    /// Stage a directory creation under the session.
    pub fn stage_mkdir(&mut self, session: u64, parent: &str, name: &str) -> Result<()> {
        write_control(
            &mut self.writer,
            &Request::StageMkdir {
                session,
                parent: parent.to_string(),
                name: name.to_string(),
            },
        )?;
        self.expect_ok("StageMkdir")
    }

    /// Stage an **on-device** copy: the daemon reads `src_path` from
    /// `src_image` (a path on its serve root) into the session's destination
    /// image. The file data never leaves the daemon — the client sends only the
    /// command.
    #[allow(clippy::too_many_arguments)]
    pub fn stage_copy_local(
        &mut self,
        session: u64,
        src_image: &str,
        src_partition: Option<u32>,
        src_path: &str,
        dest_parent: &str,
        name: &str,
        force: bool,
    ) -> Result<()> {
        write_control(
            &mut self.writer,
            &Request::StageCopyLocal {
                session,
                src_image: src_image.to_string(),
                src_partition,
                src_path: src_path.to_string(),
                dest_parent: dest_parent.to_string(),
                name: name.to_string(),
                force,
            },
        )?;
        self.expect_ok("StageCopyLocal")
    }

    // --- host-FS browse (Phase 2) ---

    /// Classify a host path on the daemon: `(exists, is_dir)`.
    pub fn host_stat(&mut self, path: &str) -> Result<(bool, bool)> {
        write_control(
            &mut self.writer,
            &Request::HostStat {
                path: path.to_string(),
            },
        )?;
        match self.read_response()? {
            Response::HostKind { exists, is_dir } => Ok((exists, is_dir)),
            Response::Error { message } => bail!("stat {path}: {message}"),
            other => bail!("unexpected reply to HostStat: {other:?}"),
        }
    }

    /// List a directory on the daemon's host filesystem.
    pub fn list_host_dir(&mut self, path: &str) -> Result<Vec<WireEntry>> {
        write_control(
            &mut self.writer,
            &Request::ListHostDir {
                path: path.to_string(),
            },
        )?;
        match self.read_response()? {
            Response::Dir { entries } => Ok(entries),
            Response::Error { message } => bail!("list host dir {path}: {message}"),
            other => bail!("unexpected reply to ListHostDir: {other:?}"),
        }
    }

    /// Stream a host file's bytes into `sink` (the host-FS analog of
    /// [`RemoteSession::read_file`]). Returns the byte count.
    pub fn read_host_file(&mut self, path: &str, sink: &mut dyn Write) -> Result<u64> {
        write_control(
            &mut self.writer,
            &Request::ReadHostFile {
                path: path.to_string(),
            },
        )?;
        match self.read_response()? {
            Response::FileBegin { .. } => {
                read_chunks(&mut self.reader, sink).map_err(|e| anyhow!("reading {path}: {e}"))
            }
            Response::Error { message } => bail!("read host file {path}: {message}"),
            other => bail!("unexpected reply to ReadHostFile: {other:?}"),
        }
    }

    /// Open a host image file as a raw block device on the daemon — it stays
    /// open for the session. Returns `(handle, size)`. The block-reader's first
    /// call.
    pub fn open_block(&mut self, path: &str) -> Result<(u64, u64)> {
        write_control(
            &mut self.writer,
            &Request::OpenBlock {
                path: path.to_string(),
            },
        )?;
        match self.read_response()? {
            Response::BlockOpened { handle, size } => Ok((handle, size)),
            Response::Error { message } => bail!("open block {path}: {message}"),
            other => bail!("unexpected reply to OpenBlock: {other:?}"),
        }
    }

    /// Open a host image file as a read-WRITE block device on the daemon — it
    /// stays open for the session. Returns `(handle, size)`. The write-side
    /// sibling of [`Self::open_block`]; the handle accepts [`Self::write_block`].
    pub fn open_block_rw(&mut self, path: &str) -> Result<(u64, u64)> {
        write_control(
            &mut self.writer,
            &Request::OpenBlockRw {
                path: path.to_string(),
            },
        )?;
        match self.read_response()? {
            Response::BlockOpened { handle, size } => Ok((handle, size)),
            Response::Error { message } => bail!("open block (rw) {path}: {message}"),
            other => bail!("unexpected reply to OpenBlockRw: {other:?}"),
        }
    }

    /// Write `bytes` at `offset` into a read-write block handle. The payload
    /// follows the control frame as a chunk stream; the daemon patches the bytes
    /// in place and replies `Ok`. The write must lie within the image (it never
    /// grows the file). The wire primitive behind the writable
    /// [`crate::remote::block_reader::RemoteBlockReader`].
    pub fn write_block(&mut self, handle: u64, offset: u64, bytes: &[u8]) -> Result<()> {
        let len = u32::try_from(bytes.len())
            .map_err(|_| anyhow!("write of {} bytes exceeds u32", bytes.len()))?;
        write_control(
            &mut self.writer,
            &Request::WriteBlock {
                handle,
                offset,
                len,
            },
        )?;
        {
            let mut cw = ChunkWriter::new(&mut self.writer);
            cw.write_all(bytes)
                .with_context(|| format!("uploading {len} bytes at {offset}"))?;
            cw.finish()?;
        }
        self.expect_ok("WriteBlock")
    }

    /// Flush a read-write block handle to stable storage on the daemon
    /// (`sync_all`). The engine's `sync_metadata` flush maps here, so a finished
    /// edit is durable.
    pub fn flush_block(&mut self, handle: u64) -> Result<()> {
        write_control(&mut self.writer, &Request::FlushBlock { handle })?;
        self.expect_ok("FlushBlock")
    }

    /// Open a restore **write target** (a device or a freshly-sized image file)
    /// on the daemon; returns `(handle, size)` where `size` is the device's real
    /// capacity (device) or the requested image size. The handle accepts
    /// [`Self::write_block`].
    pub fn open_write_target(
        &mut self,
        path: &str,
        is_device: bool,
        size: u64,
    ) -> Result<(u64, u64)> {
        write_control(
            &mut self.writer,
            &Request::OpenWriteTarget {
                path: path.to_string(),
                is_device,
                size,
            },
        )?;
        match self.read_response()? {
            Response::BlockOpened { handle, size } => Ok((handle, size)),
            Response::Error { message } => bail!("open write target {path}: {message}"),
            other => bail!("unexpected reply to OpenWriteTarget: {other:?}"),
        }
    }

    /// List the daemon machine's physical disk devices.
    pub fn list_devices(&mut self) -> Result<Vec<crate::remote::protocol::WireDevice>> {
        write_control(&mut self.writer, &Request::ListDevices)?;
        match self.read_response()? {
            Response::Devices { devices } => Ok(devices),
            Response::Error { message } => bail!("list devices: {message}"),
            other => bail!("unexpected reply to ListDevices: {other:?}"),
        }
    }

    /// Open one of the daemon's enumerated physical devices as a raw block
    /// handle (read-only). Returns `(handle, size)`. The device-backed sibling
    /// of [`Self::open_block`].
    pub fn open_device(&mut self, path: &str) -> Result<(u64, u64)> {
        write_control(
            &mut self.writer,
            &Request::OpenDevice {
                path: path.to_string(),
            },
        )?;
        match self.read_response()? {
            Response::BlockOpened { handle, size } => Ok((handle, size)),
            Response::Error { message } => bail!("open device {path}: {message}"),
            other => bail!("unexpected reply to OpenDevice: {other:?}"),
        }
    }

    /// Read a byte range `[offset, offset+len)` from an open block handle.
    /// Returns the bytes actually read (short at EOF, empty past it) — the wire
    /// primitive behind [`crate::remote::block_reader::RemoteBlockReader`].
    pub fn read_block(&mut self, handle: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
        write_control(
            &mut self.writer,
            &Request::ReadBlock {
                handle,
                offset,
                len,
            },
        )?;
        match self.read_response()? {
            Response::FileBegin { .. } => {
                let mut buf = Vec::new();
                read_chunks(&mut self.reader, &mut buf)
                    .map_err(|e| anyhow!("reading block of handle {handle}: {e}"))?;
                Ok(buf)
            }
            Response::Error { message } => bail!("read block {handle}: {message}"),
            other => bail!("unexpected reply to ReadBlock: {other:?}"),
        }
    }

    /// Close an open block handle on the daemon.
    pub fn close_block(&mut self, handle: u64) -> Result<()> {
        write_control(&mut self.writer, &Request::CloseBlock { handle })?;
        self.expect_ok("CloseBlock")
    }

    /// Replay the session's staged edits onto the image; returns the count.
    pub fn apply(&mut self, session: u64) -> Result<u64> {
        write_control(&mut self.writer, &Request::Apply { session })?;
        match self.read_response()? {
            Response::Applied { count } => Ok(count),
            Response::Error { message } => bail!("apply: {message}"),
            other => bail!("unexpected reply to Apply: {other:?}"),
        }
    }

    /// Discard the session and its staging blobs.
    pub fn close_session(&mut self, session: u64) -> Result<()> {
        write_control(&mut self.writer, &Request::CloseSession { session })?;
        self.expect_ok("CloseSession")
    }

    fn expect_ok(&mut self, what: &str) -> Result<()> {
        match self.read_response()? {
            Response::Ok => Ok(()),
            Response::Error { message } => bail!("{what}: {message}"),
            other => bail!("unexpected reply to {what}: {other:?}"),
        }
    }

    fn read_response(&mut self) -> Result<Response> {
        read_control(&mut self.reader).context("reading daemon response")
    }
}

impl Drop for RemoteSession {
    fn drop(&mut self) {
        // Best-effort polite close; ignore errors (the socket may be gone).
        let _ = write_control(&mut self.writer, &Request::Bye);
    }
}
