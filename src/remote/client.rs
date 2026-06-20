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

    /// Open a disk image on the daemon. Returns `(handle, label)` where
    /// `label` mirrors the CLI's partition line (printed to stderr by callers).
    pub fn open_image(&mut self, path: &str, partition: Option<u32>) -> Result<(u64, String)> {
        write_control(
            &mut self.writer,
            &Request::OpenImage {
                path: path.to_string(),
                partition,
            },
        )?;
        match self.read_response()? {
            Response::Opened { handle, label } => Ok((handle, label)),
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
