//! Client side of the network protocol — the `RemoteSession` the CLI verbs
//! (`ls` / `get`) use to talk to a `rb-cli serve` daemon over an `rb://` ref.
//!
//! Phase 0 is read-only: connect + handshake, then `open_image` / `list_dir` /
//! `read_file`. Blocking, one request/reply at a time (no pipelining yet).

use anyhow::{anyhow, bail, Context, Result};
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;

use crate::remote::protocol::{
    read_chunks, read_control, write_control, Request, Response, WireEntry, CAP_FAMILY_F,
    MIN_PROTOCOL_VERSION, PROTOCOL_VERSION, RB_HELLO_MAGIC,
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
