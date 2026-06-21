//! Wire protocol for the rusty-backup network daemon (`rb-cli serve`).
//!
//! **Phase 0 — Family F, read-only.** This is the make-or-break spike from
//! `docs/remote_transfer_plan.md` §15: a `serve` daemon that opens a disk image
//! it holds and lets a remote `rb-cli` browse and read files *inside* it, at the
//! operation level (the daemon parses the filesystem; the client never pulls raw
//! blocks). Only the read path is here — `OpenImage` / `ListDir` / `ReadFile`.
//!
//! ## Framing
//! Two interleaved layers over one TCP stream:
//!   * **Control frames** — `[u32 le len][len bytes JSON]`, a [`Request`] from the
//!     client or a [`Response`] from the server. Small; `serde_json` (always-on).
//!   * **Chunk stream** — after a [`Response::FileBegin`], the file body follows
//!     as `[u32 le n][n bytes]` frames terminated by a single `[u32 0]`. Bulk
//!     bytes are raw, never base64-in-JSON.
//!
//! ## Forward-compat note (Family B / cb-dos)
//! The umbrella plan calls for a **binary** `Hello` so the JSON-free DOS client
//! (cb-dos) can reach Family B without a JSON parser. Phase 0 is Family F only,
//! so the handshake here is JSON like every other control frame; it carries a
//! `magic` + `version` so the binary form can be introduced additively in
//! Phase 4a without breaking detection. See the plan §2.3 / §5.0.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

/// Magic in the `Hello` request — lets the daemon reject a non-rusty-backup
/// peer immediately ("RBK0" as a u32).
pub const RB_HELLO_MAGIC: u32 = 0x5242_4B30;

/// Current wire-protocol version. Bump on a breaking change; keep additions
/// additive so a newer client still talks to an older daemon where possible.
///
/// v2 added the block-reader tier (`OpenBlock` / `ReadBlock` / `CloseBlock`,
/// later `OpenBlockRw` / `WriteBlock` / `FlushBlock` for in-place editing) — a
/// daemon must be at v2 to serve ranged reads/writes, so a remote *image
/// inspection or edit* (vs the v1 operation-level file browse) needs the daemon
/// refreshed. The write verbs are additive, so a v2 read-only daemon still
/// serves browse / inspect / backup; only editing needs the newer build.
pub const PROTOCOL_VERSION: u16 = 2;

/// Oldest protocol version this build still understands. The v1 verbs are
/// unchanged, so a v1 daemon still serves browse / read / write.
pub const MIN_PROTOCOL_VERSION: u16 = 1;

/// Default daemon port (mrext owns 8182; we take 7341). Configurable on both
/// ends via `--bind` / an explicit `rb://host:PORT/...`.
pub const DEFAULT_PORT: u16 = 7341;

// Capability bits advertised in the `Hello` response.
/// Family F — file transfer (browse / read; the only family in Phase 0).
pub const CAP_FAMILY_F: u16 = 1 << 0;
/// Family B — backup stream. Not implemented yet (Phase 4); flag reserved.
pub const CAP_FAMILY_B: u16 = 1 << 1;

/// Largest control-frame body we'll accept — guards against a hostile or
/// corrupt length prefix. Control frames are small JSON; 8 MiB is generous.
const MAX_CONTROL_FRAME: usize = 8 * 1024 * 1024;

/// Largest single chunk on the bulk path (the fs layer already streams in
/// 64 KiB–1 MiB blocks, so this only bounds a pathological single `write`).
const MAX_CHUNK: usize = 4 * 1024 * 1024;

/// Client → server control messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Handshake. `magic` / `version` let the daemon sanity-check the peer.
    Hello { magic: u32, version: u16 },
    /// Open a disk image on the daemon (path relative to the serve root) with
    /// an optional 1-based partition selector, mirroring the CLI's `IMG@N`.
    OpenImage {
        path: String,
        partition: Option<u32>,
    },
    /// List a directory inside a previously-opened image.
    ListDir { handle: u64, path: String },
    /// Stream one file's bytes (reply: `FileBegin`, then a chunk stream).
    ReadFile { handle: u64, path: String },
    /// Drop an opened-image handle.
    Close { handle: u64 },

    // --- Family F write path (Phase 1: stage -> apply) ---
    /// Open a write session bound to a destination image. The image is opened
    /// editable only at `Apply`; staged edits accumulate until then.
    OpenSession {
        image_path: String,
        partition: Option<u32>,
    },
    /// Stage a host file into the session's queue. The file body follows
    /// **immediately** as a chunk stream (client -> server) after this frame,
    /// then the daemon replies `Ok`.
    StageUpload {
        session: u64,
        dest_parent: String,
        name: String,
        size: u64,
        force: bool,
        type_code: Option<String>,
        creator_code: Option<String>,
    },
    /// Stage a directory creation.
    StageMkdir {
        session: u64,
        parent: String,
        name: String,
    },
    /// Replay the session's staged edits onto the image: open editable once,
    /// mutate, `sync_metadata`, commit.
    Apply { session: u64 },
    /// Discard a session and its staging blobs (also serves as abort).
    CloseSession { session: u64 },

    // --- host-FS browse + on-device copy (Phase 2) ---
    /// List a directory on the daemon's host filesystem (sandboxed to root).
    ListHostDir { path: String },
    /// Classify a host path — does it exist, and is it a directory? Lets the
    /// client distinguish a host directory to browse from an image file to
    /// open when no `@N` partition was given.
    HostStat { path: String },
    /// Stream a host file's raw bytes (reply: `FileBegin`, then a chunk stream).
    /// The host-FS analog of `ReadFile` — for copying a file off the remote, or
    /// the file browser's preview.
    ReadHostFile { path: String },

    // --- block tier (v2): a host image file kept OPEN on the daemon for ---
    // --- seekable ranged reads, so the desktop engine parses partitions / ---
    // --- filesystems (and inspects) without downloading the whole disk. ---
    /// Open a host file as a raw block device and keep it open on the daemon
    /// (reply: `BlockOpened{handle, size}`). The handle lives until `CloseBlock`
    /// or the connection drops — so the user works against one open image.
    OpenBlock { path: String },
    /// Read a byte range from an open block handle (reply: `FileBegin{size:
    /// actual}`, then a chunk stream of that many bytes). `len` is capped
    /// server-side; a read past EOF returns the short tail.
    ReadBlock { handle: u64, offset: u64, len: u32 },
    /// Close an open block handle.
    CloseBlock { handle: u64 },

    // --- block tier writes (remote editing): open a host image file ---
    // --- read-WRITE and patch byte ranges in place, so the desktop engine ---
    // --- edits a remote image's filesystem over the wire (add/delete file, ---
    // --- mkdir, fsck repair, resize) exactly as for a local file. ---
    /// Open a host file as a read-WRITE block device kept open on the daemon
    /// (reply: `BlockOpened{handle, size}`). The write-side sibling of
    /// [`Request::OpenBlock`]; the handle accepts `WriteBlock`. Physical devices
    /// (`OpenDevice`) stay read-only — restore-to-device is a separate path.
    OpenBlockRw { path: String },
    /// Write a byte range to a read-write block handle: this control frame is
    /// followed **immediately** by a chunk stream of `len` bytes (client ->
    /// server), then the daemon replies `Ok`. The write must lie within the
    /// image (`offset + len <= size`) — a block-tier edit never grows the file.
    WriteBlock { handle: u64, offset: u64, len: u32 },
    /// Flush a read-write block handle to stable storage (`sync_all`); reply
    /// `Ok`. The engine's `sync_metadata` calls `flush()` once at the end of an
    /// edit, which maps to this — so a finished edit is durable on the daemon.
    FlushBlock { handle: u64 },

    /// Open a **write target** for restore: a destination the desktop pushes a
    /// finished disk image to via `WriteBlock` (reply: `BlockOpened{handle,
    /// size}`). When `is_device` is true the target is one of the daemon's
    /// **enumerated** physical devices (validated against `ListDevices`, opened
    /// read-write, elevated) and `size` is advisory — the reply carries the
    /// device's real capacity. When false it is a host **image file** under the
    /// serve root, created/truncated to `size` bytes and opened read-write.
    /// Destructive by nature (restore overwrites the target). The handle is a
    /// writable block handle, so it accepts `WriteBlock` / `FlushBlock`.
    OpenWriteTarget {
        path: String,
        is_device: bool,
        size: u64,
    },

    // --- physical-device backup (v2): the daemon enumerates its own disks ---
    // --- and serves a raw device over the same block-handle machinery, so ---
    // --- the desktop can back up a remote drive without moving the media. ---
    /// List the daemon machine's physical disk devices (reply: `Devices`).
    ListDevices,
    /// Open one of the **enumerated** physical devices (validated against the
    /// live `ListDevices` set) as a raw block handle kept open on the daemon
    /// (reply: `BlockOpened{handle, size}`). Unlike `OpenBlock` the path is NOT
    /// joined under the serve root — it must match an enumerated device path
    /// exactly, so this can't be used to read arbitrary files. Read-only.
    OpenDevice { path: String },
    /// Stage an **on-device** copy: the daemon reads `src_path` from `src_image`
    /// (relative to the serve root, partition `src_partition`) and queues it as
    /// an AddFile into the session's destination image — no desktop round-trip.
    StageCopyLocal {
        session: u64,
        src_image: String,
        src_partition: Option<u32>,
        src_path: String,
        dest_parent: String,
        name: String,
        force: bool,
    },

    /// Polite disconnect.
    Bye,
}

/// Server → client control messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Handshake reply with the negotiated capability flags.
    Hello {
        version: u16,
        capabilities: u16,
        platform: String,
    },
    /// Image opened. `label` mirrors the CLI's partition line; the rest is the
    /// opened filesystem's metadata, for a remote pane's display.
    Opened {
        handle: u64,
        label: String,
        fs_type: String,
        volume_label: Option<String>,
        total_size: u64,
        used_size: u64,
    },
    /// Directory listing.
    Dir { entries: Vec<WireEntry> },
    /// A file is about to stream as a chunk sequence (see [`read_chunks`]).
    FileBegin { size: u64 },
    /// A write session was opened.
    SessionOpened { session: u64 },
    /// `Apply` finished; `count` staged edits were replayed.
    Applied { count: u64 },
    /// `HostStat` result: whether the path exists and is a directory.
    HostKind { exists: bool, is_dir: bool },
    /// `OpenBlock` result: the block handle + the image's length in bytes.
    BlockOpened { handle: u64, size: u64 },
    /// `ListDevices` result: the daemon machine's physical disks.
    Devices { devices: Vec<WireDevice> },
    /// Generic success (e.g. `Close`).
    Ok,
    /// Operation failed with a human-readable message.
    Error { message: String },
}

/// Serde-friendly mirror of [`crate::fs::entry::FileEntry`] carrying only what
/// the read path (`ls` / `get`) needs. The daemon owns the real `FileEntry`;
/// the client only displays metadata and re-requests files by path, so a thin
/// DTO avoids deriving serde across the whole engine type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireEntry {
    pub name: String,
    pub path: String,
    pub kind: WireKind,
    pub size: u64,
    pub type_code: Option<String>,
    pub creator_code: Option<String>,
    pub symlink_target: Option<String>,
}

/// Entry kind over the wire — mirrors [`crate::fs::entry::EntryType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireKind {
    Dir,
    File,
    Symlink,
    Special,
}

impl WireEntry {
    /// Project an engine `FileEntry` down to the wire shape.
    pub fn from_entry(e: &crate::fs::entry::FileEntry) -> Self {
        use crate::fs::entry::EntryType;
        let kind = match e.entry_type {
            EntryType::Directory => WireKind::Dir,
            EntryType::File => WireKind::File,
            EntryType::Symlink => WireKind::Symlink,
            EntryType::Special => WireKind::Special,
        };
        Self {
            name: e.name.clone(),
            path: e.path.clone(),
            kind,
            size: e.size,
            type_code: e.type_code.clone(),
            creator_code: e.creator_code.clone(),
            symlink_target: e.symlink_target.clone(),
        }
    }

    pub fn is_dir(&self) -> bool {
        self.kind == WireKind::Dir
    }
}

/// Serde-friendly mirror of [`crate::device::DiskDevice`] for `ListDevices`,
/// carrying only what the desktop's remote-device picker needs to display and
/// then re-open (by `path`). The mount/partition detail in the engine struct is
/// dropped — the desktop parses the partition table itself over the block tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireDevice {
    /// Device path on the daemon machine (e.g. `/dev/sda`); the value the
    /// desktop passes back to `OpenDevice`.
    pub path: String,
    /// Short device name (e.g. `sda`).
    pub name: String,
    pub size_bytes: u64,
    pub is_removable: bool,
    pub is_read_only: bool,
    pub is_system: bool,
    /// Bus/protocol label (e.g. `USB`, `SATA`).
    pub bus: String,
    /// Human-readable media/model name.
    pub media: String,
}

impl WireDevice {
    /// Project an engine [`crate::device::DiskDevice`] down to the wire shape.
    pub fn from_device(d: &crate::device::DiskDevice) -> Self {
        Self {
            path: d.path.to_string_lossy().into_owned(),
            name: d.name.clone(),
            size_bytes: d.size_bytes,
            is_removable: d.is_removable,
            is_read_only: d.is_read_only,
            is_system: d.is_system,
            bus: d.bus_protocol.clone(),
            media: d.media_name.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Framing — control frames
// ---------------------------------------------------------------------------

/// Write one length-prefixed JSON control frame and flush it.
pub fn write_control<W: Write, T: Serialize>(w: &mut W, msg: &T) -> io::Result<()> {
    let body = serde_json::to_vec(msg).map_err(json_err)?;
    let len = u32::try_from(body.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "control frame too large"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&body)?;
    w.flush()
}

/// Read one length-prefixed JSON control frame.
///
/// Surfaces `UnexpectedEof` verbatim so the daemon's accept loop can treat a
/// peer hangup as a clean disconnect rather than an error.
pub fn read_control<R: Read, T: DeserializeOwned>(r: &mut R) -> io::Result<T> {
    let mut len_bytes = [0u8; 4];
    r.read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > MAX_CONTROL_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control frame exceeds limit",
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    serde_json::from_slice(&buf).map_err(json_err)
}

fn json_err(e: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

// ---------------------------------------------------------------------------
// Framing — bulk chunk stream
// ---------------------------------------------------------------------------

/// Adapts a writer into a length-delimited chunk stream. Each `write` becomes
/// one `[u32 le len][bytes]` frame; [`ChunkWriter::finish`] writes the
/// terminating `[u32 0]`. Used to stream a file body after `FileBegin`.
pub struct ChunkWriter<'a, W: Write> {
    inner: &'a mut W,
}

impl<'a, W: Write> ChunkWriter<'a, W> {
    pub fn new(inner: &'a mut W) -> Self {
        Self { inner }
    }

    /// Emit the zero-length terminator and flush.
    pub fn finish(self) -> io::Result<()> {
        self.inner.write_all(&0u32.to_le_bytes())?;
        self.inner.flush()
    }
}

impl<W: Write> Write for ChunkWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // Cap one frame; `write_all` loops on the short return for the rest.
        let n = buf.len().min(MAX_CHUNK);
        self.inner.write_all(&(n as u32).to_le_bytes())?;
        self.inner.write_all(&buf[..n])?;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Read a chunk stream (written by [`ChunkWriter`]) into `sink` until the
/// zero-length terminator. Returns the total byte count.
pub fn read_chunks<R: Read, W: Write + ?Sized>(r: &mut R, sink: &mut W) -> io::Result<u64> {
    let mut total = 0u64;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let mut len_bytes = [0u8; 4];
        r.read_exact(&mut len_bytes)?;
        let n = u32::from_le_bytes(len_bytes) as usize;
        if n == 0 {
            break;
        }
        if n > MAX_CHUNK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk exceeds limit",
            ));
        }
        if n > buf.len() {
            buf.resize(n, 0);
        }
        r.read_exact(&mut buf[..n])?;
        sink.write_all(&buf[..n])?;
        total += n as u64;
    }
    Ok(total)
}

// ---------------------------------------------------------------------------
// rb:// reference parsing (client side)
// ---------------------------------------------------------------------------

/// A parsed `rb://host[:port]/path` reference (the image path on the daemon,
/// relative to its serve root). The `IMG@N` partition selector is parsed
/// separately by [`crate::cli::img_at::ImageRef`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRef {
    pub host: String,
    pub port: u16,
    /// Image path on the daemon, leading `/` retained (relative to serve root).
    pub path: String,
}

impl RemoteRef {
    /// Parse `rb://host[:port]/path`. Returns `None` when `s` is not an
    /// `rb://` reference, so callers fall through to the local path.
    ///
    /// Tolerates a collapsed `rb:/` (a single slash) in case a `PathBuf`
    /// round-trip ate one — clap parses the ref into an `ImageRef { path:
    /// PathBuf, .. }` before we see it.
    pub fn parse(s: &str) -> Option<RemoteRef> {
        let rest = s.strip_prefix("rb://").or_else(|| s.strip_prefix("rb:/"))?;
        // Authority runs up to the first '/'; the rest (with that '/') is the path.
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        if authority.is_empty() {
            return None;
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (h, p.parse().ok()?),
            None => (authority, DEFAULT_PORT),
        };
        if host.is_empty() {
            return None;
        }
        Some(RemoteRef {
            host: host.to_string(),
            port,
            path: path.to_string(),
        })
    }

    /// `host:port`, ready for [`std::net::TcpStream::connect`].
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_host_port_path() {
        let r = RemoteRef::parse("rb://mister:7341/games/dos.img").unwrap();
        assert_eq!(r.host, "mister");
        assert_eq!(r.port, 7341);
        assert_eq!(r.path, "/games/dos.img");
        assert_eq!(r.addr(), "mister:7341");
    }

    #[test]
    fn default_port_when_omitted() {
        let r = RemoteRef::parse("rb://192.168.1.5/d.img").unwrap();
        assert_eq!(r.host, "192.168.1.5");
        assert_eq!(r.port, DEFAULT_PORT);
        assert_eq!(r.path, "/d.img");
    }

    #[test]
    fn bare_host_lists_root() {
        let r = RemoteRef::parse("rb://host").unwrap();
        assert_eq!(r.path, "/");
    }

    #[test]
    fn tolerates_collapsed_double_slash() {
        // A PathBuf round-trip *shouldn't* collapse `//`, but be defensive.
        let r = RemoteRef::parse("rb:/host:9/x.img").unwrap();
        assert_eq!(r.host, "host");
        assert_eq!(r.port, 9);
        assert_eq!(r.path, "/x.img");
    }

    #[test]
    fn non_remote_is_none() {
        assert!(RemoteRef::parse("/local/disk.img").is_none());
        assert!(RemoteRef::parse("disk.img").is_none());
        assert!(RemoteRef::parse(r"C:\disk.img").is_none());
    }

    #[test]
    fn image_ref_preserves_rb_url() {
        // The whole client wiring relies on clap's ImageRef keeping the rb://
        // string intact through its PathBuf (and on `@N` still splitting).
        let r = crate::cli::img_at::ImageRef::parse("rb://mister:7341/d.img@2").unwrap();
        assert_eq!(r.partition, Some(2));
        let s = r.path.to_string_lossy();
        assert_eq!(s, "rb://mister:7341/d.img");
        assert_eq!(RemoteRef::parse(&s).unwrap().path, "/d.img");
    }
}
