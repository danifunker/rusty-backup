//! The `rb-cli serve` daemon — Family F (Phase 0 read + Phase 1 write).
//!
//! Blocking, thread-per-connection (`std::net`, no async runtime — matches the
//! repo's `std::thread` idiom and keeps the slim build lean). Each connection
//! owns its own handle + session tables, so no shared state / locking is needed.
//!
//! The daemon runs **the exact same pipeline the local CLI runs** — for reads
//! `resolve_partition_streaming_forced_inside` → `open_filesystem` →
//! `list_directory` / `write_file_to`; for writes `resolve_partition_rw_forced`
//! → `open_editable_filesystem` → `create_file` / `create_directory` →
//! `sync_metadata` → commit — so a remote operation behaves identically to a
//! local one. The client never sees raw blocks; it browses, reads, and stages
//! edits at the operation level (plan §2.2 / §2.4).
//!
//! **Phase 1 stage→apply.** A write session (bound to a destination image)
//! accumulates staged edits — uploaded host files land in a per-session staging
//! dir, directory creations queue as records — and `Apply` opens the image
//! editable **once** and replays them, mirroring Commander's `EditQueue` (plan
//! §2.4 / §6). Staging blobs and the queue live for the session's lifetime and
//! are dropped on `CloseSession` / disconnect. (Phase-1 caveat: `Apply` writes
//! to the live image like the local CLI does — a true atomic batch / recoverable
//! queue is a later refinement.)

use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};

use crate::cli::verbs::ls::resolve_path;
use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions, Filesystem};
use crate::remote::protocol::{
    read_chunks, read_control, write_control, ChunkWriter, Request, Response, WireEntry, WireKind,
    CAP_FAMILY_F, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION, RB_HELLO_MAGIC,
};

/// Daemon configuration (from the `serve` verb).
pub struct ServeConfig {
    /// Bind address, e.g. `0.0.0.0:7341`.
    pub bind: String,
    /// Root directory images are served from; every opened path is sandboxed
    /// under it.
    pub root: PathBuf,
    /// Where per-session upload staging blobs live. `None` uses the system temp
    /// dir. On a MiSTer this should point at a roomy writable mount, never tmpfs
    /// (plan §6) — multi-hundred-MB uploads would OOM `/tmp`.
    pub staging_dir: Option<PathBuf>,
}

/// One staged edit awaiting `Apply` (the relocated `StagedEdit`, plan §2.4).
enum StagedEdit {
    AddFile {
        dest_parent: String,
        name: String,
        blob: PathBuf,
        size: u64,
        force: bool,
        type_code: Option<String>,
        creator_code: Option<String>,
    },
    Mkdir {
        parent: String,
        name: String,
    },
}

/// A write session: the destination image + a queue of staged edits + the
/// staging dir holding uploaded blobs (cleaned when the session is dropped).
struct Session {
    image_path: PathBuf,
    partition: Option<u32>,
    staging: tempfile::TempDir,
    edits: Vec<StagedEdit>,
    blob_seq: u64,
}

/// Bind and run the accept loop forever (until the process is killed).
pub fn serve(cfg: ServeConfig) -> Result<()> {
    let root = cfg
        .root
        .canonicalize()
        .with_context(|| format!("serve root {:?}", cfg.root))?;
    let listener = TcpListener::bind(&cfg.bind).with_context(|| format!("binding {}", cfg.bind))?;
    let bound = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| cfg.bind.clone());
    // Status to stderr; keeps stdout clean for anything scripted.
    eprintln!(
        "rb-cli serve: listening on {bound} (root {})",
        root.display()
    );
    eprintln!("rb-cli serve: Family F read+write (stage->apply). Ctrl-C to stop.");
    serve_on(listener, root, cfg.staging_dir)
}

/// Run the accept loop on an already-bound listener. `root` must already be
/// canonicalized (the sandbox check compares paths against it). Blocks until the
/// process exits. Exposed so integration tests can bind a port-0 listener and
/// drive the daemon without guessing a port.
pub fn serve_on(listener: TcpListener, root: PathBuf, staging_dir: Option<PathBuf>) -> Result<()> {
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let root = root.clone();
                let staging_dir = staging_dir.clone();
                std::thread::spawn(move || {
                    let peer = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "?".into());
                    eprintln!("rb-cli serve: {peer} connected");
                    match handle_conn(stream, &root, staging_dir.as_deref()) {
                        Ok(()) => eprintln!("rb-cli serve: {peer} disconnected"),
                        Err(e) => eprintln!("rb-cli serve: {peer} ended: {e:#}"),
                    }
                });
            }
            Err(e) => eprintln!("rb-cli serve: accept error: {e}"),
        }
    }
    Ok(())
}

/// Serve one connection until the peer says `Bye` or hangs up.
fn handle_conn(stream: TcpStream, root: &Path, staging_dir: Option<&Path>) -> Result<()> {
    stream.set_nodelay(true).ok();
    let mut reader = BufReader::new(stream.try_clone().context("cloning socket")?);
    let mut writer = BufWriter::new(stream);
    let mut handles: HashMap<u64, Box<dyn Filesystem>> = HashMap::new();
    let mut next_handle: u64 = 1;
    // Block tier: host image files kept open for ranged reads. Shares the
    // `next_handle` counter so handles are unique across both tables.
    let mut block_handles: HashMap<u64, std::fs::File> = HashMap::new();
    let mut sessions: HashMap<u64, Session> = HashMap::new();
    let mut next_session: u64 = 1;

    loop {
        let req: Request = match read_control(&mut reader) {
            Ok(r) => r,
            // Peer hung up between requests — a clean end, not an error.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e).context("reading request"),
        };

        match req {
            Request::Hello { magic, version } => {
                if magic != RB_HELLO_MAGIC || version < MIN_PROTOCOL_VERSION {
                    reply_err(
                        &mut writer,
                        format!("unsupported client (magic {magic:#x}, version {version})"),
                    )?;
                    return Ok(());
                }
                write_control(
                    &mut writer,
                    &Response::Hello {
                        version: PROTOCOL_VERSION,
                        capabilities: CAP_FAMILY_F,
                        platform: std::env::consts::OS.to_string(),
                    },
                )?;
            }

            Request::OpenImage { path, partition } => match open_image(root, &path, partition) {
                Ok(o) => {
                    let handle = next_handle;
                    next_handle += 1;
                    handles.insert(handle, o.fs);
                    write_control(
                        &mut writer,
                        &Response::Opened {
                            handle,
                            label: o.label,
                            fs_type: o.fs_type,
                            volume_label: o.volume_label,
                            total_size: o.total_size,
                            used_size: o.used_size,
                        },
                    )?;
                }
                Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
            },

            Request::ListDir { handle, path } => match handles.get_mut(&handle) {
                Some(fs) => match list_dir(&mut **fs, &path) {
                    Ok(entries) => write_control(&mut writer, &Response::Dir { entries })?,
                    Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                },
                None => reply_err(&mut writer, format!("no such handle {handle}"))?,
            },

            Request::ReadFile { handle, path } => match handles.get_mut(&handle) {
                None => reply_err(&mut writer, format!("no such handle {handle}"))?,
                Some(fs) => match resolve_path(&mut **fs, &path) {
                    Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                    Ok(entry) if entry.is_directory() => {
                        reply_err(&mut writer, format!("{path} is a directory"))?
                    }
                    Ok(entry) => {
                        // Commit to the chunk stream. Once FileBegin is on the
                        // wire the client is in chunk-read mode, so a mid-stream
                        // engine error can't be reported as a control frame —
                        // we drop the connection instead (the client sees a
                        // truncated read, never bad data).
                        write_control(&mut writer, &Response::FileBegin { size: entry.size })?;
                        let mut cw = ChunkWriter::new(&mut writer);
                        if let Err(e) = fs.write_file_to(&entry, &mut cw) {
                            return Err(anyhow!("write_file_to({path}): {e}"));
                        }
                        cw.finish()?;
                    }
                },
            },

            Request::Close { handle } => {
                handles.remove(&handle);
                write_control(&mut writer, &Response::Ok)?;
            }

            // --- write path (Phase 1) ---
            Request::OpenSession {
                image_path,
                partition,
            } => match open_session(root, &image_path, partition, staging_dir) {
                Ok(session) => {
                    let id = next_session;
                    next_session += 1;
                    sessions.insert(id, session);
                    write_control(&mut writer, &Response::SessionOpened { session: id })?;
                }
                Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
            },

            Request::StageUpload {
                session,
                dest_parent,
                name,
                size,
                force,
                type_code,
                creator_code,
            } => {
                // The file body follows as a chunk stream and MUST be consumed
                // regardless of session validity, or the framing desyncs.
                match sessions.get_mut(&session) {
                    Some(sess) => {
                        let blob = sess.staging.path().join(format!("blob-{}", sess.blob_seq));
                        sess.blob_seq += 1;
                        match stage_blob(&mut reader, &blob) {
                            Ok(()) => {
                                sess.edits.push(StagedEdit::AddFile {
                                    dest_parent,
                                    name,
                                    blob,
                                    size,
                                    force,
                                    type_code,
                                    creator_code,
                                });
                                write_control(&mut writer, &Response::Ok)?;
                            }
                            Err(e) => reply_err(&mut writer, format!("staging upload: {e:#}"))?,
                        }
                    }
                    None => {
                        // Drain the body to keep the stream aligned, then report.
                        let _ = read_chunks(&mut reader, &mut std::io::sink());
                        reply_err(&mut writer, format!("no such session {session}"))?;
                    }
                }
            }

            Request::StageMkdir {
                session,
                parent,
                name,
            } => match sessions.get_mut(&session) {
                Some(sess) => {
                    sess.edits.push(StagedEdit::Mkdir { parent, name });
                    write_control(&mut writer, &Response::Ok)?;
                }
                None => reply_err(&mut writer, format!("no such session {session}"))?,
            },

            Request::Apply { session } => match sessions.get(&session) {
                Some(sess) => match apply_session(sess) {
                    Ok(count) => write_control(&mut writer, &Response::Applied { count })?,
                    Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                },
                None => reply_err(&mut writer, format!("no such session {session}"))?,
            },

            Request::CloseSession { session } => {
                // Dropping the Session drops its TempDir → staging cleaned up.
                sessions.remove(&session);
                write_control(&mut writer, &Response::Ok)?;
            }

            // --- host-FS browse + on-device copy (Phase 2) ---
            Request::ListHostDir { path } => match list_host_dir(root, &path) {
                Ok(entries) => write_control(&mut writer, &Response::Dir { entries })?,
                Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
            },

            Request::HostStat { path } => match host_stat(root, &path) {
                Ok((exists, is_dir)) => {
                    write_control(&mut writer, &Response::HostKind { exists, is_dir })?
                }
                Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
            },

            Request::ReadHostFile { path } => match sandbox_join(root, &path) {
                Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                Ok(full) if full.is_dir() => {
                    reply_err(&mut writer, format!("{path} is a directory"))?
                }
                Ok(full) => match std::fs::File::open(&full) {
                    Err(e) => reply_err(&mut writer, format!("opening {path}: {e}"))?,
                    Ok(mut f) => {
                        let size = f.metadata().map(|m| m.len()).unwrap_or(0);
                        // Same commit-to-the-stream contract as ReadFile: once
                        // FileBegin is on the wire a mid-stream error drops the
                        // connection rather than corrupting the chunk stream.
                        write_control(&mut writer, &Response::FileBegin { size })?;
                        let mut cw = ChunkWriter::new(&mut writer);
                        if let Err(e) = std::io::copy(&mut f, &mut cw) {
                            return Err(anyhow!("reading host file {path}: {e}"));
                        }
                        cw.finish()?;
                    }
                },
            },

            Request::OpenBlock { path } => match open_block(root, &path) {
                Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                Ok((file, size)) => {
                    let handle = next_handle;
                    next_handle += 1;
                    block_handles.insert(handle, file);
                    write_control(&mut writer, &Response::BlockOpened { handle, size })?;
                }
            },

            Request::ReadBlock {
                handle,
                offset,
                len,
            } => match block_handles.get_mut(&handle) {
                None => reply_err(&mut writer, format!("no such block handle {handle}"))?,
                Some(file) => match read_block(file, offset, len) {
                    Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                    Ok(buf) => {
                        // Commit to the stream: FileBegin{actual len}, then the
                        // bytes as one chunk frame (len is capped, so it fits).
                        write_control(
                            &mut writer,
                            &Response::FileBegin {
                                size: buf.len() as u64,
                            },
                        )?;
                        let mut cw = ChunkWriter::new(&mut writer);
                        if let Err(e) = cw.write_all(&buf) {
                            return Err(anyhow!("sending block of handle {handle}: {e}"));
                        }
                        cw.finish()?;
                    }
                },
            },

            Request::CloseBlock { handle } => {
                block_handles.remove(&handle);
                write_control(&mut writer, &Response::Ok)?;
            }

            Request::StageCopyLocal {
                session,
                src_image,
                src_partition,
                src_path,
                dest_parent,
                name,
                force,
            } => match sessions.get_mut(&session) {
                Some(sess) => {
                    match stage_copy_local(root, &src_image, src_partition, &src_path, sess) {
                        Ok((blob, size, type_code, creator_code)) => {
                            sess.edits.push(StagedEdit::AddFile {
                                dest_parent,
                                name,
                                blob,
                                size,
                                force,
                                type_code,
                                creator_code,
                            });
                            write_control(&mut writer, &Response::Ok)?;
                        }
                        Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                    }
                }
                None => reply_err(&mut writer, format!("no such session {session}"))?,
            },

            Request::Bye => return Ok(()),
        }
    }
}

/// Send an `Error` control frame.
fn reply_err<W: std::io::Write>(writer: &mut W, message: String) -> Result<()> {
    write_control(writer, &Response::Error { message })?;
    Ok(())
}

/// Read a chunk stream from the client into a staging blob file.
fn stage_blob<R: std::io::Read>(reader: &mut R, blob: &Path) -> Result<()> {
    let mut f = std::fs::File::create(blob)
        .with_context(|| format!("creating staging blob {}", blob.display()))?;
    // std::fs::File is unbuffered, so read_chunks' write_all lands every byte
    // before it returns — no explicit flush needed.
    read_chunks(reader, &mut f)?;
    Ok(())
}

/// An opened filesystem plus the metadata a remote pane displays.
struct OpenedFs {
    fs: Box<dyn Filesystem>,
    label: String,
    fs_type: String,
    volume_label: Option<String>,
    total_size: u64,
    used_size: u64,
}

/// Open `rel` (relative to `root`, partition `partition`) for reading exactly
/// as the local CLI would, returning the live filesystem + display metadata.
fn open_image(root: &Path, rel: &str, partition: Option<u32>) -> Result<OpenedFs> {
    let full = sandbox_join(root, rel)?;
    let (reader, ctx) = crate::cli::resolve::resolve_partition_streaming_forced_inside(
        &full, partition, None, None, None,
    )?;
    let fs = crate::fs::open_filesystem(
        reader,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem: {e}"))?;
    Ok(OpenedFs {
        fs_type: fs.fs_type().to_string(),
        volume_label: fs.volume_label().map(|s| s.to_string()),
        total_size: fs.total_size(),
        used_size: fs.used_size(),
        fs,
        label: ctx.label,
    })
}

/// Open a write session: sandbox + verify the target image exists; the editable
/// open is deferred to `Apply`.
fn open_session(
    root: &Path,
    rel: &str,
    partition: Option<u32>,
    staging_dir: Option<&Path>,
) -> Result<Session> {
    let image_path = sandbox_join(root, rel)?;
    let mut builder = tempfile::Builder::new();
    builder.prefix("rb-stage-");
    let staging = match staging_dir {
        Some(d) => builder
            .tempdir_in(d)
            .with_context(|| format!("creating staging dir under {}", d.display()))?,
        None => builder.tempdir().context("creating staging dir")?,
    };
    Ok(Session {
        image_path,
        partition,
        staging,
        edits: Vec::new(),
        blob_seq: 0,
    })
}

/// Replay a session's staged edits onto its image (open editable once, mutate,
/// sync, commit). Mirrors the local `put` / `mkdir` verbs.
fn apply_session(sess: &Session) -> Result<u64> {
    let (file, ctx, commit) =
        crate::cli::resolve::resolve_partition_rw_forced(&sess.image_path, sess.partition, None)?;
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let mut count = 0u64;
    for edit in &sess.edits {
        match edit {
            StagedEdit::AddFile {
                dest_parent,
                name,
                blob,
                size,
                force,
                type_code,
                creator_code,
            } => {
                let parent = resolve_path(&mut *fs, dest_parent)?;
                if !parent.is_directory() {
                    bail!("parent is not a directory: {dest_parent}");
                }
                if let Some(existing) = fs
                    .list_directory(&parent)
                    .map_err(|e| anyhow!("list_directory: {e}"))?
                    .into_iter()
                    .find(|e| &e.name == name)
                {
                    if !*force {
                        bail!("{} already exists (use --force)", disp(dest_parent, name));
                    }
                    fs.delete_entry(&parent, &existing)
                        .map_err(|e| anyhow!("delete existing {name}: {e}"))?;
                }
                let options = CreateFileOptions {
                    type_code: type_code.clone(),
                    creator_code: creator_code.clone(),
                    ..Default::default()
                };
                let mut blobf = std::fs::File::open(blob)
                    .with_context(|| format!("opening staging blob {}", blob.display()))?;
                fs.create_file(&parent, name, &mut blobf, *size, &options)
                    .map_err(|e| anyhow!("create_file {name}: {e}"))?;
                count += 1;
            }
            StagedEdit::Mkdir { parent, name } => {
                let parent_entry = resolve_path(&mut *fs, parent)?;
                if !parent_entry.is_directory() {
                    bail!("parent is not a directory: {parent}");
                }
                if fs
                    .list_directory(&parent_entry)
                    .map_err(|e| anyhow!("list_directory: {e}"))?
                    .iter()
                    .any(|e| &e.name == name)
                {
                    bail!("{} already exists", disp(parent, name));
                }
                fs.create_directory(&parent_entry, name, &CreateDirectoryOptions::default())
                    .map_err(|e| anyhow!("create_directory {name}: {e}"))?;
                count += 1;
            }
        }
    }

    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;
    Ok(count)
}

/// Stage an on-device copy: extract `src_path` from the source image into a
/// staging blob in the session's staging dir, returning the blob + the source
/// file's size and type/creator (preserved for HFS-style filesystems). The blob
/// lives on the daemon, so nothing round-trips through the desktop.
fn stage_copy_local(
    root: &Path,
    src_rel: &str,
    src_partition: Option<u32>,
    src_path: &str,
    sess: &mut Session,
) -> Result<(PathBuf, u64, Option<String>, Option<String>)> {
    let mut opened = open_image(root, src_rel, src_partition)?;
    let entry = resolve_path(&mut *opened.fs, src_path)?;
    if entry.is_directory() {
        bail!("{src_path} is a directory (recursive copy over rb:// isn't supported yet)");
    }
    let blob = sess.staging.path().join(format!("blob-{}", sess.blob_seq));
    sess.blob_seq += 1;
    let mut f = std::fs::File::create(&blob)
        .with_context(|| format!("creating staging blob {}", blob.display()))?;
    opened
        .fs
        .write_file_to(&entry, &mut f)
        .map_err(|e| anyhow!("reading {src_path}: {e}"))?;
    Ok((
        blob,
        entry.size,
        entry.type_code.clone(),
        entry.creator_code.clone(),
    ))
}

/// Largest single `ReadBlock` we honour — bounds one block-reader fetch so a
/// hostile `len` can't make us allocate unbounded. The reader fetches in windows
/// well under this; a larger ask is simply clamped.
const MAX_RANGE_READ: u32 = 4 * 1024 * 1024;

/// Open a host file as a raw block device (sandboxed): returns the open handle
/// plus its length, kept open by the caller for the session's ranged reads.
fn open_block(root: &Path, rel: &str) -> Result<(std::fs::File, u64)> {
    let full = sandbox_join(root, rel)?;
    if full.is_dir() {
        bail!("{rel} is a directory");
    }
    let f = std::fs::File::open(&full).with_context(|| format!("opening {}", full.display()))?;
    let size = f.metadata().map(|m| m.len()).unwrap_or(0);
    Ok((f, size))
}

/// Read up to `len` bytes from an open block handle at `offset`. A read at/after
/// EOF returns an empty/short buffer rather than erroring — the block reader
/// treats that as EOF.
fn read_block(file: &mut std::fs::File, offset: u64, len: u32) -> Result<Vec<u8>> {
    let size = file.metadata().map(|m| m.len()).unwrap_or(0);
    if offset >= size {
        return Ok(Vec::new());
    }
    let want = len.min(MAX_RANGE_READ) as u64;
    let avail = (size - offset).min(want) as usize;
    file.seek(SeekFrom::Start(offset))
        .with_context(|| format!("seeking block handle to {offset}"))?;
    let mut buf = vec![0u8; avail];
    file.read_exact(&mut buf)
        .with_context(|| format!("reading {avail} bytes at {offset}"))?;
    Ok(buf)
}

/// List a directory on the host filesystem (sandboxed to the serve root).
fn list_host_dir(root: &Path, rel: &str) -> Result<Vec<WireEntry>> {
    let dir = sandbox_dir(root, rel)?;
    if !dir.is_dir() {
        bail!("{rel} is not a directory");
    }
    let rel_disp = rel.trim_end_matches('/');
    let mut out = Vec::new();
    for ent in
        std::fs::read_dir(&dir).with_context(|| format!("reading host dir {}", dir.display()))?
    {
        let ent = ent?;
        let name = ent.file_name().to_string_lossy().to_string();
        let ft = ent.file_type()?;
        let kind = if ft.is_dir() {
            WireKind::Dir
        } else if ft.is_symlink() {
            WireKind::Symlink
        } else {
            WireKind::File
        };
        let size = ent.metadata().map(|m| m.len()).unwrap_or(0);
        let path = if rel_disp.is_empty() {
            format!("/{name}")
        } else {
            format!("{rel_disp}/{name}")
        };
        out.push(WireEntry {
            name,
            path,
            kind,
            size,
            type_code: None,
            creator_code: None,
            symlink_target: None,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// Classify a host path: `(exists, is_dir)`. A path that escapes the serve root
/// is an error; a missing path is `(false, false)`.
fn host_stat(root: &Path, rel: &str) -> Result<(bool, bool)> {
    let rel = rel.trim_start_matches('/');
    let joined = if rel.is_empty() {
        root.to_path_buf()
    } else {
        root.join(rel)
    };
    match joined.canonicalize() {
        Ok(canon) => {
            if !canon.starts_with(root) {
                bail!("path {rel:?} escapes the serve root");
            }
            Ok((true, canon.is_dir()))
        }
        Err(_) => Ok((false, false)),
    }
}

/// Like `sandbox_join` but allows the root itself (empty `rel`) — for browsing
/// host directories rather than opening an image file.
fn sandbox_dir(root: &Path, rel: &str) -> Result<PathBuf> {
    let rel = rel.trim_start_matches('/');
    let joined = if rel.is_empty() {
        root.to_path_buf()
    } else {
        root.join(rel)
    };
    let canon = joined
        .canonicalize()
        .with_context(|| format!("resolving host path {rel:?} under serve root"))?;
    if !canon.starts_with(root) {
        bail!("path {rel:?} escapes the serve root");
    }
    Ok(canon)
}

/// Resolve a wire path under the serve root and refuse anything that escapes
/// it (`..`, absolute paths, symlinks out). The wire path is slash-separated
/// with a leading `/`; we treat it as relative to `root`.
fn sandbox_join(root: &Path, rel: &str) -> Result<PathBuf> {
    let rel = rel.trim_start_matches('/');
    if rel.is_empty() {
        bail!("no image path given");
    }
    let joined = root.join(rel);
    let canon = joined
        .canonicalize()
        .with_context(|| format!("resolving {rel:?} under serve root"))?;
    if !canon.starts_with(root) {
        bail!("path {rel:?} escapes the serve root");
    }
    Ok(canon)
}

/// Join a parent dir + name for display, avoiding a doubled slash at root.
fn disp(parent: &str, name: &str) -> String {
    if parent.ends_with('/') {
        format!("{parent}{name}")
    } else {
        format!("{parent}/{name}")
    }
}

/// List a directory inside an opened image, projected to wire entries.
fn list_dir(fs: &mut dyn Filesystem, path: &str) -> Result<Vec<WireEntry>> {
    let entry = resolve_path(fs, path)?;
    if !entry.is_directory() {
        bail!("{path} is not a directory");
    }
    let children = fs
        .list_directory(&entry)
        .map_err(|e| anyhow!("list_directory: {e}"))?;
    Ok(children.iter().map(WireEntry::from_entry).collect())
}
