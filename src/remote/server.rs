//! The `rb-cli serve` daemon — Family F, read-only (Phase 0).
//!
//! Blocking, thread-per-connection (`std::net`, no async runtime — matches the
//! repo's `std::thread` idiom and keeps the slim build lean). Each connection
//! owns its own handle table, so no shared state / locking is needed.
//!
//! The daemon runs **the exact same pipeline the local CLI runs** —
//! `resolve_partition_streaming_forced_inside` → `open_filesystem` →
//! `list_directory` / `write_file_to` — so a remote browse behaves identically
//! to a local one. The client never sees raw blocks; it asks for directory
//! listings and file bytes at the operation level (plan §2.2).

use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};

use crate::cli::verbs::ls::resolve_path;
use crate::fs::filesystem::Filesystem;
use crate::remote::protocol::{
    read_control, write_control, ChunkWriter, Request, Response, WireEntry, CAP_FAMILY_F,
    MIN_PROTOCOL_VERSION, PROTOCOL_VERSION, RB_HELLO_MAGIC,
};

/// Daemon configuration (from the `serve` verb).
pub struct ServeConfig {
    /// Bind address, e.g. `0.0.0.0:7341`.
    pub bind: String,
    /// Root directory images are served from; every opened path is sandboxed
    /// under it.
    pub root: PathBuf,
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
    eprintln!("rb-cli serve: Family F read-only (Phase 0 spike). Ctrl-C to stop.");

    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let root = root.clone();
                std::thread::spawn(move || {
                    let peer = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "?".into());
                    eprintln!("rb-cli serve: {peer} connected");
                    match handle_conn(stream, &root) {
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
fn handle_conn(stream: TcpStream, root: &Path) -> Result<()> {
    stream.set_nodelay(true).ok();
    let mut reader = BufReader::new(stream.try_clone().context("cloning socket")?);
    let mut writer = BufWriter::new(stream);
    let mut handles: HashMap<u64, Box<dyn Filesystem>> = HashMap::new();
    let mut next_handle: u64 = 1;

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
                    write_control(
                        &mut writer,
                        &Response::Error {
                            message: format!(
                                "unsupported client (magic {magic:#x}, version {version})"
                            ),
                        },
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
                Ok((fs, label)) => {
                    let handle = next_handle;
                    next_handle += 1;
                    handles.insert(handle, fs);
                    write_control(&mut writer, &Response::Opened { handle, label })?;
                }
                Err(e) => write_control(
                    &mut writer,
                    &Response::Error {
                        message: format!("{e:#}"),
                    },
                )?,
            },

            Request::ListDir { handle, path } => match handles.get_mut(&handle) {
                Some(fs) => match list_dir(&mut **fs, &path) {
                    Ok(entries) => write_control(&mut writer, &Response::Dir { entries })?,
                    Err(e) => write_control(
                        &mut writer,
                        &Response::Error {
                            message: format!("{e:#}"),
                        },
                    )?,
                },
                None => write_control(
                    &mut writer,
                    &Response::Error {
                        message: format!("no such handle {handle}"),
                    },
                )?,
            },

            Request::ReadFile { handle, path } => match handles.get_mut(&handle) {
                None => write_control(
                    &mut writer,
                    &Response::Error {
                        message: format!("no such handle {handle}"),
                    },
                )?,
                Some(fs) => match resolve_path(&mut **fs, &path) {
                    Err(e) => write_control(
                        &mut writer,
                        &Response::Error {
                            message: format!("{e:#}"),
                        },
                    )?,
                    Ok(entry) if entry.is_directory() => write_control(
                        &mut writer,
                        &Response::Error {
                            message: format!("{path} is a directory"),
                        },
                    )?,
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

            Request::Bye => return Ok(()),
        }
    }
}

/// Open `rel` (relative to `root`, partition `partition`) exactly as the local
/// CLI would, returning the live filesystem + the CLI's partition label.
fn open_image(
    root: &Path,
    rel: &str,
    partition: Option<u32>,
) -> Result<(Box<dyn Filesystem>, String)> {
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
    Ok((fs, ctx.label))
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
