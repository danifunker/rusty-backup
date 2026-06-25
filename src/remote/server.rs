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

use serde::{Deserialize, Serialize};

use crate::cli::verbs::ls::resolve_path;
use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions, Filesystem};
use crate::remote::protocol::{
    read_chunk_header, read_chunks, read_control, read_family_b_op, read_handshake,
    read_member_header, read_member_request, write_binary_hello, write_control,
    write_get_open_reply, write_member_stream, write_put_ack, write_put_result, write_resume_map,
    ChunkWriter, FamilyBOp, Handshake, PutHeader, Request, Response, ResumeEntry, WireEntry,
    WireKind, CAP_FAMILY_B, CAP_FAMILY_F, MAX_PUT_CHUNK, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION,
    RB_HELLO_MAGIC,
};

/// A block-tier handle: a host image file (or raw device) kept open for ranged
/// reads — and, when `writable`, in-place ranged writes (remote editing). The
/// stored `size` is the byte length captured at open: `metadata().len()` for a
/// regular file, the ioctl size for a device (whose `metadata().len()` is 0).
struct BlockHandle {
    file: std::fs::File,
    size: u64,
    writable: bool,
}

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

/// Handle a binary Family-B client (cb-dos / `CRUSTYBK.EXE`).
///
/// **Phases 7b–7e.** The opening binary `Hello` has already been read; we reply
/// with our version + capabilities, then read the next frame: the peer hangs up
/// (a bare hello-and-quit probe like `NETHELLO` → EOF, a clean close), streams a
/// chunk **PUT** (`backup rb://…` → a `.cbk` on the host, [`receive_put`]), or
/// issues a **GET** (`restore rb://…` → pull a `.cbk`'s members back to rebuild a
/// disk, [`serve_get`]).
fn handle_family_b(
    mut reader: BufReader<TcpStream>,
    mut writer: BufWriter<TcpStream>,
    client_version: u16,
    client_caps: u16,
    root: &Path,
    staging_dir: Option<&Path>,
) -> Result<()> {
    eprintln!(
        "rb-cli serve: Family-B client connected (version {client_version}, caps {client_caps:#06x})"
    );
    // The chunk PUT protocol (7b) exists now, so advertise Family B alongside F.
    write_binary_hello(&mut writer, PROTOCOL_VERSION, CAP_FAMILY_F | CAP_FAMILY_B)
        .context("writing Family-B hello reply")?;

    match read_family_b_op(&mut reader).context("reading Family-B op")? {
        // Hello-only client (e.g. NETHELLO): clean disconnect, nothing to do.
        None => Ok(()),
        Some(FamilyBOp::Put(hdr)) => receive_put(&mut reader, &mut writer, root, staging_dir, hdr),
        Some(FamilyBOp::Get { name }) => serve_get(&mut reader, &mut writer, root, &name),
    }
}

/// Serve a GET (restore over the wire): open `<root>/<name>.cbk`, advertise its
/// members, then stream whichever member the client asks for until it sends the
/// done marker. A `partition-N.gz` member is served as its **raw gzip bytes** (the
/// client inflates); a Raw member (`metadata.json`/`mbr.bin`) is served
/// **decompressed** (its original file bytes). Read-only — no resume state.
fn serve_get(
    reader: &mut BufReader<TcpStream>,
    writer: &mut BufWriter<TcpStream>,
    root: &Path,
    name: &str,
) -> Result<()> {
    let cbk = cbk_dest_path(root, name)?;
    let members = match crate::rbformats::cbk::read_cbk_index(&cbk) {
        Ok(m) => m,
        Err(e) => {
            eprintln!(
                "rb-cli serve: GET {name:?} -> cannot open {}: {e:#}",
                cbk.display()
            );
            write_get_open_reply(writer, None)?;
            return Ok(());
        }
    };
    let names: Vec<String> = members.iter().map(|m| m.name.clone()).collect();
    eprintln!(
        "rb-cli serve: GET {name:?} -> serving {} member(s) from {}",
        names.len(),
        cbk.display()
    );
    write_get_open_reply(writer, Some(&names))?;

    while let Some(want) = read_member_request(reader)? {
        match members.iter().find(|m| m.name == want) {
            None => {
                eprintln!("rb-cli serve: GET {name:?} member {want:?} not found");
                write_member_stream(writer, false, std::io::empty())?;
            }
            Some(member) => {
                // A `*.gz` member is sent verbatim (its chunk payloads ARE the gz);
                // any other member is gunzipped to its original file bytes.
                if want.to_ascii_lowercase().ends_with(".gz") {
                    let src = crate::rbformats::cbk::CbkPayloadReader::open(&cbk, member)
                        .with_context(|| format!("opening member {want}"))?;
                    write_member_stream(writer, true, src)?;
                } else {
                    let src = crate::rbformats::cbk::cbk_member_content_reader(&cbk, member)
                        .with_context(|| format!("opening member {want}"))?;
                    write_member_stream(writer, true, src)?;
                }
            }
        }
    }
    Ok(())
}

/// Durable resume state for an in-progress PUT (the ddrescue-mapfile analog,
/// §3). Lives as `journal.json` in the persistent staging dir; rewritten
/// atomically after each chunk's bytes are fsynced (fsync-data-then-record), so
/// a crash can never mark a chunk committed that isn't on disk. Only **Gz**
/// members (partitions) are tracked — Raw members (`mbr.bin`/`metadata.json`)
/// are tiny and re-sent fresh on every (re)connect.
#[derive(Serialize, Deserialize, Default)]
struct ResumeJournal {
    /// The §4 source fingerprint this staging belongs to. A reconnect with a
    /// different fingerprint means a swapped/edited card → discard and restart.
    fingerprint: u32,
    members: Vec<MemberProgress>,
}

#[derive(Serialize, Deserialize, Clone)]
struct MemberProgress {
    name: String,
    total_chunks: u32,
    committed_chunks: u32,
    committed_bytes: u64,
}

impl ResumeJournal {
    fn get(&self, name: &str) -> Option<&MemberProgress> {
        self.members.iter().find(|m| m.name == name)
    }
    fn upsert(&mut self, name: &str, total: u32, committed: u32, bytes: u64) {
        if let Some(m) = self.members.iter_mut().find(|m| m.name == name) {
            m.total_chunks = total;
            m.committed_chunks = committed;
            m.committed_bytes = bytes;
        } else {
            self.members.push(MemberProgress {
                name: name.to_string(),
                total_chunks: total,
                committed_chunks: committed,
                committed_bytes: bytes,
            });
        }
    }
}

fn journal_path(staging: &Path) -> PathBuf {
    staging.join("journal.json")
}

fn load_journal(staging: &Path) -> Option<ResumeJournal> {
    let data = std::fs::read(journal_path(staging)).ok()?;
    serde_json::from_slice(&data).ok()
}

/// Write the journal atomically (write `.tmp`, fsync, rename) so a crash leaves
/// either the old or the new map, never a torn one.
fn save_journal(staging: &Path, j: &ResumeJournal) -> Result<()> {
    let p = journal_path(staging);
    let tmp = staging.join("journal.json.tmp");
    std::fs::write(&tmp, serde_json::to_vec(j)?).context("writing resume journal")?;
    if let Ok(f) = std::fs::File::open(&tmp) {
        f.sync_all().ok();
    }
    std::fs::rename(&tmp, &p).context("committing resume journal")?;
    Ok(())
}

/// The stable, persistent staging dir for a container `name` (so a reconnect
/// finds the partial transfer). The name is already validated as a flat base.
fn staging_path_for(staging_dir: Option<&Path>, name: &str) -> PathBuf {
    let base = staging_dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(std::env::temp_dir);
    base.join(format!("rb-cbk-{name}"))
}

/// Open a staging member for append, first truncating to `committed_bytes` — so
/// a half-written trailing chunk from a crash (bytes fsynced but the journal not
/// yet updated) is discarded and the client re-sends it. Returns the file seeked
/// to the (new) end.
fn open_staging_for_append(path: &Path, committed_bytes: u64) -> Result<std::fs::File> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("opening staging member {}", path.display()))?;
    f.set_len(committed_bytes).with_context(|| {
        format!(
            "truncating staging member {} to last commit",
            path.display()
        )
    })?;
    f.seek(SeekFrom::Start(committed_bytes))?;
    Ok(f)
}

/// Receive a chunk PUT and assemble it into a frozen `.cbk` under the serve root,
/// resumably (§3). A persistent per-container staging dir + `journal.json` is the
/// durable assembly point: each Gz chunk's bytes are fsynced **before** the
/// journal records it committed, and a reconnect (same `name` + fingerprint)
/// truncates each member to its last committed chunk and tells the client where
/// to resume. At finalize the daemon fills each partition's real checksum (it
/// owns the bytes — the rebooted client can't recompute a CRC across a resume),
/// packs with the frozen `pack_folder_to_cbk`, and drops the staging.
fn receive_put(
    reader: &mut BufReader<TcpStream>,
    writer: &mut BufWriter<TcpStream>,
    root: &Path,
    staging_dir: Option<&Path>,
    hdr: PutHeader,
) -> Result<()> {
    let dest = cbk_dest_path(root, &hdr.name)?;
    let staging = staging_path_for(staging_dir, &hdr.name);

    // Load durable state, or discard it on a fingerprint mismatch (the source
    // disk changed since the partial transfer — splicing would corrupt §4b).
    let mut journal = match load_journal(&staging) {
        Some(j) if j.fingerprint == hdr.fingerprint => {
            eprintln!(
                "rb-cli serve: PUT {:?} resuming (fingerprint {:#010x} matches, {} member(s) in progress)",
                hdr.name,
                hdr.fingerprint,
                j.members.len()
            );
            j
        }
        Some(_) => {
            eprintln!(
                "rb-cli serve: PUT {:?} fingerprint changed — discarding stale staging, starting fresh",
                hdr.name
            );
            std::fs::remove_dir_all(&staging).ok();
            ResumeJournal {
                fingerprint: hdr.fingerprint,
                members: Vec::new(),
            }
        }
        None => ResumeJournal {
            fingerprint: hdr.fingerprint,
            members: Vec::new(),
        },
    };
    std::fs::create_dir_all(&staging)
        .with_context(|| format!("creating PUT staging dir {}", staging.display()))?;

    // Tell the client what's already committed (Gz members only).
    let entries: Vec<ResumeEntry> = journal
        .members
        .iter()
        .map(|m| ResumeEntry {
            name: m.name.clone(),
            committed_chunks: m.committed_chunks,
        })
        .collect();
    write_resume_map(writer, &entries).context("writing resume map")?;

    eprintln!(
        "rb-cli serve: PUT {:?} ({} member{}) -> {}",
        hdr.name,
        hdr.member_count,
        if hdr.member_count == 1 { "" } else { "s" },
        dest.display()
    );

    // Test-only fault injection: drop the connection after N Gz chunks are
    // durably committed, to exercise the resume path deterministically. Set
    // `RB_SERVE_TEST_DROP_AFTER_CHUNKS=N`. Never set in production.
    let drop_after: Option<u64> = std::env::var("RB_SERVE_TEST_DROP_AFTER_CHUNKS")
        .ok()
        .and_then(|s| s.parse().ok());
    let mut gz_committed: u64 = 0;

    for mi in 0..hdr.member_count {
        let m =
            read_member_header(reader).with_context(|| format!("reading member {mi} header"))?;
        validate_member_name(&m.name)?;
        let path = staging.join(&m.name);
        let is_gz = m.kind == 0;

        // Resume applies to Gz (partition) members only; Raw members are re-sent
        // fresh each session (cheap), so they always start from chunk 0.
        let prior = if is_gz { journal.get(&m.name) } else { None };
        let committed = prior.map(|p| p.committed_chunks).unwrap_or(0);
        let committed_bytes = prior.map(|p| p.committed_bytes).unwrap_or(0);

        let mut f = open_staging_for_append(&path, committed_bytes)?;
        let mut written = committed_bytes;

        for ci in committed..m.chunk_count {
            let ch = read_chunk_header(reader)
                .with_context(|| format!("reading {} chunk {ci} header", m.name))?;
            if ch.len > MAX_PUT_CHUNK {
                write_put_ack(writer, false).ok();
                bail!(
                    "PUT chunk on {} is {} bytes (over the {MAX_PUT_CHUNK}-byte limit)",
                    m.name,
                    ch.len
                );
            }
            // Stream the payload into the staging file, hashing as we go.
            let mut hasher = crc32fast::Hasher::new();
            let mut remaining = ch.len as usize;
            let mut buf = vec![0u8; 256 * 1024];
            while remaining > 0 {
                let want = remaining.min(buf.len());
                reader
                    .read_exact(&mut buf[..want])
                    .with_context(|| format!("reading {} chunk payload", m.name))?;
                hasher.update(&buf[..want]);
                f.write_all(&buf[..want])
                    .with_context(|| format!("writing staging member {}", m.name))?;
                remaining -= want;
            }
            let got = hasher.finalize();
            if got != ch.crc32 {
                write_put_ack(writer, false).ok();
                bail!(
                    "PUT chunk CRC mismatch on {} (wire {:#010x} != computed {got:#010x})",
                    m.name,
                    ch.crc32
                );
            }
            written += ch.len as u64;
            // fsync the staged bytes, THEN record the commit, THEN ack — so the
            // ack (and the resume cursor) only ever advance past durable data.
            f.flush().ok();
            f.sync_all()
                .with_context(|| format!("fsync staging member {}", m.name))?;
            if is_gz {
                journal.upsert(&m.name, m.chunk_count, ci + 1, written);
                save_journal(&staging, &journal)?;
            }
            write_put_ack(writer, true)?;
            if is_gz {
                gz_committed += 1;
                if drop_after == Some(gz_committed) {
                    eprintln!(
                        "rb-cli serve: TEST drop after {gz_committed} committed chunk(s) — resume to continue"
                    );
                    bail!("test-induced drop after {gz_committed} committed chunks");
                }
            }
        }
    }

    // All members complete. Fill the partition checksums (the daemon owns the
    // bytes — authoritative across a resume), drop the journal so it isn't
    // packed, then pack atomically and clear the staging.
    fill_partition_checksums(&staging)?;
    std::fs::remove_file(journal_path(&staging)).ok();
    std::fs::remove_file(staging.join("journal.json.tmp")).ok();

    let tmp = dest.with_file_name(format!(
        "{}.tmp",
        dest.file_name().unwrap_or_default().to_string_lossy()
    ));
    crate::rbformats::cbk::pack_folder_to_cbk(&staging, &tmp)
        .with_context(|| format!("packing PUT into {}", tmp.display()))?;
    if let Ok(f) = std::fs::File::open(&tmp) {
        f.sync_all().ok();
    }
    std::fs::rename(&tmp, &dest)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), dest.display()))?;
    std::fs::remove_dir_all(&staging).ok();
    let cbk_size = std::fs::metadata(&dest).map(|m| m.len()).unwrap_or(0);

    eprintln!(
        "rb-cli serve: PUT complete -> {} ({cbk_size} bytes)",
        dest.display()
    );
    write_put_result(writer, 0, cbk_size)?;
    Ok(())
}

/// `partition-N.gz` → `Some(N)`; anything else (`.gz.crc32`, `.idx`, `mbr.bin`,
/// `metadata.json`) → `None`.
fn partition_gz_index(name: &str) -> Option<usize> {
    let stem = name.strip_prefix("partition-")?.strip_suffix(".gz")?;
    stem.parse().ok()
}

/// CRC32 of a file's bytes, streamed.
fn crc32_of_file(path: &Path) -> Result<u32> {
    let mut f = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

/// Fill each `partition-N.gz` member's real checksum into the staged
/// `metadata.json`. The networked producer ships a placeholder (it can't compute
/// the gz CRC across a resume — it never holds the whole compressed member), so
/// the daemon, which does hold every byte, computes it here. Best-effort: a
/// `metadata.json` that isn't a parseable backup manifest is left untouched (the
/// checksum is informational — restore doesn't re-verify it).
fn fill_partition_checksums(staging: &Path) -> Result<()> {
    let meta_path = staging.join("metadata.json");
    let Ok(data) = std::fs::read_to_string(&meta_path) else {
        return Ok(());
    };
    let mut metadata: crate::backup::metadata::BackupMetadata = match serde_json::from_str(&data) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("rb-cli serve: metadata.json not a fillable manifest ({e}); leaving checksums as-is");
            return Ok(());
        }
    };
    let mut changed = false;
    for entry in
        std::fs::read_dir(staging).with_context(|| format!("reading {}", staging.display()))?
    {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(idx) = partition_gz_index(&name) {
            let crc = crc32_of_file(&entry.path())?;
            if let Some(p) = metadata.partitions.iter_mut().find(|p| p.index == idx) {
                p.checksum = format!("{crc:08x}");
                changed = true;
            }
        }
    }
    if changed {
        std::fs::write(&meta_path, serde_json::to_vec_pretty(&metadata)?)
            .with_context(|| format!("rewriting {}", meta_path.display()))?;
    }
    Ok(())
}

/// The destination `.cbk` path for a PUT, under the serve root. The container
/// name is a flat base (a `.cbk` suffix is optional); anything with a path
/// separator or traversal is refused so a client can't escape the root.
fn cbk_dest_path(root: &Path, name: &str) -> Result<PathBuf> {
    if name.is_empty() {
        bail!("PUT container name is empty");
    }
    if name.contains('/') || name.contains('\\') || name.contains("..") || name.contains('\0') {
        bail!("PUT container name {name:?} is not a flat base name");
    }
    let file = if name.to_ascii_lowercase().ends_with(".cbk") {
        name.to_string()
    } else {
        format!("{name}.cbk")
    };
    Ok(root.join(file))
}

/// Reject an unsafe member name (path separators / traversal / NUL) before it's
/// used as a staging file name — mirrors the guard `materialize_cbk_to_folder`
/// applies on the way out.
fn validate_member_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || name.contains('\0')
    {
        bail!("PUT member name {name:?} is unsafe");
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
    // Block tier: host image files (and raw physical devices) kept open for
    // ranged reads (and in-place writes when opened read-write). Shares the
    // `next_handle` counter so handles are unique across both tables.
    let mut block_handles: HashMap<u64, BlockHandle> = HashMap::new();
    let mut sessions: HashMap<u64, Session> = HashMap::new();
    let mut next_session: u64 = 1;

    // The opening frame disambiguates a JSON-free Family-B client (cb-dos —
    // leads with the binary magic) from a JSON Family-F client (desktop). A
    // binary client diverges entirely into the Family-B handler; a JSON client
    // seeds the first request into the existing operation loop.
    let mut pending: Option<Request> = match read_handshake(&mut reader) {
        Ok(Handshake::Binary {
            version,
            capabilities,
        }) => {
            if version < MIN_PROTOCOL_VERSION {
                // Can't speak the JSON error path to a binary client; just log
                // and drop. The reply omits any capability so the client sees
                // the mismatch (its own version is above ours).
                eprintln!(
                    "rb-cli serve: rejecting Family-B client (version {version} < {MIN_PROTOCOL_VERSION})"
                );
                return Ok(());
            }
            return handle_family_b(reader, writer, version, capabilities, root, staging_dir);
        }
        // Peer hung up before sending anything — a clean end, not an error.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
        Err(e) => return Err(e).context("reading handshake"),
        Ok(Handshake::Json(req)) => Some(req),
    };

    loop {
        let req: Request = match pending.take() {
            Some(r) => r,
            None => match read_control(&mut reader) {
                Ok(r) => r,
                // Peer hung up between requests — a clean end, not an error.
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e).context("reading request"),
            },
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
                    block_handles.insert(
                        handle,
                        BlockHandle {
                            file,
                            size,
                            writable: false,
                        },
                    );
                    write_control(&mut writer, &Response::BlockOpened { handle, size })?;
                }
            },

            Request::OpenBlockRw { path } => match open_block_rw(root, &path) {
                Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                Ok((file, size)) => {
                    let handle = next_handle;
                    next_handle += 1;
                    block_handles.insert(
                        handle,
                        BlockHandle {
                            file,
                            size,
                            writable: true,
                        },
                    );
                    write_control(&mut writer, &Response::BlockOpened { handle, size })?;
                }
            },

            Request::ListDevices => {
                let devices = crate::os::enumerate_devices()
                    .iter()
                    .map(crate::remote::protocol::WireDevice::from_device)
                    .collect();
                write_control(&mut writer, &Response::Devices { devices })?;
            }

            Request::OpenDevice { path } => match open_device(&path) {
                Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                Ok((file, size)) => {
                    let handle = next_handle;
                    next_handle += 1;
                    // Devices are served read-only — restore-to-remote-device is
                    // a separate, deliberate path, not implied by a backup open.
                    block_handles.insert(
                        handle,
                        BlockHandle {
                            file,
                            size,
                            writable: false,
                        },
                    );
                    write_control(&mut writer, &Response::BlockOpened { handle, size })?;
                }
            },

            Request::ReadBlock {
                handle,
                offset,
                len,
            } => match block_handles.get_mut(&handle) {
                None => reply_err(&mut writer, format!("no such block handle {handle}"))?,
                Some(bh) => match read_block(&mut bh.file, bh.size, offset, len) {
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

            Request::WriteBlock {
                handle,
                offset,
                len,
            } => {
                // The payload follows as a chunk stream and MUST be consumed
                // regardless of handle validity, or the framing desyncs.
                let mut payload = Vec::new();
                let drained = read_chunks(&mut reader, &mut payload);
                match drained {
                    Err(e) => return Err(anyhow!("reading WriteBlock payload: {e}")),
                    Ok(_) => match block_handles.get_mut(&handle) {
                        None => reply_err(&mut writer, format!("no such block handle {handle}"))?,
                        Some(bh) if !bh.writable => {
                            reply_err(&mut writer, format!("block handle {handle} is read-only"))?
                        }
                        Some(bh) => match write_block(&mut bh.file, bh.size, offset, len, &payload)
                        {
                            Ok(()) => write_control(&mut writer, &Response::Ok)?,
                            Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                        },
                    },
                }
            }

            Request::FlushBlock { handle } => match block_handles.get(&handle) {
                None => reply_err(&mut writer, format!("no such block handle {handle}"))?,
                Some(bh) if !bh.writable => {
                    reply_err(&mut writer, format!("block handle {handle} is read-only"))?
                }
                Some(bh) => match bh.file.sync_all() {
                    Ok(()) => write_control(&mut writer, &Response::Ok)?,
                    Err(e) => reply_err(&mut writer, format!("flushing block {handle}: {e}"))?,
                },
            },

            Request::CloseBlock { handle } => {
                // Sync a writable handle to disk on close as a safety net, so a
                // finished edit is durable even if the client never flushed.
                if let Some(bh) = block_handles.remove(&handle) {
                    if bh.writable {
                        bh.file.sync_all().ok();
                    }
                }
                write_control(&mut writer, &Response::Ok)?;
            }

            Request::OpenWriteTarget {
                path,
                is_device,
                size,
            } => match open_write_target(root, &path, is_device, size) {
                Err(e) => reply_err(&mut writer, format!("{e:#}"))?,
                Ok((file, actual_size)) => {
                    let handle = next_handle;
                    next_handle += 1;
                    block_handles.insert(
                        handle,
                        BlockHandle {
                            file,
                            size: actual_size,
                            writable: true,
                        },
                    );
                    write_control(
                        &mut writer,
                        &Response::BlockOpened {
                            handle,
                            size: actual_size,
                        },
                    )?;
                }
            },

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

/// Largest single `WriteBlock` payload we honour. The block writer splits big
/// writes into frames well under this; a larger `len` is rejected rather than
/// silently truncated, so a desync can't corrupt the image.
const MAX_RANGE_WRITE: u32 = 4 * 1024 * 1024;

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

/// Open a host file as a read-WRITE block device (sandboxed): returns the open
/// handle plus its length, kept open by the caller for in-place ranged writes
/// (remote editing). The file is opened read+write so the engine can both read
/// the existing on-disk structures and patch them; the size is fixed at open
/// (a block-tier edit never grows the file).
fn open_block_rw(root: &Path, rel: &str) -> Result<(std::fs::File, u64)> {
    let full = sandbox_join(root, rel)?;
    if full.is_dir() {
        bail!("{rel} is a directory");
    }
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&full)
        .with_context(|| format!("opening {} read-write", full.display()))?;
    let size = f.metadata().map(|m| m.len()).unwrap_or(0);
    Ok((f, size))
}

/// Open a **restore write target**: where the desktop pushes a finished disk
/// image via `WriteBlock`. For a device (`is_device`), validate against the
/// enumerated set and open it read-write (the daemon must run elevated); the
/// requested `size` is advisory and the device's real capacity is returned. For
/// an image file, create/truncate it under the serve root to exactly `size`
/// bytes and open it read-write. Destructive — restore overwrites the target.
fn open_write_target(
    root: &Path,
    path: &str,
    is_device: bool,
    size: u64,
) -> Result<(std::fs::File, u64)> {
    if is_device {
        // Same validation as the read-side `open_device`: the path must match a
        // live enumerated device exactly, so this can't write an arbitrary file.
        let devices = crate::os::enumerate_devices();
        let dev = devices
            .iter()
            .find(|d| d.path.to_string_lossy() == path)
            .ok_or_else(|| anyhow!("{path:?} is not an enumerated device on this machine"))?;
        let dev_path = dev.path.clone();
        // `open_target_for_writing` does the platform-appropriate prep (unmount /
        // lock); on Linux/MiSTer — the supported daemon target — it returns a
        // plain read-write File. (A Windows/macOS daemon would need to keep the
        // returned handle's volume locks alive; out of scope here.)
        let handle = crate::os::open_target_for_writing(&dev_path).with_context(|| {
            format!(
                "opening device {} for writing (the daemon may need to run as root)",
                dev_path.display()
            )
        })?;
        let dev_size = if dev.size_bytes > 0 {
            dev.size_bytes
        } else {
            crate::os::get_file_size(&handle.file, &dev_path).unwrap_or(0)
        };
        if dev_size == 0 {
            bail!("device {} reports zero size", dev_path.display());
        }
        if size > dev_size {
            bail!(
                "restore image ({size} bytes) is larger than device {} ({dev_size} bytes)",
                dev_path.display()
            );
        }
        Ok((handle.file, dev_size))
    } else {
        let full = sandbox_join_create(root, path)?;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&full)
            .with_context(|| format!("creating image target {}", full.display()))?;
        f.set_len(size)
            .with_context(|| format!("sizing image target {} to {size} bytes", full.display()))?;
        Ok((f, size))
    }
}

/// Write `payload` at `offset` into an open read-write block handle. The write
/// must lie wholly within the image (`offset + payload.len() <= size`) — a
/// block-tier edit patches existing bytes and never extends the file. `len` is
/// the client's declared length; it must match the bytes actually streamed and
/// stay under [`MAX_RANGE_WRITE`].
fn write_block(
    file: &mut std::fs::File,
    size: u64,
    offset: u64,
    len: u32,
    payload: &[u8],
) -> Result<()> {
    if len > MAX_RANGE_WRITE {
        bail!("write of {len} bytes exceeds the {MAX_RANGE_WRITE}-byte limit");
    }
    if payload.len() as u64 != len as u64 {
        bail!(
            "write payload ({} bytes) does not match declared length ({len})",
            payload.len()
        );
    }
    let end = offset
        .checked_add(payload.len() as u64)
        .ok_or_else(|| anyhow!("write offset {offset} + length overflows"))?;
    if end > size {
        bail!(
            "write [{offset}, {end}) extends past the image end ({size}) — \
             block-tier edits never grow the file"
        );
    }
    file.seek(SeekFrom::Start(offset))
        .with_context(|| format!("seeking block handle to {offset} for write"))?;
    file.write_all(payload)
        .with_context(|| format!("writing {} bytes at {offset}", payload.len()))?;
    Ok(())
}

/// Open a raw physical device for read-only ranged access.
///
/// The path is **not** sandbox-joined under the serve root — a device lives at
/// `/dev/...`, outside it. Instead the path must match one of the live
/// `enumerate_devices()` entries exactly, so this verb can only ever open a real
/// disk the daemon already advertises (it can't be turned into an arbitrary-file
/// read oracle that bypasses `--root`). The device's byte length comes from the
/// platform ioctl (`os::get_file_size`) because a block device reports
/// `metadata().len() == 0`. Read-only; opening fails cleanly if the daemon lacks
/// privilege (e.g. not running as root).
fn open_device(path: &str) -> Result<(std::fs::File, u64)> {
    let devices = crate::os::enumerate_devices();
    let dev = devices
        .iter()
        .find(|d| d.path.to_string_lossy() == path)
        .ok_or_else(|| anyhow!("{path:?} is not an enumerated device on this machine"))?;
    let dev_path = dev.path.clone();
    let f = std::fs::OpenOptions::new()
        .read(true)
        .open(&dev_path)
        .with_context(|| {
            format!(
                "opening device {} (the daemon may need to run as root)",
                dev_path.display()
            )
        })?;
    // Prefer the enumerated size; fall back to the ioctl probe if it's 0.
    let size = if dev.size_bytes > 0 {
        dev.size_bytes
    } else {
        crate::os::get_file_size(&f, &dev_path).unwrap_or(0)
    };
    if size == 0 {
        bail!("device {} reports zero size", dev_path.display());
    }
    Ok((f, size))
}

/// Read up to `len` bytes from an open block handle at `offset`. A read at/after
/// EOF returns an empty/short buffer rather than erroring — the block reader
/// treats that as EOF.
fn read_block(file: &mut std::fs::File, size: u64, offset: u64, len: u32) -> Result<Vec<u8>> {
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

/// Resolve a wire path under the serve root for a file that may **not exist
/// yet** (a restore target). Canonicalizes the parent directory (which must
/// exist and stay under the root) and appends the final component, so the
/// not-yet-created target still can't escape the sandbox via `..` or a symlink.
fn sandbox_join_create(root: &Path, rel: &str) -> Result<PathBuf> {
    let rel = rel.trim_start_matches('/');
    if rel.is_empty() {
        bail!("no target path given");
    }
    let joined = root.join(rel);
    let parent = joined
        .parent()
        .ok_or_else(|| anyhow!("target path {rel:?} has no parent directory"))?;
    let file_name = joined
        .file_name()
        .ok_or_else(|| anyhow!("target path {rel:?} has no file name"))?;
    let canon_parent = parent
        .canonicalize()
        .with_context(|| format!("resolving parent of {rel:?} under serve root"))?;
    if !canon_parent.starts_with(root) {
        bail!("target path {rel:?} escapes the serve root");
    }
    Ok(canon_parent.join(file_name))
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
