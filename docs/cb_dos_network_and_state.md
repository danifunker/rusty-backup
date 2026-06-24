# crusty-backup — network transport + disk-state model — scoping plan

Living scope + plan for **crusty-backup**'s (`cb-dos`) networked backup/restore and
its **disk-state / resume / change-detection** model. This is the companion to
[`cb_dos.md`](cb_dos.md) (the overall cb-dos scope); that doc deferred networking
to §6 / Phase 7 and left "incremental-ish" state tracking unspecified. This file
captures the design discussion that fleshed both out.

> **Unified-daemon note (2026-06-20):** the networked transport here is now
> **Family B** of the single `rb-cli serve` daemon described in the umbrella plan
> [`remote_transfer_plan.md`](remote_transfer_plan.md). The host listener is
> `rb-cli serve` (not a separate `net-serve` verb); cb-dos reaches it as a
> JSON-free Family-B client over a shared binary `Hello` handshake, and the same
> chunk-stream now also serves **desktop-pulls-remote-disk** and **restore-over-
> the-wire** (producer/consumer symmetry — see the umbrella §0, §7). The chunk
> container, resume map, fingerprint, and manifest specced below are unchanged and
> remain the source of truth for Family B's internals.

Two intertwined subjects live here:

1. **Transport** — how cb-dos moves the native backup folder to/from the modern
   desktop *over a wire* (not just removable media).
2. **Disk-state calculations** — how cb-dos and the desktop decide whether a
   transfer is *resumable*, whether a source disk is *unchanged*, what files are
   *volatile* (swap), and how backup↔restore stays *idempotent*.

These are coupled: resume, fingerprinting, the chunked container, and the file
manifest are all the same machinery viewed from different angles.

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[-]` dropped/out of scope

> **Nothing here is built yet.** This is a scoping document. It slots into
> `cb_dos.md`'s **Phase 7 (deferred)** and should not be started until the local
> removable-media round-trip (cb_dos Phases 2–4) proves the format.

---

## 0. Confirmed scope decisions

From the scoping discussion (2026-06-03):

| Decision | Choice |
|----------|--------|
| Transport family | **TCP/IP over a packet driver** — *not* raw L2 Ethernet, *not* serial/parallel |
| Why TCP (not raw L2 or UDP) | Reliable in-order stream → **no hand-rolled ACK/retransmit**; testable with stock tools (`nc`, Wireshark, loopback sockets); host becomes a **plain unprivileged socket** (no pcap/Npcap/BPF/root) |
| Stack on DOS | A small TCP/IP stack over the **Crynwr packet-driver API** (INT 60h). Reference / candidate-to-borrow: **mTCP** (GPLv3, AGPL-compatible) |
| Host listener home | **`rb-cli serve` daemon, Family B** (unified — was `net-serve`) — reuses format + restore code, scriptable, cross-platform |
| On-disk artifact | A **single chunked container** (`.cbk`), not a folder-of-files on the wire. Container doubles as the resume log; folder is materialized at finalize for the desktop restore path |
| Resume model | **ddrescue-style transfer map** of committed chunks; fsync-before-record ordering; truncate-to-last-committed on resume |
| Chunk unit | One **independently-compressed gzip member** covering a fixed span of *uncompressed source data* (~1–4 MB). Self-verifying (gzip CRC trailer); producer resumes by source-offset seek, never recompresses |
| "Same source?" gate | Cheap **composite fingerprint** (geometry + MBR/ptable CRC + volume serials + **allocation-bitmap CRC**), *not* a full-disk CRC |
| Change detection | **rsync-tier**: size + mtime + **archive bit**, per file, via a manifest. Best-effort gate; streamed per-member/per-partition CRC is the real integrity guarantee |
| Idempotency | Restore **replays** recorded mtime + attributes (incl. archive bit) so a fresh restore does **not** look like a fresh modification |
| Boot protection | Manifest fingerprints + round-trips MBR boot code, VBR/hidden sectors, and `IO.SYS`/`MSDOS.SYS`/`COMMAND.COM` |
| Swap files | **Excluded** from the change counter always; **zeroed** in the payload by default (`--keep-swap` to opt back in) |

Out of scope (for now): raw L2 Ethernet, serial, parallel; hand-rolled TCP
(borrow/port mTCP if we want TCP-the-library); whole-disk content CRC as a gate;
Level-2 swap deallocation (see §6).

---

## 1. Transport — TCP/IP over the packet driver

### 1a. Why TCP, and why over a packet driver

DOS networking layers as:

```
cb-dos transfer protocol
   TCP                ← reliable stream (borrowed or minimal)
   IP + ARP + ICMP
   packet driver      ← Crynwr INT 60h (send/recv raw frames)
   NIC
```

The packet-driver API (Russ Nelson / Crynwr) is the thin, universal L2 substrate
every DOS NIC ships a driver for, and the standard base for mTCP / WATTCP. It is
*not* what DOS games used — those were mostly **IPX/ODI** (Doom, Descent, etc.);
the packet driver is the TCP/IP-tools world.

**TCP over raw L2** was chosen because TCP's reliable, in-order byte stream means
we write **zero** sequencing/ACK/retransmit code, and — critically — it moves all
the painful verification problems off the host:

| | Raw L2 plan | TCP plan |
|---|---|---|
| Host privilege | pcap/Npcap, root, BPF perms | **plain unprivileged socket** |
| Wi-Fi | breaks (AP drops spoofed-MAC L2) | fine (routable) |
| 86Box test mode | **must** be bridged/pcap on a *wired* iface (SLiRP drops frames) | **SLiRP "just works"** via port-forward |
| Host test tooling | bespoke | `nc`/`socat`/Wireshark/loopback unit tests |
| Reliability layer | hand-rolled | free (TCP) |

The cost of going IP is paid on the **DOS** side (carry ARP/IP/TCP; the box needs
an IP — one `AUTOEXEC.BAT` line or DHCP). That trade — a bit more DOS code for a
trivial host and painless testing — is judged worth it.

### 1b. mTCP — reference, and borrow-vs-rewrite

mTCP (Michael Brutman, **GPLv3**, compatible with our AGPL-3.0) is the canonical
"small TCP/IP stack on a packet driver." It implements **both TCP and UDP** over
IP (ARP, IP fragments, DNS, DHCP client, listen/accept, Karn's-algorithm
retransmit, compile-in/out features for size). Docs:

- Home: <https://www.brutman.com/mTCP/mTCP.html>
- User docs PDF: <https://www.brutman.com/mTCP/download/mTCP_2024-10-20.pdf>
- FreeDOS setup guide: <http://help.fdos.org/en/hhstndrd/network/mtcp.htm>
- Source (unofficial fork w/ DOS build host): <https://github.com/retrohun/mTCP>

**Decision (2026-06-21) — WATT-32, not mTCP.** mTCP is **Open Watcom / C++**;
cb-dos is **DJGPP / C**, and the borrow-vs-port question (build mTCP under Watcom
and link, switch cb-dos to Watcom, or port mTCP's UDP/IP to DJGPP and hand-roll
reliability) all carried real cost. **WATT-32** (Gisle Vanem / Erick Engelke,
the Waterloo-TCP lineage) dissolves it: it builds **natively under DJGPP** and
exposes a **BSD-sockets API** (`socket`/`connect`/`send`/`recv`/`closesocket`)
over the same Crynwr packet driver. One toolchain, real TCP, no Watcom.

- **Acquisition:** the **prebuilt DJGPP binary package** (`wat3211b.zip` on the
  DJGPP mirrors) ships `libwatt.a` + headers — no cross-build of the stack from
  source. Fetched by [`../crusty-backup/net/fetch-watt32.sh`](../crusty-backup/net/fetch-watt32.sh)
  into `net/watt32/` (gitignored, like the mTCP apps); the `make net` target
  links `-Lnet/watt32/lib -lwatt`.
- **License:** WATT-32 is **BSD** (Regents of California). The historical
  advertising-acknowledgment clause is a **permitted additional term under
  AGPL-3.0 §7**, so it is compatible with this project's AGPL-3.0 — the license
  file travels with the library.
- **Startup:** `sock_init()` brings up the packet driver + IP (DHCP or static
  from `WATTCP.CFG`); blocking `recv`/`send` drive the stack internally — no
  hand-rolled tick loop in the client.
- Considered-and-rejected: **borrow mTCP** (Watcom toolchain split), **port
  mTCP UDP + hand-rolled reliability** (gives up free TCP), **hand-roll TCP**
  (a real project — the whole reason a stack exists). Reserve raw-L2/UDP only if
  a reason resurfaces.

mTCP's prebuilt **apps** (DHCP/FTP/PING) still ride the CD for manual/diagnostic
use ([`cb_dos_networking.md`](cb_dos_networking.md)); WATT-32 is specifically the
*library the streaming client links*.

### 1c. The DOS driver requirement (no way around *some* driver)

cb-dos does **not** eliminate the per-NIC driver — nothing in DOS can auto-drive
arbitrary NIC silicon. It requires a **packet driver** (Crynwr API), *not* NDIS
or ODI:

| Model | Used by | Us |
|---|---|---|
| **Packet driver** | mTCP, WATTCP, EtherDFS | **yes** — one small TSR, e.g. `NE2000 0x60 3 0x300` |
| NDIS 2.x | MS LAN Manager | no |
| ODI | NetWare/IPX | no |

NDIS/ODI-only cards can still feed us via a **shim** (`DIS_PKT.DOS` for NDIS,
`ODIPKT.COM` for ODI). The boot-media plan softens the burden: **bundle the
common packet drivers** (NE2000, 3C5x9, PCNTPK, RTSPKT…) + a boot-menu "Network"
profile that autodetects mainstream cards. The long tail loads its own driver
manually — documented, detected, refused gracefully if absent.

### 1d. Test rig — 86Box / DOSBox-X networking mode (gotcha)

The VM NIC **must** be in **PCap / bridged** mode (true L2 bridge via
libpcap/Npcap) — **not** SLiRP — for raw frames to reach the host. *However*,
because we chose **TCP/IP**, **SLiRP works too** (it NATs IP): set a port-forward
and the whole loop runs on the default emulator networking. Caveats if bridged:
- **Wired, not Wi-Fi** (Wi-Fi drivers drop bridged spoofed-MAC frames).
- Cleanest rig: host listener on a **second physical machine** on the same switch
  (removes same-host loopback ambiguity).
- macOS BPF / `/dev/bpf*` access for the emulator (and for pcap if ever used).

The emulated NE2000 / PCnet has a stock packet driver, so the **entire path is
exercisable on the Mac** before real 486 hardware.

### 1e. Security posture

Intended use is an **isolated host↔vintage-box LAN**. The protocol is
unauthenticated/unencrypted — a 486 can't afford crypto. Document "don't run on
an untrusted network"; do **not** bolt on encryption.

---

## 2. The chunked container (`.cbk`) — wire framing = on-disk artifact = resume log

The key idea: **separate the wire stream from the on-disk artifact**, then make
the on-disk artifact a single chunked container that *is* the resume journal.

### 2a. On the wire — one multiplexed chunk stream

Never N separate file transfers. One ordered sequence of chunks, each with a
small header:

```
chunk hdr: { magic, version, logical_id, src_offset, len, crc32 }
```

`logical_id` names the logical member (metadata / mbr / partition-0 / …). One
stream, one resume cursor, regardless of how many logical files exist.

### 2b. On disk — append-only container + index

The host writes one file, `MYDISK.cbk`, = chunks appended in arrival order plus a
trailer/sidecar index:

```
[chunk hdr][payload][chunk hdr][payload] ...   append-only log; payload = gzip member
[index/trailer]                                logical_id -> {chunk offsets, crc}
```

This one file does **triple duty**: wire framing, **resume log**, and deliverable.
A small `.idx` sidecar (updated via atomic rename) avoids rescanning a multi-GB
container on every resume.

**Bridge to the desktop restore path:** the desktop's resize-on-restore consumes
the **native PerPartition folder**, not `.cbk`. Two options:
- **(a) Materialize** the folder from the container at finalize (fast local
  untar). Lower effort, desktop unchanged. **Ship first.**
- **(b) Teach the desktop to read partitions directly out of `.cbk`** as a backup
  source. Nicer if the single file should be the permanent artifact. Later.

### 2c. The chunk = an independent, source-keyed gzip member

Instead of one continuous gzip stream per partition, emit the partition as a
**sequence of independent gzip members**, each covering a fixed span of
*uncompressed source data* (~1–4 MB; a bit bigger than the 400 KB first floated,
to balance ratio vs. fsync frequency vs. resume granularity).

Why this exact shape:
- **gzip is multi-member by spec** — concatenated members decompress as one file.
  `partition-1.gz` = member₀++member₁++… is valid gzip. **Desktop must decode with
  `flate2::MultiGzDecoder`, not `GzDecoder`** (the latter stops after member 0) —
  a one-line change in `compress.rs` alongside the planned `Gzip` codec.
- **Each member self-verifies** — gzip's CRC32 + length trailer means a fully
  written member is provably complete. Free per-chunk integrity.
- **Producer resume is a seek, not a recompress** — member K maps to source range
  [K·S, (K+1)·S); resuming = `int 13h`-seek the card to K·S and continue. **Never
  recompresses** already-sent data (critical on a slow 486).
- **No determinism dependency** — resuming at *source* boundaries sidesteps any
  "skip the first N compressed bytes" fragility across zlib versions.

Tradeoff: independent members reset the deflate dictionary each span — negligible
ratio loss at MB granularity. Tune span up for ratio/fewer fsyncs, down for finer
resume.

### 2d. Frozen v1 on-disk format (shipped desktop-first, 2026-06-24)

Implemented on the desktop ahead of the network transport — `src/rbformats/cbk.rs`
(`pack_folder_to_cbk` / `materialize_cbk_to_folder`), `rb-cli cbk pack|unpack`, and
`rb-cli restore <x>.cbk` (materializes a temp folder, then restores). **This
freezes the format the DOS Family-B producer (7b) must emit.** All integers
big-endian (the `RBK` convention).

```text
file = [chunk]* [index] [footer:16]

chunk (24-byte header + payload):
  magic       u32 = "RBKC" (0x5242_4B43)
  version     u16 = 1
  logical_id  u16              member index (into the index table)
  src_offset  u64              uncompressed offset of this span in the member
  len         u32              payload byte length
  crc32       u32              CRC32 of the payload bytes
  payload     [len]            one independent gzip member

index:
  magic        u32 = "RBKI" (0x5242_4B49)
  version      u16 = 1
  member_count u16
  per member:  name_len u16, name (UTF-8, folder-relative),
               kind u8  (0=Gz: payloads concatenated ARE the file, e.g.
                         partition-N.gz; 1=Raw: file = gunzip(concat payloads),
                         e.g. metadata.json / mbr.bin),
               logical_id u16, chunk_count u32, chunk_count × chunk_offset u64

footer (last 16 bytes): magic u32 = "RBKF" (0x5242_4B46), index_offset u64,
                        index_len u32
```

A reader seeks EOF−16, reads the footer, jumps to the index — no full scan. The
desktop packer emits **one chunk per member** (simplest valid chunking); the DOS
producer will append **many** chunks per partition (each a ~1–4 MB-span gzip
member) for the resume granularity §2c/§3 need. Both are valid `.cbk` this
materializer reads. The network `.idx` sidecar (§2b/§3) layers *on top* of this:
it is the same per-member chunk-offset list written incrementally for crash-safe
resume, with the trailer index as the finalized form.

### 2e. `.cbk` as a first-class image — native inspect/browse/restore (planned)

**Decision (2026-06-24):** a `.cbk` must behave like any other disk image
everywhere in the app — `inspect`, `ls`/`get` (browse + extract files), `fsck`,
GUI Inspect/Restore — **with no user-visible "extract first" step**. Restore from
`.cbk` is treated exactly like a folder restore. *Editing* a `.cbk` may do extra
legwork (materialize → edit → repack) and is in scope but lower priority.

The whole app reads disks through one abstraction — `Box<dyn ReadSeek>`
(`ReadSeek: Read + Seek + Send`, `src/rbformats/mod.rs`) — and a partition is just
a byte offset into that whole-disk reader (`open_filesystem(reader, offset, …)`).
So native `.cbk` support = **present the container as a reconstructed whole disk**
(table at sector 0, partitions at their `byte_offset()`, gaps zero) behind that
trait, then hook it into the **one** open/detect dispatch every consumer funnels
through.

Phased plan (this is the work, in order):

- **(i) `CbkReader: ReadSeek`** — open a `.cbk` as a seekable disk. v1
  implementation reuses the existing compressed-image precedent
  (`ZipDiskReader` / `GzipTempReader`): materialize the container to a temp
  folder, reconstruct the disk into a **sparse temp file**
  (`reconstruct_disk_from_backup`, `src/rbformats/mod.rs`), and delegate
  `Read`/`Seek` to it. Transparent — the user never runs an extract step; the
  temp disk is the app's business, exactly like opening a `.zip`-wrapped image
  today. *(Future: a truly lazy reader that decompresses only the chunked
  members a seek touches — the §2c/§2d span layout is designed for this; it's a
  reader upgrade, no format change.)*
- **(ii) Hook the dispatch** — add an `is_cbk_path` arm to `open_read_dispatch`
  (`src/model/source_reader.rs`) and `.cbk` to `is_container_path`. That single
  arm lights up **inspect**, the CLI `resolve` layer (`ls`/`get`/`fsck` via
  `open_peeled_read`), and GUI Inspect automatically. Add a `.cbk` arm to
  `BrowseSession::open_image` (`src/model/browse_session.rs`) for browse/extract.
- **(iii) GUI picker + associations** — add `"cbk"` to `DISK_IMAGE_EXTS`
  (`src/model/file_types.rs`; *not* `NON_ASSOCIATED_EXTS` — a `.cbk` should
  double-click-open) + a regression test. The Inspect/Restore tabs then accept a
  `.cbk` with no further change.
- **(iv) Restore parity** — already done for the CLI (materialize-to-temp arm in
  `restore.rs`); GUI restore picks up `.cbk` for free once it's in
  `DISK_IMAGE_EXTS` and routed like a folder.
- **(v) Edit (lower priority)** — write-through to a `.cbk` is materialize →
  edit the folder/partition → repack (`pack_folder_to_cbk`). Acceptable to be a
  copy-on-write repack rather than in-place.

---

## 3. Resume — the transfer map

### 3a. The crash-consistency rule

TCP guarantees order **only while the connection lives**; a crash/reboot kills the
socket and TCP gives nothing across it. **Resume is entirely application-layer.**

The **host** owns durable resume state (stable storage, assembly point, survives
the DOS box rebooting; the DOS side stays ~stateless and can't write the source
card anyway). The receive loop, with **strict ordering**:

1. Receive a full member into the container at its offset.
2. **fsync the container** — bytes durable.
3. *Then* append the member record to the index, written **atomically** (write
   `.idx.tmp`, fsync, `rename()` over the real file; `ReplaceFile`/`MoveFileEx`
   on Windows).

Because data is fsynced **before** it's recorded committed, a crash can never mark
a member committed that isn't on disk. The reverse (durable but unrecorded) is
harmless — re-receive that one member.

### 3b. The resume handshake

1. cb-dos reconnects: `RESUME session_id, source=<fingerprint>` (see §4).
2. Host loads the index, **verifies source fingerprint** (refuse/warn on swap),
   and **truncates each in-progress member back to its last committed end offset**
   — discards any half-written trailing member. Disk state now == recorded state.
3. Host replies per logical member: "complete" or "resume at member K
   (source offset K·S)".
4. cb-dos seeks the source and resumes emitting members; host continues the
   checkpoint loop.
5. At finalize, host verifies each member against the per-partition checksum the
   native format already expects (end-to-end gate). Per-member CRCs caught
   mid-flight corruption.

**Ordering:** send big resumable members first; write tiny `metadata.json` (needs
final compressed sizes/checksums) **last** — if a crash precedes it, regenerate it
on resume (cheap; not worth checkpointing).

### 3c. Prior art

This is **GNU ddrescue's mapfile** pattern (durable `offset size status` map →
resume; periodic rewrite bounds loss) made content-aware. Reference its mapfile
format when specing the `.idx`. partclone's per-N-block embedded CRC32 is the
direct analog of our per-member CRC.

---

## 4. Disk-state fingerprint — "same source?" (cheap gate)

**Never take a full-disk CRC** — reading the whole card over int13h is the exact
work resume exists to avoid, and you'd pay it every check. "Resumable?" only needs
"is this the same, unchanged source I started?", answered by a **composite
fingerprint** from a few seconds of reads.

### 4a. The layered fingerprint, by cost vs. power

| Ingredient | Cost | Catches a change when… |
|---|---|---|
| Geometry + LBA count + MBR/ptable CRC | ~free (1 sector) | layout / repartition |
| Volume serial(s) (from BPB) | ~free (1 sector/vol) | reformat |
| Free-cluster count | ~free | add/delete/resize — **UX only**, *subsumed below* |
| **Allocation-bitmap CRC** (FAT / `$Bitmap`) | **~free — already walked for compaction** | **any allocation change**: add/delete/move/**defrag**/resize |
| Directory-tree hash (mtime/size/start cluster) | dir walk | most same-size edits |
| Full content hash | whole-disk read | everything |

**The allocation-bitmap CRC is the workhorse** and it's free (cb-dos walks the
FAT/`$Bitmap` for compaction regardless). It *subsumes* free-space (popcount of
the same bitmap) — keep free-space for **UX/preflight** only, not as the detector.

Recorded **per partition**:

```
disk:  { geometry, lba_count, mbr_crc, ptable_crc }
vol[N]:{ serial, free_clusters, alloc_bitmap_crc }   // bitmap_crc = workhorse
```

### 4b. Why this isn't redundant with the per-member CRCs

Per-member CRCs verify **wire integrity** (sent == received). They **cannot**
detect that the *source changed between the original session and the resume* —
each member is individually valid, just from different points in time. Splicing
old + new members yields an internally inconsistent, split-vintage image with
every CRC passing. The **bitmap fingerprint is the only cheap thing that catches
that.** Not duplication — it guards a hole the content CRCs structurally can't see.

### 4c. Honest limit

Even bitmap-CRC + dir-hash miss a **same-cluster, same-size, timestamp-preserving
overwrite**. Only a content hash catches that — and we *do* compute it, **during
transfer** (per-member + per-partition), not as an upfront gate. So:

- **Fingerprint = cheap pre-flight gate** ("probably same, unchanged → proceed").
- **Streamed per-member / per-partition CRC = the integrity guarantee.**

Given cb-dos's **quiescent-source model** (boot from floppy → the card is a
passive int13h device nothing writes; the DOS substitute for a snapshot), the
realistic threat is a *swapped* card or one *booted-and-edited between resume
attempts* — both caught by the fingerprint. Optional extra: on resume, re-hash
just the **last committed member's** source range and compare (one ~MB read).

### 4d. Bonus: the fingerprint *is* the incremental index

The same per-partition `{serial, bitmap_crc}` compared against a *prior completed
backup* answers "is this partition unchanged → skip re-imaging, copy from prior."
Resume gate and incremental index are the **same artifact**. v1 = resume only;
incremental falls out later.

---

## 5. File manifest — "did it change?" + idempotency + boot protection

Change-detection is **rsync-tier best-effort**: size + mtime + **archive bit**,
per file. The payload **stays a block image** (preserves bootability + desktop
resize); we **add a file-level manifest sidecar** — file-awareness without giving
up block fidelity. This is the "more of a filesystem backup" feel.

### 5a. Manifest contents (per partition)

```
files[]: { path, size, mtime, attribs{archive,hidden,system,readonly}, start_cluster }
system:  { mbr_boot_code_hash, vbr_hash, hidden_sectors_hash,
           sysfiles:[ {name:"IO.SYS",  size, mtime, attribs, first_cluster, hash},
                      {name:"MSDOS.SYS", ...}, {name:"COMMAND.COM", ...} ] }
```

Built by one directory-tree walk (the same walk that finds swap files, §6).

### 5b. Change detection

Diff new manifest vs. stored:
- **Whole-disk identity gate** — all files (size+mtime+archive) and the system
  section equal a prior backup → *"identical to backup B1, nothing to do"* and
  stop. (This is the "prevent a pointless re-backup right after a restore.")
- **Per-file incremental** — otherwise the diff *is* the changed set (combine with
  §4 bitmap CRC for the resume gate).

### 5c. Idempotency — the archive bit is **round-tripped, not computed**

The footgun: **a restore writes every file, and DOS sets the archive bit on every
write** → a naïve restore leaves every file archive=set → re-backup thinks
"everything changed," defeating the gate.

Fix — make mtime + attribs a **round-tripped attribute**:
1. **Backup records** each file's mtime + attribs (incl. the archive bit's actual
   state).
2. **Restore replays them** — after writing data, set original mtime
   (int 21h/5701h) and attributes incl. archive bit (int 21h/4301h) from the
   manifest. A restored file looks exactly as at backup time, **not** "freshly
   written."
3. **Re-backup sees no change** → backup↔restore is **idempotent**.

So the archive bit's job here is **surviving the round-trip**, not detecting
modification (it's too spoofable for that — see 5e).

### 5d. Boot protection ("check boot stuff")

A backup that loses bootability is worthless for the vintage use case. The
`system` block fingerprints + round-trips exactly what the DOS `SYS` command
babysits: MBR boot code (446 bytes), each VBR + hidden sectors, and
`IO.SYS`/`MSDOS.SYS`/`COMMAND.COM` (name, size, mtime, attribs, **first cluster +
contiguity**, content hash). First-cluster/contiguity is what actually determines
boot and is the natural pre-flight for the Phase-B defrag (which must keep those
first and contiguous). Mismatch vs. prior backup ⇒ flag a bootability change.

### 5e. Honest caveat (don't oversell)

size+mtime+archive is fooled by a same-size, timestamp-preserving, bit-cleared
overwrite, and the archive bit can be **cleared by another DOS backup tool**
(MSBACKUP/XCOPY /M) between sessions. Only content hashing is airtight — which we
still stream. Manifest = **cheap gate + idempotency mechanism**; streamed CRCs =
the guarantee.

### 5f. Prior art

rsync's default **quick-check** (size+mtime, no content read; `--checksum` to read)
is exactly our two-tier split. The DOS-native **archive bit** (BACKUP/MSBACKUP/
XCOPY /M set-on-write, clear-on-backup) is the historical file-level change
primitive — we *round-trip* it rather than rely on it. Snapshots (VSS/LVM/
zfs-send) are the "proper" solution DOS lacks; our boot-from-floppy quiescence is
the substitute.

---

## 6. Swap-file exclusion

The manifest walk identifies swap files (you can only exclude what you can find
the cluster chain for). Two separable actions.

### 6a. Recognized swap/page files

| System | File(s) | Location / attribs |
|---|---|---|
| Windows 3.x (386 Enh.) | `386SPART.PAR` (perm), `WIN386.SWP` (temp) | root, hidden; `SPART.PAR` in Win dir points to it |
| Windows 9x | `WIN386.SWP` | Windows dir or root |
| Windows NT/2k/XP (NTFS or FAT) | `pagefile.sys`, **`hiberfil.sys`** | root, hidden+system |
| OS/2 | `SWAPPER.DAT` | usually `\OS2\SYSTEM` |

`hiberfil.sys` (= RAM size) and `pagefile.sys`/`386SPART.PAR` are the big wins.

**DO NOT EXCLUDE** `DBLSPACE.000` / `DRVSPACE.000` / `STACVOL.DSK` — those **are**
the compressed filesystem; excluding them destroys the volume. The exclusion list
is a strict **allowlist** of known names in expected locations with expected
attributes — never a fuzzy match.

### 6b. Exclude from the change counter (always)

Swap churns every boot but carries no meaningful state. Keep it listed in the
manifest (name/size/attribs/chain) flagged `volatile:true`, but **exclude from the
change signal** — else every backup looks "changed" and idempotency (§5c)
collapses. Unambiguously correct, free.

### 6c. Exclude from the payload (default on, `--keep-swap` to opt out)

The payload images allocated clusters; a swap file's clusters are allocated.
**Level 1 (ship this) — zero the content, keep the allocation** (what
Ghost/ntfsclone do):
- Keep the directory entry / MFT record, FAT/`$Bitmap` allocation, and cluster
  chain **as-is** → filesystem stays fully consistent, file still exists full-size.
- Emit **zeros** for those clusters in the stream → gzip crushes them.
- Restore brings the file back full-size, zero-filled; OS reinitializes swap on
  next boot. (`hiberfil.sys` consequence: restored disk cold-boots instead of
  resuming from hibernation — almost always desired; document it.)

**Level 2 (optional, later) — actually deallocate** (free the FAT chain + drop the
dir entry) so the volume's *minimum size* shrinks and resize-restore reclaims it.
A real FS mutation in the image (more code/risk); the OS recreates swap anyway, so
the only gain over Level 1 is a smaller resize-down minimum. Defer.

### 6d. Behavior

- **On by default**, `--keep-swap` escape hatch.
- **Conservative ID**: exact name **+** expected location **+** expected attribs.
- **Log every exclusion** ("Excluded WIN386.SWP (12 MB, zeroed)") — never silent.
- **Record** `excluded:true, content:zeroed` in the manifest so restore recreates
  the file and the counter keeps ignoring it.

### 6e. Feature-parity note

The desktop's native-format compaction is bitmap-based and doesn't zero swap
either. Per CLAUDE.md's GUI/CLI parity + shared-business-logic rules, swap
exclusion is a candidate for the **shared** path (desktop benefits identically),
not a cb-dos-only trick.

---

## 7. Prior-art map (for the record)

| Our piece | Established pattern |
|---|---|
| Transfer map / resume | **GNU ddrescue mapfile** |
| Used-blocks-only image | **partimage / partclone / Ghost** "smart" copy |
| Chunked / spanned image | Ghost segments, multi-volume ARJ/ZIP/RAR |
| Per-chunk CRC | **partclone** embedded CRC32 every N blocks |
| Cheap change gate | **rsync** size+mtime quick-check (vs. `--checksum`) |
| File-level change primitive | DOS **archive bit** (BACKUP/MSBACKUP/XCOPY /M) |
| Snapshot we lack | VSS / LVM / zfs-send → substituted by **boot-from-floppy quiescence** |
| TCP/IP on packet driver | **mTCP / WATTCP** |

No single tool does resumable + fingerprint-gated + *networked* block imaging on
DOS — the **combination** is the new part; the **primitives** are all proven.

---

## 8. Open risks / unknowns

- ~~**mTCP toolchain mixing** — Open Watcom/C++ vs our DJGPP/C.~~ **Resolved
  (§1b): WATT-32** (DJGPP-native BSD sockets, prebuilt `libwatt.a`) — no Watcom,
  one toolchain. The `nethello` client links it and compiles clean under DJGPP.
- **DOS IP config UX** — static IP (one `AUTOEXEC.BAT` line) vs DHCP client;
  detect/guide.
- **Container ↔ folder bridge** — ship (a) materialize-at-finalize first; (b)
  desktop reads `.cbk` directly is a later, larger change.
- **`MultiGzDecoder`** — confirm the desktop reads multi-member `.gz` everywhere
  the `Gzip` codec lands (not just one call site).
- **Fingerprint false-negative** — same-cluster/same-size/timestamp-preserving
  overwrite escapes the gate; mitigated by streamed CRC + quiescent-source model.
  Document the ceiling.
- **Archive-bit cleared by 3rd-party tools** — the gate weakens if another backup
  tool ran between sessions; not a correctness bug (streamed CRC backstops), but
  the "identical, nothing to do" shortcut could mis-fire. Keep it advisory.
- **Swap ID safety** — strict allowlist; never touch DBLSPACE/DRVSPACE/STACVOL.
- **86Box networking mode** — SLiRP+port-forward (TCP) works; if ever bridged,
  wired-only + ideally a second host.

---

## 9. Phased plan (within cb_dos Phase 7)

> Prerequisite: cb_dos Phases 2–4 (local removable-media round-trip) must prove
> the native format first. Networking only swaps the *destination* under a working
> engine.

- [x] **7a — Frame/socket hello-world.** **Done — handshake round-trips
      end-to-end (real FreeDOS in qemu over an NE2000 packet driver + SLiRP,
      2026-06-24).**
      - **Host (done, headless-tested):** `rb-cli serve` now accepts a **binary
        Family-B handshake** alongside the JSON Family-F one on the same port —
        `read_handshake` peeks the 4-byte magic (`b"RBK0"`) to disambiguate, and
        `write_binary_hello` replies (`magic + version + caps`, big-endian).
        `src/remote/{protocol,server}.rs`; loopback test
        `family_b_binary_handshake_over_loopback`.
      - **DOS client (done, runtime-verified):** `crusty-backup/src/net_hello.c`
        (`NETHELLO.EXE`) — **WATT-32** `sock_init` + BSD `socket`/`connect`/
        `send`/`recv`, sends the binary Hello to `<agent-ip>:7341`, prints the
        reply. Builds clean under DJGPP (`make -C crusty-backup net`).
      - **Runtime check (done, 2026-06-24, emulator):** booted the FreeDOS 1.4
        image headless in **qemu-system-i386** (`-device ne2k_isa,iobase=0x300,
        irq=3` + `-netdev user` SLiRP), loaded the Crynwr **`NE2000.COM`** packet
        driver (`NE2000 0x60 3 0x300`), and ran `NETHELLO 10.0.2.2 7341` against a
        host `rb-cli serve`. Handshake round-tripped: client printed *"Connected.
        Agent protocol v2, capabilities 0x0001 [file]"* (exit 0), host logged
        *"Family-B client connected (version 2, caps 0x0000)"*. **WATTCP.CFG UX:**
        a one-line `my_ip = dhcp` is enough — SLiRP's DHCP leases the guest and
        `10.0.2.2` reaches the host listener. **DOS gotcha:** FreeCOM doesn't honor
        `2>`/`2>&1` (parses the `2` as an argv) — pass the port explicitly and use
        single `>` redirects, or all output goes to stderr/console. CWSDPMI.EXE
        must sit next to `NETHELLO.EXE` (real FreeDOS has no DPMI host).
- [ ] **7b — Chunk protocol + container.** Define `.cbk` chunk header + index;
      stop-and-go single-member PUT DOS→host; byte-verify; write/read the index.
- [ ] **7c — Whole-folder backup over wire.** Stream a full native folder as
      members into `.cbk`; materialize the folder at finalize; desktop restores it
      unchanged.
- [ ] **7d — Resume.** fsync-before-record + truncate-to-last-committed +
      `RESUME` handshake + fingerprint verify (§4). Kill mid-transfer, reconnect,
      finish; verify end-to-end checksum.
- [ ] **7e — Restore over wire.** Host serves members on `GET`; cb-dos restores
      with resize; restored card boots.
- [ ] **7f — Manifest + idempotency.** Emit the file manifest (§5); restore
      replays mtime/attribs; prove backup→restore→backup is a no-op.
- [ ] **7g — Boot section + swap exclusion.** System-block fingerprint/round-trip
      (§5d); Level-1 swap zeroing (§6c) in the **shared** compaction path.
- [ ] **7h (optional) — Incremental backup.** Reuse the §4d fingerprint + §5
      manifest to skip unchanged partitions/files.
- [ ] **7i (optional) — Level-2 swap deallocation; desktop reads `.cbk` directly.**

---

## Progress log

- 2026-06-24 — **`.cbk` container frozen + shipped desktop-first (§2d).** Built
  `src/rbformats/cbk.rs` (`pack_folder_to_cbk` / `materialize_cbk_to_folder`),
  the `rb-cli cbk pack|unpack` verb, and taught `rb-cli restore` to read a `.cbk`
  directly (materialize a temp folder, then restore — §2b option a). The on-disk
  format is now **frozen at v1** (RBKC chunk headers = gzip members, RBKI trailer
  index, RBKF footer; big-endian) so the future DOS Family-B producer (7b) emits
  the same bytes. Verified: a cb-dos backup folder → `.cbk` (1255 B vs 1882 B
  folder) → `rb-cli restore MYDISK.cbk` rebuilds a valid disk (FS mounts, file
  intact), and `cbk unpack` round-trips every folder file byte-for-byte. Desktop
  packer emits one chunk/member; the network producer will append many (per-span)
  for resume — both valid. The §3 `.idx` sidecar layers on top (incremental form
  of the same chunk-offset table). 3 unit tests; clippy clean. Bridges 7b: the
  container exists, so 7b is now just the wire framing + incremental index.
- 2026-06-24 — **Phase 7a complete — handshake round-trips on real FreeDOS.**
  Closed the only open 7a item (the runtime check) on a headless Linux box, no
  86Box/DOSBox-X GUI needed: **qemu-system-i386** booting the FreeDOS 1.4 image
  with an emulated **NE2000** (`-device ne2k_isa,iobase=0x300,irq=3`) + **SLiRP**
  usermode net (`-netdev user`). Pulled the Crynwr **`NE2000.COM`** out of the
  FreeDOS image's own `crynwr.zip`, loaded it on int `0x60`, and ran
  `NETHELLO 10.0.2.2 7341` against `rb-cli serve`. Both ends confirmed: client
  *"Connected. Agent protocol v2, capabilities 0x0001 [file]"* (exit 0), host
  *"Family-B client connected (version 2, caps 0x0000)"*. The binary `RBK0`
  Family-B handshake is now proven over a true DOS TCP stack, not just the
  loopback unit test. Two lessons worth keeping: (1) **FreeCOM mis-parses `2>` /
  `2>&1`** — it treats the `2` as a program argument (which silently sent
  `NETHELLO` to *port 2*), so pass the port explicitly and stick to single `>`
  redirects; (2) **`my_ip = dhcp`** in WATTCP.CFG is the whole network config —
  SLiRP's DHCP does the rest. CWSDPMI.EXE must travel next to the exe (real
  FreeDOS provides no DPMI host; only the disk-spike's DOSBox-X faked one).
  Next: **7b** (the `.cbk` chunk protocol + container). The transport is unblocked.
- 2026-06-21 — **Phase 7a started (socket + handshake hello-world).** Resolved
  the long-open **TCP-stack question: WATT-32** (DJGPP-native BSD sockets,
  prebuilt `libwatt.a`, BSD/AGPL-compatible) over mTCP/Watcom (§1b). Host:
  `rb-cli serve` gained a **binary Family-B handshake** path that coexists with
  the JSON Family-F handshake on one port (peek the `b"RBK0"` magic) —
  `read_handshake` / `write_binary_hello` in `src/remote/protocol.rs`,
  `handle_family_b` in `server.rs`; headless test
  `family_b_binary_handshake_over_loopback`. DOS client:
  `crusty-backup/src/net_hello.c` (`NETHELLO.EXE`) connects to an agent IP and
  exchanges the binary Hello — compiles clean under DJGPP via the new `make net`
  target + `net/fetch-watt32.sh`. Runtime check on real NIC / DOSBox-X is the
  next step (the only remaining 7a item). The chunk/`.cbk` data protocol (7b+)
  is untouched.
- 2026-06-03 — Scoping discussion captured into this doc. **Transport: TCP/IP over
  the Crynwr packet driver** (not raw L2 / serial / parallel) — chosen for TCP's
  free reliability + a **plain-socket, unprivileged, SLiRP-friendly host** (vs.
  pcap/Npcap/BPF/root for raw L2). **mTCP** (GPLv3, AGPL-OK) is the reference /
  borrow candidate; borrow-vs-port-to-DJGPP is open. Host = **`rb-cli serve`**
  (Family B of the unified daemon; was `net-serve`).
  Designed the **chunked `.cbk` container** (wire framing = on-disk artifact =
  ddrescue-style resume log) with chunks = **independent source-keyed gzip
  members** (`MultiGzDecoder` on the desktop; producer resumes by source seek, no
  recompress). Resume via **fsync-before-record + truncate-to-last-committed**.
  Disk-state **fingerprint** = geometry+MBR/ptable CRC+serials+**allocation-bitmap
  CRC** (no full-disk CRC; bitmap CRC subsumes free-space, doubles as the
  incremental index). Change detection = **rsync-tier** size+mtime+**archive bit**
  via a **file manifest sidecar** (block image stays the payload); archive bit is
  **round-tripped** by restore for **backup↔restore idempotency**; manifest also
  fingerprints/round-trips **boot** (MBR code, VBR, IO.SYS/MSDOS.SYS/COMMAND.COM).
  **Swap files** (386SPART.PAR / WIN386.SWP / pagefile.sys / hiberfil.sys /
  SWAPPER.DAT) excluded from the counter always and **zeroed** in the payload by
  default (`--keep-swap`); strict allowlist — never DBLSPACE/DRVSPACE/STACVOL.
  Prior-art map recorded. No code yet.
