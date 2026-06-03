# crusty-backup — network transport + disk-state model — scoping plan

Living scope + plan for **crusty-backup**'s (`cb-dos`) networked backup/restore and
its **disk-state / resume / change-detection** model. This is the companion to
[`cb_dos.md`](cb_dos.md) (the overall cb-dos scope); that doc deferred networking
to §6 / Phase 7 and left "incremental-ish" state tracking unspecified. This file
captures the design discussion that fleshed both out.

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
| Host listener home | **`rb-cli net-serve` subcommand** — reuses format + restore code, scriptable, cross-platform |
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

**Open decision — borrow vs port.** mTCP is **Open Watcom / C++**; our POC is
**DJGPP / C**. Options:
- **Borrow mTCP TCP**: get correct TCP for free, but solve toolchain mixing
  (build mTCP objects under Watcom and link, or switch cb-dos to Watcom).
- **Port mTCP's UDP/IP/ARP to DJGPP** and hand-roll our thin reliability layer on
  UDP (a few hundred lines) — keeps one toolchain, avoids inheriting TCP.
- **Hand-roll TCP**: rejected — connection state machine / RTT / congestion is a
  real project; mTCP exists precisely so we don't.

Leaning: if we want TCP, **borrow mTCP**; resolve the toolchain question in the
Phase-7 spike. Reserve raw-L2/UDP only if a reason resurfaces.

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

- **mTCP toolchain mixing** — Open Watcom/C++ vs our DJGPP/C. Resolve borrow-vs-
  port in the Phase-7 spike (§1b).
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

- [ ] **7a — Frame/socket hello-world.** DOS: bind packet driver, bring up
      IP/ARP (borrowed or minimal), open a TCP socket to the host, exchange a
      greeting. Host: `rb-cli net-serve` accepts a plain socket, prints peer.
      Resolve the mTCP borrow-vs-port question here. Test in 86Box (SLiRP +
      port-forward).
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

- 2026-06-03 — Scoping discussion captured into this doc. **Transport: TCP/IP over
  the Crynwr packet driver** (not raw L2 / serial / parallel) — chosen for TCP's
  free reliability + a **plain-socket, unprivileged, SLiRP-friendly host** (vs.
  pcap/Npcap/BPF/root for raw L2). **mTCP** (GPLv3, AGPL-OK) is the reference /
  borrow candidate; borrow-vs-port-to-DJGPP is open. Host = **`rb-cli net-serve`**.
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
