# crusty-backup (`cb-dos`) — **network phase** resume prompt

Hand-off for continuing the **optional Net 7h–7i** (networked backup/restore over
TCP). The local removable-media engine is complete and qemu-verified; networking
only swaps the *destination* under it. **7a (handshake), 7b (chunk PUT protocol +
host `.cbk`), 7c (block-level networked backup `CRUSTYBK BACKUP rb://...`), 7d
(resume), 7e (restore over the wire `CRUSTYBK RESTORE rb://...`), 7f (per-partition
file manifest + idempotency), and 7g (boot-section deepening + Level-1 swap
exclusion) are done and qemu-verified — the backup↔restore loop is closed,
resumable, file-aware, boot-fingerprinted, and swap-aware.** What remains is
explicitly **optional** (7h incremental, 7i Level-2 swap dealloc + desktop swap
parity). Paste the section below (from "Resume the…" down) to kick off the next
session.

---

Resume the crusty-backup (cb-dos) **network transport** work on git branch `cbdos`.

Read these first, in order, before doing anything:
1. **docs/cb_dos_network_and_state.md** — the network + disk-state design. The
   load-bearing doc: §1 transport (TCP over a packet driver; host = `rb-cli serve`
   "Family B"; DOS client over WATT-32), §2 the `.cbk` chunked container (wire
   framing = on-disk artifact = resume log; **frozen v1** format in §2d), §3 resume,
   §4 the disk-state fingerprint, §5 manifest/idempotency/boot, §6 swap exclusion,
   and **§9 the phased plan 7a–7i** (the tick-it-off list).
2. docs/cb_dos_networking.md — the pragmatic interim (mTCP/FTP off the box today).
3. docs/cb_dos_resume.md — the local-engine hand-off + the **qemu test-rig recipe**
   and the "gotchas learned the hard way" (do not relearn them).
4. docs/cb_dos.md — full scope + progress log (skim the recent entries).

**Work the TOP UNCHECKED box in §9 of cb_dos_network_and_state.md. 7h(a)
(host-side change detection + the §5d bootability-change flag) and §6e desktop
swap parity are DONE; what's left is optional: the `7h` **streaming-skip**
optimization (per-partition fingerprints → daemon skip-map → cb-dos skips
unchanged partitions → host copies them from the prior `.cbk`) and `7i` (Level-2
swap dealloc + single-file-CHD swap). The core loop is done; these are stretch.**

## Where we are

Branch `cbdos` (off `main`), ~65 commits ahead, tree clean, **everything below
qemu-verified on real FreeDOS**.

- **Local cb-dos engine — DONE.** backup / restore / clone / browse across
  **FAT12/16/32 + NTFS + extended/logical**, on-DOS FAT resize, per-partition
  selection, live-disk browse, live progress, **boot-aware FAT defrag**
  (`backup`/`clone /DEFRAG`), a **second codec** (LZ4, `/CODEC:LZ4`), bootable
  FreeDOS floppy + CD in CI. The redirected-`backup` mbr.bin bug is fixed.
- **Net 7a — DONE.** The **binary Family-B handshake** (`RBK0` magic) round-trips
  end-to-end over **real FreeDOS in qemu** (emulated NE2000 + SLiRP, Crynwr
  `NE2000.COM` packet driver, WATT-32 client → `rb-cli serve`). Host:
  `src/remote/{protocol,server}.rs` (`read_handshake` peeks the magic;
  `write_binary_hello`). DOS client: `crusty-backup/src/net_hello.c`
  (`NETHELLO.EXE`, `make -C crusty-backup net`).
- **Net 7b — DONE.** The **chunk-PUT wire protocol + the host receiver**.
  `src/remote/protocol.rs` (`PutHeader`/`MemberHeader`/`ChunkHeader`,
  `read_put_header`/`read_member_header`/`read_chunk_header` + client-side writers,
  ack/result) + `server.rs` `handle_family_b`/`receive_put` (stage each member's
  chunks to a temp folder, CRC-check, fsync-before-ack, then `pack_folder_to_cbk`
  atomic `.tmp`+rename — reuse, not a second format). `CAP_FAMILY_B` advertised.
  Loopback test `family_b_chunk_put_assembles_cbk_over_loopback` (multi-chunk).
- **Net 7c — DONE.** **Networked backup baked into `CRUSTYBK BACKUP rb://...`**,
  block-level, no intermediate folder. `crusty-backup/src/cbnet.{h,c}` (WATT-32
  sockets + the chunk-PUT framing + a zlib **gzip span streamer**): each partition
  is imaged over int13h, smart-compacted, and compressed into independent **1 MiB
  gzip-member spans** sent as chunks stop-and-go; `mbr.bin`/`metadata.json` ride as
  Raw members. `cmd_backup.c` detects the `rb://` dest (`parse_rb_dest`/
  `cmd_netbackup`); `build_metadata` is shared by the local + net paths.
  `CRUSTYBK.EXE` links WATT-32. qemu-verified: a 3.5 MiB disk → 4 spans →
  multi-member `.cbk` → `rb-cli restore` byte-identical. *(Primaries + FAT/NTFS,
  gzip only; extended/logical, `/DEFRAG`, LZ4 compose later.)*
- **Net 7d — DONE.** **Resume — a killed transfer continues instead of
  restarting.** PUT header carries the §4 fingerprint; daemon replies with a
  resume map (`RBKR`). Host `receive_put` assembles into a **persistent staging
  dir + `journal.json`** (fsync-data-then-record), truncates each member to its
  last committed chunk on a fingerprint-matching reconnect, **owns the gz
  checksum** (fills `metadata.json` at finalize). DOS `cbnet` reads the resume map
  and skips committed spans by seeking the source to `committed·CBNET_SPAN`.
  Loopback test `family_b_chunk_put_resumes_after_drop`; qemu-verified with the
  test-only `RB_SERVE_TEST_DROP_AFTER_CHUNKS` knob (drop after 2 spans → reconnect
  → resume → byte-identical restore). *(Per-Gz-member resume; Raw members re-sent
  fresh. The host still stage-then-packs — a true streaming-append `.cbk` is a
  later optimization, not needed for correctness.)*
- **Net 7e — DONE.** **Restore over the wire — the loop is closed.** `CRUSTYBK
  RESTORE rb://HOST/NAME 81 /Y` pulls a `.cbk` back from the agent and rebuilds the
  disk over int13h, no local folder. A **GET** op (`RBKG`) joins PUT under one
  dispatcher (`read_family_b_op`); the daemon (`serve_get`) serves a `*.gz` member
  as raw gzip bytes (client inflates) and a Raw member decompressed, reusing
  `CbkPayloadReader` / `cbk_member_content_reader`. DOS: a `cbnet` GET client
  (`cbnet_start_get` / `cbnet_get_raw` / `cbnet_get_member_*` with manual
  multi-member `inflateReset`) + `cmd_netrestore` (same-size: stream gz → int13h,
  zero-pad, rebuild EBR, write MBR verbatim). Loopback test
  `family_b_get_serves_cbk_members_over_loopback`; qemu-verified backup-then-restore
  byte-identical. *(Same-size only; resize-over-the-wire is the follow-up.)*
- **Net 7f — DONE.** **Per-partition file manifest + idempotency.** Backup emits a
  `manifest-N.json` sidecar per FAT partition — a depth-first `files[]` list
  (path / size / mtime / attr / start_cluster, dirs flagged) plus a `system`
  boot-fingerprint block (MBR boot-code CRC, the partition's reserved/boot-sectors
  CRC, the DOS sysfiles with size/mtime/attr/first_cluster/contiguity). New
  `crusty-backup/src/cbmanifest.{c,h}` walks the live source read-only via the
  `cbbrowse` FAT reader (extended to carry the dir write-time); `cmd_backup` writes
  it locally, `cmd_netbackup` ships it as a Raw member, so it rides the folder /
  `.cbk` / network PUT for free. **Idempotency is structural** — cb-dos restore is
  block-level (`write_lba`), so dir entries (mtime/attribs/archive bit) round-trip
  verbatim; a same-size backup→restore→backup is a no-op and re-building the
  manifest yields the byte-identical document, so the §5c int-21h replay isn't
  needed. *(FAT only — NTFS has no on-DOS dir reader.)* **Also** bundled DOSLFN on
  the boot media (vendored adoxa/doslfn v0.42 `media/DOSLFN.COM` + attribution,
  shipped at the media root via `mkmedia.sh`, auto-loaded in `cbdos-autoexec.bat`)
  so the non-8.3 member names (`metadata.json` / `partition-N.gz` /
  `manifest-N.json`) work on a bare DOS host — the FreeDOS kernel has no LFN API of
  its own. qemu-verified: local backup→restore→backup byte-identical manifest, and
  a networked PUT (4 members) assembling a `.cbk` whose manifest matches the local
  one.
- **Net 7g — DONE.** **Boot-section deepening + Level-1 swap exclusion.** Two
  parts. **(a)** Each DOS sysfile in the manifest `system` block now carries a
  `hash` (CRC32 over its content, FAT-chain order) — a stable idempotency
  fingerprint that round-trips a block-level restore (`cbmanifest.c` `chain_crc32`).
  **(b)** New `crusty-backup/src/cbswap.{c,h}` allowlists swap/page files (exact
  name + location + attribs: `386SPART.PAR` / `WIN386.SWP` / `PAGEFILE.SYS` /
  `HIBERFIL.SYS` / `SWAPPER.DAT`; never DBLSPACE/DRVSPACE/STACVOL — those ARE the
  FS) and marks their cluster chains; the shared FAT compaction (`backup_fat_partition`
  + `netstream_fat_partition` via `build_swap_mask`/`swap_hit`) zeros that content
  while keeping the allocation (file survives full-size, OS rebuilds swap on boot),
  the manifest flags them `volatile`/`content:zeroed` (same `cbswap_is_swap()`
  predicate so flags and payload agree), `/KEEPSWAP` opts out, every exclusion is
  logged. qemu-verified local (restored swap files full-size + all-zero; IO.SYS
  byte-identical; BK1==BK2 manifest byte-identical; `/KEEPSWAP` images verbatim)
  **and** network (4-member PUT whose `.cbk` manifest is byte-identical to the
  local one). *(FAT only; §6e desktop-compaction parity deferred to 7i.)*
- **The `.cbk` container — FROZEN v1 and already the producer's chunk shape.**
  `src/rbformats/cbk.rs` (`pack_folder_to_cbk` / `materialize_cbk_to_folder`, RBKC
  chunks / RBKI index / RBKF footer, big-endian). The desktop reads `.cbk` as a
  first-class image (inspect / ls / get / fsck / restore / edit). **Crucially for
  7b:** backups now emit each partition as **source-span independent gzip members**
  (4 MiB spans) with a `partition-N.gz.idx` seek layout (`src/rbformats/gz_index.rs`),
  and the packer splits a partition into **per-span chunks carrying `src_offset`** —
  i.e. **§2c's exact chunk shape already exists desktop-side**. A chunk's payload is
  one independent gzip member; the desktop `MultiGzDecoder` + `CbkLazyReader`
  seek per-chunk; cb-dos (`gzread`/`gzseek`) reads the multi-member `.gz` too
  (qemu-verified). So 7b is **wire framing + index over a producer/format that's
  already built**, not new container work.

## What 7a–7g already give us (the closed loop)

A vintage box boots cb-dos and backs up **and** restores over the network, no local
folder either side:
- `CRUSTYBK BACKUP rb://<agent>:7341/MYDISK 81` — images the disk **block-level over
  int13h** and streams gzip-member spans to `rb-cli serve`, which assembles
  `MYDISK.cbk`. **Resumable** (7d): kill it, re-run, it continues from the last
  committed span (§4 fingerprint guards a swapped card). Each FAT partition also
  ships a **`manifest-N.json`** (7f/7g) Raw member, with **sysfile content hashes**
  (7g §5d) and **swap files zeroed + flagged** (7g §6, `/KEEPSWAP` to opt out).
- `CRUSTYBK RESTORE rb://<agent>:7341/MYDISK 82 /Y` — pulls the `.cbk`'s members back
  (GET) and rebuilds the disk over int13h, same-size.
Both directions are qemu-verified byte-identical, and the round-trip is **idempotent**
(7f/7g: a re-backup of a freshly-restored disk yields a byte-identical manifest,
hashes and swap flags included).
**Block-level, baked in, crash-proof, bidirectional, file-aware, boot-fingerprinted,
swap-aware.** The core feature is **complete**; what remains (7h/7i) is optional.

## What's left — optional 7h / 7i (pick the top unchecked box in §9)

- **7h — incremental.** **7h(a) DONE** (2026-06-25): host-side change detection +
  the §5d **bootability-change flag** — a networked PUT over a prior `NAME.cbk`
  logs which partitions are unchanged / changed and whether the boot chain differs
  (`src/remote/manifest.rs` diff + `server.rs::compare_to_prior_cbk`; reuses the
  7f/7g manifests, no wire change). **Remaining:** the **streaming-skip**
  optimization — send per-partition fingerprints in the PUT, the daemon replies a
  skip-map, cb-dos skips imaging the unchanged partitions, and the host copies them
  from the prior `.cbk`. (The §4d fingerprint + §5 manifest are the index.)
- **7i — Level-2 swap dealloc** (free the FAT chain + drop the dir entry so a
  resize-down minimum shrinks; 7g only zeros content). **Desktop swap parity is
  DONE** (§6e, 2026-06-25 — the Rust backup now Level-1 excludes swap via
  `CompactFatReader::new_excluding_swap` + a `--keep-swap` flag / GUI checkbox);
  only Level-2 dealloc and single-file-CHD swap exclusion remain under 7i.
- The deferred **resize-over-the-wire** (a forward-only streaming peek-then-resize,
  the one gap 7e left) also lives here.

## How we work

- **One small commit per step on `cbdos`** (never `main`). End commit messages with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
  The pre-commit hook runs `cargo fmt` + `clippy --all-targets -D warnings` on Rust
  changes (no Rust staged → it skips). New files need an explicit `git add`.
- **Build.** Host: `cargo build --bin rb-cli`; `cargo test --lib` (2093+ tests) +
  `cargo test --test remote_filesystem --features remote family_b` (the 4 PUT /
  resume / GET loopback tests). The DOS tool: `make -C crusty-backup crustybk` — it
  now links zlib + lz4 + WATT-32, so run all three fetches once
  (`deps/fetch-zlib.sh`, `deps/fetch-lz4.sh`, `net/fetch-watt32.sh`). Networked
  backup **and restore** are **in CRUSTYBK** (`backup`/`restore rb://...`), not a
  separate tool; `make net` still builds the `NETHELLO` handshake probe.
- **VERIFY ON REAL FREEDOS IN QEMU before claiming anything done.** The transport
  must be exercised against a live `rb-cli serve`, not just a loopback unit test.
- **As each box lands:** tick it in §9 of cb_dos_network_and_state.md, add a
  progress-log line there, and refresh this file + cb_dos_resume.md's commit list /
  ahead-count in a follow-up `docs(cb-dos): refresh` commit (the established rhythm).
- **Ask before any large scope change or new external dependency.**

## The qemu **network** test rig (verified pattern from 7a)

Persistent on this machine: `~/djgpp`, `~/FD14FULL.img`, `qemu-system-i386`,
`mtools`; gitignored-but-present `net/watt32`, `net/drivers/NE2000.COM`. Refetch
`CWSDPMI.EXE` per cb_dos_resume.md (it must sit next to the `.EXE` — real FreeDOS
has no DPMI host).

```bash
# Host listener: `serve` binds all interfaces on :7341 and accepts both the
# binary Family-B (RBK0) and JSON Family-F handshakes; reachable from the guest at
# the SLiRP gateway 10.0.2.2. Override with `--bind host:port` if needed.
cargo build --bin rb-cli
./target/debug/rb-cli serve &                  # default bind :7341 (Family B + F)

# Guest: emulated NE2000 wired to SLiRP usermode net.
qemu-system-i386 -m 64 -display none \
  -drive file=/tmp/base.img,format=raw,if=ide,index=0 \
  -netdev user,id=n0 -device ne2k_isa,netdev=n0,iobase=0x300,irq=3
```

In the guest (via `FDAUTO.BAT`): load the packet driver `NE2000 0x60 3 0x300`,
a one-line `WATTCP.CFG` = `my_ip = dhcp` (SLiRP's DHCP does the rest), then run
`CRUSTYBK` against the agent. The **7c** proof (2026-06-25): `FDAUTO.BAT` =
`NE2000 0x60 3 0x300` → `CD \CB` → `CRUSTYBK BACKUP rb://10.0.2.2:7341/MYDISK 81`
→ `FDAPM POWEROFF`, with the host serving `--root /tmp/agent` (hdb 0x81 = the
source FAT disk). The host logged *"PUT \"MYDISK\" (3 members) … complete"*; a
3.5 MiB used-data disk streamed as **4 gzip-member spans** the host concatenated
into one multi-member `partition-0.gz`, and `rb-cli restore` rebuilt the disk
(`BIG.BIN`/`HELLO.TXT` byte-identical). Copy `CRUSTYBK.EXE`, `CWSDPMI.EXE`,
`NE2000.COM`, `WATTCP.CFG` into `\CB`; run from there (CWSDPMI + WATTCP.CFG must
be reachable from the cwd). A throwaway `/tmp/run_netbackup.sh` (serve + qemu) was
the harness; rebuild it from this recipe. **Build the source disk**: an MBR FAT16
partition (type 0x06, LBA 2048) in a 17 MiB image (`mformat -i img@@1048576`),
attached as `index=1` (0x81). For multi-span coverage drop a >1 MiB *incompressible*
file in it. (`NETHELLO` still exists as a handshake-only probe: `make net`.)

**7d resume proof (2026-06-25):** two qemu boots against the same agent + a fixed
`--staging-dir`, with `RB_SERVE_TEST_DROP_AFTER_CHUNKS=2` on RUN 1 so the daemon
drops after 2 committed spans (`journal.json` shows `committed_chunks:2`), then
RUN 2 (no drop) where the same `CRUSTYBK BACKUP rb://...` reconnects and the daemon
logs *"PUT resuming (fingerprint … matches)"* → completes → restore byte-identical.
**Two-qemu gotcha:** each FreeDOS boot + backup takes ~30–60 s, over the Bash
tool's 120 s foreground cap — run the multi-boot script with `run_in_background`
and poll its output file, and `pkill -9 qemu-system-i386` between runs (a
killed-but-orphaned qemu keeps the boot.img write-lock and the next boot fails
*"Failed to get write lock"*). The staging must be a fixed `--staging-dir` (not
the default temp) so it survives the daemon restart between runs.

**7e restore proof (2026-06-25):** one qemu boot with **three** drives —
boot (0x80), source FAT (0x81), blank same-size target (0x82) — and an `FDAUTO.BAT`
that runs `CRUSTYBK BACKUP rb://10.0.2.2:7341/MYDISK 81` then `CRUSTYBK RESTORE
rb://10.0.2.2:7341/MYDISK 82 /Y` against one `rb-cli serve --root`. The host logs a
`PUT … complete` then a `GET … serving N member(s)`; pull 0x82's files with `mcopy`
and `cmp` against the source. The target must be `>=` the source size (same-size
restore). The guest console isn't captured by `-display none` without `-serial`, so
read success from the serve log + the byte compare.

**7f manifest/idempotency proof (2026-06-25):** one boot with three drives — boot
(0x80), source FAT (0x81), blank same-size target (0x82) — running `BACKUP C:\BK1 81`
→ `RESTORE C:\BK1 82 /Y` → `BACKUP C:\BK2 82`, then `cmp BK1\manifest-0.json
BK2\manifest-0.json` off the boot disk → **byte-identical** (the no-op proof; the
source 0x81 is only read, never written). Build the source with a real directory
tree (nested dirs + an LFN file + root `IO.SYS`/`MSDOS.SYS`/`COMMAND.COM`) so the
walk, the sysfiles block, and LFN reassembly are all exercised. To test the
**vendored** `DOSLFN.COM` (not FD14's), copy `crusty-backup/media/DOSLFN.COM` into
`\CB` and load it with a bare `DOSLFN` from there (as `cbdos-autoexec.bat` does from
the media root). Network half: `BACKUP rb://10.0.2.2:7341/MYDISK 81` (NE2000+SLiRP,
no DOSLFN needed — the net path writes no DOS files) → the serve log shows a
**4-member** PUT → `rb-cli cbk unpack MYDISK.cbk` → its `manifest-0.json` is
byte-identical to the local one (same source disk).

**Net gotchas (do not relearn):** (1) FreeCOM mis-parses `2>` / `2>&1` (the `2`
becomes an argv) — pass the port explicitly, use single `>`. (2) `CWSDPMI.EXE`
must travel next to the net `.EXE`. (3) `my_ip = dhcp` is the whole network config
under SLiRP. (4) The protocol is unauthenticated/unencrypted by design (isolated
host↔vintage LAN; §1e) — do not bolt on crypto. (5) **Local-folder** backup/restore
needs **DOSLFN** loaded — the member names (`metadata.json`/`partition-N.gz`/
`manifest-N.json`) aren't 8.3 and the FreeDOS kernel has no LFN API; the boot media
now auto-loads the vendored `DOSLFN.COM`. The `.cbk` + network paths don't need it
(those names never become DOS files).

## Key files

- **Host:** `src/remote/protocol.rs` — handshake + the **PUT framing** (`PUT_MAGIC`
  / `MEMBER_MAGIC` / `RESUME_MAGIC`, `PutHeader{fingerprint}` /`MemberHeader`/
  `ChunkHeader` / `ResumeEntry`, the `read_*`/client-side `write_*` pairs incl.
  `write_resume_map`, `write_put_ack`/`write_put_result`). `src/remote/server.rs`
  — `handle_family_b` (reads the post-handshake frame: EOF=clean close, `RBKP`=PUT)
  + `receive_put` (persistent staging + `ResumeJournal`/`journal.json`,
  fsync-then-record, truncate-to-committed on a fingerprint match,
  `fill_partition_checksums` at finalize, then `pack_folder_to_cbk`) +
  **`serve_get`** (GET: advertise members, then stream each — `*.gz` raw,
  Raw decompressed). One post-handshake dispatcher: `read_family_b_op`/`FamilyBOp`
  (PUT/GET). The `RB_SERVE_TEST_DROP_AFTER_CHUNKS` knob lives here.
- **`.cbk` format:** `src/rbformats/cbk.rs` (RBKC/RBKI/RBKF, `pack_folder_to_cbk`,
  `materialize_cbk_to_folder`, `read_cbk_index`, `CbkPayloadReader` /
  `cbk_member_content_reader` — the GET source) + `src/rbformats/gz_index.rs`
  (`GzSpan`). **Reuse these — the DOS producer must emit identical bytes.**
- **DOS networked backup + restore (baked into `CRUSTYBK`):**
  `crusty-backup/src/cbnet.{h,c}` — WATT-32 sockets + `cbnet_parse_url`; the **PUT**
  side (chunk framing, span streamer `cbnet_part_*`, `read_resume_map`); the **GET**
  side (`cbnet_start_get`, `cbnet_get_raw`, the inflating `cbnet_get_member_*` with
  manual multi-member `inflateReset`). `cmd_backup.c` — `cmd_netbackup` (§4
  fingerprint + `netstream_fat`/`_ntfs_partition`). `cmd_restore.c` —
  **`cmd_netrestore`** (GET metadata/mbr, `netrestore_partition` streams gz →
  int13h same-size, `scan_parts`/`rebuild_ebr_chain` shared with the local path).
  `net_hello.c` (`NETHELLO.EXE`, `make net`) is the standalone handshake probe.
- **File manifest (7f):** `crusty-backup/src/cbmanifest.{h,c}` — `manifest_build_fat`
  walks the live FAT volume read-only via `cbbrowse` (a grow-on-demand JSON buffer;
  `dirent_t` gained `dos_time`/`dos_date`) and emits the §5 `manifest-N.json`.
  Called by `cmd_backup`'s `emit_partition_manifest` (local) and `cmd_netbackup`
  (Raw member; the PUT member count = `1 + nsel + n_fat_manifests + 1`). The
  desktop carries it for free — `pack_folder_to_cbk` / `receive_put` already pack
  arbitrary members; `cbk.rs`'s round-trip test now covers a manifest member.
- **Boot media (DOSLFN):** `crusty-backup/media/DOSLFN.COM` (vendored adoxa/doslfn
  v0.42 + `DOSLFN-ATTRIBUTION.md`), shipped at the media root by `mkmedia.sh` and
  auto-loaded in `media/cbdos-autoexec.bat` so local-folder backups' non-8.3 names
  work on a bare DOS host.
- **The local engines the net paths mirror:** `cmd_backup.c`'s
  `backup_fat_partition`/`backup_ntfs_partition` (smart-compaction) and
  `cmd_restore.c`'s `restore_partition` (the file path, with peek+resize — the
  forward-only socket path can't peek, so resize-over-the-wire is deferred),
  `cbdisk.{h,c}`, `cbcodec.{h,c}`.

The **core network feature is complete** through 7g (handshake → chunk PUT →
block-level backup → resume → restore → manifest/idempotency → boot hashes + swap
exclusion), all qemu-verified. What's left (7h incremental, 7i Level-2 dealloc +
desktop swap parity) is **optional** — start by reading the four docs, then pick
the top unchecked box in §9 if continuing. Keep each box small (one commit or a
small handful), verify on real FreeDOS in qemu, and tick §9 + refresh the resume
as you go.
