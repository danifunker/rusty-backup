# crusty-backup (`cb-dos`) — **network phase** resume prompt

Hand-off for continuing **Net 7e–7i** (networked backup/restore over TCP). The
local removable-media engine is complete and qemu-verified; networking only swaps
the *destination* under it. **7a (handshake), 7b (chunk PUT protocol + host
`.cbk`), 7c (block-level networked backup baked into `CRUSTYBK BACKUP rb://...`),
and 7d (resume — crash-proof transfer) are done and qemu-verified.** Paste the
section below (from "Resume the…" down) to kick off the next session.

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

**Work the TOP UNCHECKED box in §9 of cb_dos_network_and_state.md — currently
`7e — Restore over wire` (close the producer/consumer loop), or `7f — Manifest +
idempotency`.**

## Where we are

Branch `cbdos` (off `main`), ~52 commits ahead, tree clean, **everything below
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

## What 7a–7d already give us (the working baseline)

A vintage box boots cb-dos and runs one command —
`CRUSTYBK BACKUP rb://<agent-ip>:7341/MYDISK 81` — which images the disk
**block-level over int13h** and streams it straight to `rb-cli serve` as gzip-member
spans; the agent assembles `MYDISK.cbk` and `rb-cli restore MYDISK.cbk` rebuilds the
disk. No local folder, no second tool. The transfer is **resumable** (7d): kill it
mid-stream, run the same command again, and it continues from the last committed
span (the §4 fingerprint guards against a swapped card). **Block-level, baked in,
crash-proof.** What's missing is the *other* direction — restore over the wire.

## 7e — restore over the wire (what "done" looks like)

Close the producer/consumer loop: a vintage box with a blank/wrong disk pulls a
`.cbk` back from the agent and restores it, so backup **and** restore both work
over the network.
- **Host:** serve a container's members on a `GET`. The desktop already reads a
  `.cbk` natively (`read_cbk_index`, `cbk_member_content_reader`,
  `materialize_cbk_to_folder`), so the daemon has the bytes — add a Family-B GET
  (request metadata.json + mbr.bin, then stream each `partition-N.gz` member's
  chunks). Mirror the PUT framing.
- **DOS client:** a `CRUSTYBK RESTORE rb://HOST/NAME 81 [/SIZE:...]`. cb-dos already
  has the full restore engine (`cmd_restore.c`: folder → disk, FAT `/SIZE` resize,
  EBR rebuild). Feed it members from the socket instead of a local folder — stream
  the gz bytes into zlib inflate → int13h write. Resize stays as-is.
- **Verify on qemu:** back a disk up over the wire, wipe the target, restore it over
  the wire, and **boot it** (the SYS'd-marker test from cb_dos_resume.md).

After 7e: **7f** (file manifest + idempotency, §5), **7g** (boot section + swap
exclusion), the optional **7h** (incremental), and **7i** (Level-2 swap dealloc;
desktop reads `.cbk` directly). Pick the top unchecked box in §9.

## How we work

- **One small commit per step on `cbdos`** (never `main`). End commit messages with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
  The pre-commit hook runs `cargo fmt` + `clippy --all-targets -D warnings` on Rust
  changes (no Rust staged → it skips). New files need an explicit `git add`.
- **Build.** Host: `cargo build --bin rb-cli`; `cargo test --lib` (2093+ tests) +
  `cargo test --test remote_filesystem --features remote family_b` (the PUT +
  resume loopback tests). The DOS tool: `make -C crusty-backup crustybk` — it now
  links zlib + lz4 + WATT-32, so run all three fetches once (`deps/fetch-zlib.sh`,
  `deps/fetch-lz4.sh`, `net/fetch-watt32.sh`). Networked backup is **in CRUSTYBK**
  (`backup rb://...`), not a separate tool; `make net` still builds the `NETHELLO`
  handshake probe.
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

**Net gotchas (do not relearn):** (1) FreeCOM mis-parses `2>` / `2>&1` (the `2`
becomes an argv) — pass the port explicitly, use single `>`. (2) `CWSDPMI.EXE`
must travel next to the net `.EXE`. (3) `my_ip = dhcp` is the whole network config
under SLiRP. (4) The protocol is unauthenticated/unencrypted by design (isolated
host↔vintage LAN; §1e) — do not bolt on crypto.

## Key files

- **Host:** `src/remote/protocol.rs` — handshake + the **PUT framing** (`PUT_MAGIC`
  / `MEMBER_MAGIC` / `RESUME_MAGIC`, `PutHeader{fingerprint}` /`MemberHeader`/
  `ChunkHeader` / `ResumeEntry`, the `read_*`/client-side `write_*` pairs incl.
  `write_resume_map`, `write_put_ack`/`write_put_result`). `src/remote/server.rs`
  — `handle_family_b` (reads the post-handshake frame: EOF=clean close, `RBKP`=PUT)
  + `receive_put` (persistent staging + `ResumeJournal`/`journal.json`,
  fsync-then-record, truncate-to-committed on a fingerprint match,
  `fill_partition_checksums` at finalize, then `pack_folder_to_cbk`). The
  `RB_SERVE_TEST_DROP_AFTER_CHUNKS` knob lives here. **For 7e** add a Family-B GET
  that streams a `.cbk`'s members back to cb-dos.
- **`.cbk` format:** `src/rbformats/cbk.rs` (RBKC/RBKI/RBKF, `pack_folder_to_cbk`,
  `materialize_cbk_to_folder`, `read_cbk_index`, `cbk_member_content_reader` — the
  GET source for 7e) + `src/rbformats/gz_index.rs` (`GzSpan`, the `.gz.idx` seek
  layout). **Reuse these — the DOS producer must emit identical bytes.**
- **DOS networked backup (baked into `CRUSTYBK`):** `crusty-backup/src/cbnet.{h,c}`
  — WATT-32 sockets + the chunk-PUT framing (hand-rolled BE writers, send_chunk +
  stop-and-go ack, `read_resume_map`/`resume_committed`) + the **zlib gzip span
  streamer** (`cbnet_part_begin{committed_out}`/`_part_write`/`flush_span`/
  `_part_end`, `cbnet_raw_member`). `cmd_backup.c` — `parse_rb_dest` +
  `cmd_netbackup` (computes the §4 fingerprint) + `netstream_fat_partition`/
  `netstream_ntfs_partition` (int13h block loop + smart-compaction feeding the span
  streamer, seeking to the resumed span) + the shared `build_metadata`. **For 7e:**
  feed the existing `cmd_restore.c` engine from the socket instead of a folder.
  `net_hello.c`
  (`NETHELLO.EXE`, `make net`) is the standalone handshake probe.
- **The local engine the net path mirrors:** `cmd_backup.c`'s local
  `backup_fat_partition`/`backup_ntfs_partition` (same smart-compaction; the net
  path streams spans instead of writing a `gzFile`), `cbdisk.{h,c}`, `cbcodec.{h,c}`.

Start by reading the four docs, then plan and implement **7e** (restore over the
wire — close the loop) or **7f** (manifest + idempotency). Keep each box small
(one commit or a small handful), verify on real FreeDOS in qemu, and tick §9 +
refresh the resume as you go.
