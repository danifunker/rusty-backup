# crusty-backup (`cb-dos`) — **network phase** resume prompt

Hand-off for continuing **Net 7d–7i** (networked backup/restore over TCP). The
local removable-media engine is complete and qemu-verified; networking only swaps
the *destination* under it. **7a (handshake), 7b (chunk PUT protocol + host
`.cbk`), and 7c (block-level networked backup baked into `CRUSTYBK BACKUP
rb://...`) are done and qemu-verified.** Paste the section below (from "Resume
the…" down) to kick off the next session.

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
`7d — Resume` (or `7e — Restore over wire` if you'd rather close the loop
first).**

## Where we are

Branch `cbdos` (off `main`), ~50 commits ahead, tree clean, **everything below
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
  Raw members (metadata last, with the per-partition gz CRC accumulated mid-stream).
  `cmd_backup.c` detects the `rb://` dest (`parse_rb_dest`/`cmd_netbackup`);
  `build_metadata` is now shared by the local + net paths. `CRUSTYBK.EXE` links
  WATT-32. Host unchanged. qemu-verified: a 3.5 MiB disk → 4 spans → multi-member
  `.cbk` → `rb-cli restore` byte-identical. *(Primaries + FAT/NTFS, gzip only;
  extended/logical, `/DEFRAG`, LZ4 over the wire compose later.)*
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

## What 7a–7c already give us (the working baseline)

A vintage box boots cb-dos and runs one command —
`CRUSTYBK BACKUP rb://<agent-ip>:7341/MYDISK 81` — which images the disk
**block-level over int13h** and streams it straight to `rb-cli serve` as gzip-member
spans; the agent assembles `MYDISK.cbk` and `rb-cli restore MYDISK.cbk` rebuilds the
disk. No local folder, no second tool. The cb-dos producer (`cbnet.c`) emits **1 MiB
gzip-member spans** as chunks; the host (`receive_put`) concatenates them into a
multi-member `partition-N.gz` and packs the frozen `.cbk`. **Block-level, baked in.**

## 7d — resume (what "done" looks like)

7c streams stop-and-go but a crash/reboot loses everything — there's no resume yet.
7d makes the transfer **resumable** (§3 + §4):
- **Host:** instead of stage-then-pack, write the `.cbk` **incrementally** — append
  each span chunk as an `RBKC` chunk, fsync-before-record, and maintain the `.idx`
  sidecar (atomic rename) as the durable transfer map; truncate to the last
  committed chunk on reconnect. The `.cbk` writers in `cbk.rs` are the reference
  for the exact bytes (don't drift).
- **DOS client:** on reconnect send a `RESUME session, source=<fingerprint>` (§4:
  geometry + MBR/ptable CRC + volume serials + **allocation-bitmap CRC** — cheap,
  already walked for compaction); the host replies "resume member K at span S", and
  cbnet `int13h`-seeks to S·SPAN and continues (never recompresses — the span model
  is built for this). The §4 fingerprint guards against a swapped/edited card.
- **Verify:** kill mid-transfer, reconnect, finish; the assembled `.cbk` restores
  byte-identical and the per-partition checksum matches.

Alternatively close the producer/consumer loop first with **7e — restore over the
wire** (host serves members on a `GET`; cb-dos restores with resize; restored card
boots). Either is a fine next box; pick from §9.

## How we work

- **One small commit per step on `cbdos`** (never `main`). End commit messages with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
  The pre-commit hook runs `cargo fmt` + `clippy --all-targets -D warnings` on Rust
  changes (no Rust staged → it skips). New files need an explicit `git add`.
- **Build.** Host: `cargo build --bin rb-cli`; `cargo test --lib` (2093+ tests).
  DOS net client: `sh crusty-backup/net/fetch-watt32.sh` once, then
  `make -C crusty-backup net` (WATT-32 = DJGPP BSD sockets, gitignored-but-present).
  The main tool: `make -C crusty-backup crustybk` (needs `deps/fetch-zlib.sh` +
  `deps/fetch-lz4.sh`).
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

**Net gotchas (do not relearn):** (1) FreeCOM mis-parses `2>` / `2>&1` (the `2`
becomes an argv) — pass the port explicitly, use single `>`. (2) `CWSDPMI.EXE`
must travel next to the net `.EXE`. (3) `my_ip = dhcp` is the whole network config
under SLiRP. (4) The protocol is unauthenticated/unencrypted by design (isolated
host↔vintage LAN; §1e) — do not bolt on crypto.

## Key files

- **Host:** `src/remote/protocol.rs` — handshake + the **PUT framing** (`PUT_MAGIC`
  / `MEMBER_MAGIC`, `PutHeader`/`MemberHeader`/`ChunkHeader`,
  `read_put_header`/`read_member_header`/`read_chunk_header` + the client-side
  writers used by the loopback test, `write_put_ack`/`write_put_result`).
  `src/remote/server.rs` — `handle_family_b` (reads the post-handshake frame:
  EOF=clean close, `RBKP`=PUT) + `receive_put` (stage members → `pack_folder_to_cbk`
  atomic `.tmp`+rename). **For 7d** this stage-then-pack host becomes an
  incremental streaming-append + `.idx` resume log; for 7c it was unchanged.
- **`.cbk` format:** `src/rbformats/cbk.rs` (RBKC/RBKI/RBKF, `pack_folder_to_cbk`,
  `materialize_cbk_to_folder`, `read_cbk_index`) + `src/rbformats/gz_index.rs`
  (`GzSpan`, the `.gz.idx` seek layout — for 7d per-span chunks). **Reuse these —
  the DOS producer must emit identical bytes.**
- **DOS networked backup (baked into `CRUSTYBK`):** `crusty-backup/src/cbnet.{h,c}`
  — WATT-32 sockets + the chunk-PUT framing (hand-rolled BE writers, send_chunk +
  stop-and-go ack) + the **zlib gzip span streamer** (`cbnet_part_begin`/
  `_part_write`/`flush_span`/`_part_end`, `cbnet_raw_member`). `cmd_backup.c` —
  `parse_rb_dest` + `cmd_netbackup` + `netstream_fat_partition`/
  `netstream_ntfs_partition` (the int13h block loop + smart-compaction feeding the
  span streamer) + the shared `build_metadata`. **For 7d:** add per-span resume
  (seek by source offset) + the `RESUME`/fingerprint handshake. `net_hello.c`
  (`NETHELLO.EXE`, `make net`) is the standalone handshake probe.
- **The local engine the net path mirrors:** `cmd_backup.c`'s local
  `backup_fat_partition`/`backup_ntfs_partition` (same smart-compaction; the net
  path streams spans instead of writing a `gzFile`), `cbdisk.{h,c}`, `cbcodec.{h,c}`.

Start by reading the four docs, then plan and implement **7d** (resume) or **7e**
(restore over the wire). Keep each box small (one commit or a small handful),
verify on real FreeDOS in qemu, and tick §9 + refresh the resume as you go.
