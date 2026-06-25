# crusty-backup (`cb-dos`) — **network phase** resume prompt

Hand-off for continuing **Net 7c–7i** (networked backup/restore over TCP). The
local removable-media engine is complete and qemu-verified; networking only swaps
the *destination* under it. **7a (handshake) and 7b (chunk PUT → host `.cbk`) are
done and qemu-verified.** Paste the section below (from "Resume the…" down) to
kick off the next session.

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
`7c — Whole-folder backup over wire`.**

## Where we are

Branch `cbdos` (off `main`), ~48 commits ahead, tree clean, **everything below
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
- **Net 7b — DONE.** The **chunk PUT** streams a `CRUSTYBK BACKUP` folder DOS→host
  and the host assembles a frozen `.cbk` — qemu-verified byte-identical to a
  desktop pack, and `rb-cli restore` rebuilds the disk. DOS client:
  `crusty-backup/src/net_put.c` (`NETPUT.EXE`, `make net`): handshake, then per
  member an `RBKM` `{kind, name, chunk_count}` + per chunk `{src_offset, len,
  crc32}` + payload, **stop-and-go**. Host: `src/remote/protocol.rs`
  (`read_put_header`/`read_member_header`/`read_chunk_header` + the client-side
  writers) + `server.rs` `receive_put` (stage members to a temp folder, CRC-check,
  fsync, then `pack_folder_to_cbk` atomic `.tmp`+rename). `CAP_FAMILY_B` advertised.
  Loopback test `family_b_chunk_put_assembles_cbk_over_loopback`.
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

## What 7b already gave us (so 7c is small)

`NETPUT.EXE` already streams a **whole** `CRUSTYBK BACKUP` folder (every member —
metadata.json, mbr.bin, partition-N.gz, the .crc32 sidecars) DOS→host, the host
assembles the frozen `.cbk` (reusing `pack_folder_to_cbk`, byte-identical to a
desktop pack), and `rb-cli restore MYDISK.cbk` rebuilds the disk. So the wire
protocol + container + whole-folder transfer + materialize/restore are **done**.

## 7c — what "done" looks like

The gap 7b left: today the DOS box must **image to a local folder first**
(`CRUSTYBK BACKUP C:\BK 81`) and then `NETPUT` it — two steps, and it needs a
spare DOS disk big enough for the folder. 7c removes the intermediate folder by
**imaging the live disk straight to the wire**:
- **DOS client:** teach the imaging engine (`cmd_backup.c` / `cbdisk`) to stream
  each member to the agent **as it images** — open the socket, send the PUT +
  member headers, and pipe the compressed `partition-N.gz` member out in chunks
  instead of (or as well as) to a `gzFile` on a DOS disk. Likely a `netbackup`
  subcommand or a `\\host:port` destination on `backup`, reusing `net_put.c`'s
  framing helpers. metadata.json/mbr.bin are tiny — buffer + send last (§3
  ordering). One ordered chunk stream, still stop-and-go.
- **Host:** unchanged — `receive_put` already assembles any member stream.
- **Verify:** a one-step networked backup of a live FAT (and NTFS) disk →
  `MYDISK.cbk` → `rb-cli restore` rebuilds it, files byte-identical, on qemu.

After 7c, **7d — resume**: per-span chunks (cb-dos emits source-span multi-member
gzip like the desktop does — `gz_index.rs`), fsync-before-record into the `.cbk`
incrementally, the `.idx` resume log, the `RESUME` handshake + §4 fingerprint, and
the truncate-to-last-committed receive loop. That's where the streaming-append
(vs. the 7b stage-then-pack) and the incremental index actually land.

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
a one-line `WATTCP.CFG` = `my_ip = dhcp` (SLiRP's DHCP does the rest), then run the
DOS client against `10.0.2.2 7341`. The 7a proof: `NETHELLO 10.0.2.2 7341` printed
*"Connected. Agent protocol v2, capabilities 0x0001 [file]"*. The **7b** proof
(2026-06-25): `FDAUTO.BAT` = `NE2000 0x60 3 0x300` → `CRUSTYBK BACKUP C:\BK 81` →
`NETPUT 10.0.2.2 7341 C:\BK MYDISK` → `FDAPM POWEROFF`, with the host serving
`--root /tmp/agent`; the host logged *"PUT \"MYDISK\" (4 members) … complete …
(21824 bytes)"*, the `.cbk` was byte-identical to `rb-cli cbk pack` of the mcopy'd
folder, and `rb-cli restore` rebuilt the disk (both files byte-identical). Copy
`CRUSTYBK.EXE`, `NETPUT.EXE`, `CWSDPMI.EXE`, `NE2000.COM`, `WATTCP.CFG` into `\CB`;
run from there (CWSDPMI + WATTCP.CFG must be reachable from the cwd). Grow the
imaging engine into a one-step streaming backup for 7c.

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
  atomic `.tmp`+rename). **For 7c:** the host needs no change — `receive_put` already
  assembles any member stream.
- **`.cbk` format:** `src/rbformats/cbk.rs` (RBKC/RBKI/RBKF, `pack_folder_to_cbk`,
  `materialize_cbk_to_folder`, `read_cbk_index`) + `src/rbformats/gz_index.rs`
  (`GzSpan`, the `.gz.idx` seek layout — for 7d per-span chunks). **Reuse these —
  the DOS producer must emit identical bytes.**
- **DOS net clients:** `crusty-backup/src/net_put.c` (`NETPUT.EXE`) — the 7b
  whole-folder PUT client (hand-rolled CRC32, BE field writers, `opendir`/`readdir`
  enumeration, `put_member` stop-and-go). `net_hello.c` (`NETHELLO.EXE`) — the
  handshake-only probe. Both `make -C crusty-backup net` (WATT-32 only, no zlib).
- **The local engine 7c streams from:** `crusty-backup/src/cmd_backup.c`
  (smart-compaction + the §2c member shape — teach it to pipe members to the socket
  via `net_put.c`'s framing instead of/as well as a `gzFile`), `cbdisk.{h,c}`,
  `cbcodec.{h,c}`.

Start by reading the four docs, then plan and implement **7c**. Keep each box small
(one commit or a small handful), verify on real FreeDOS in qemu, and tick §9 +
refresh the resume as you go.
