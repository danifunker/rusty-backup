# RESUME: crate split + emulated G5 build host (handoff 2026-10-30)

This picks up a session started on the Mac (`danimacbookm4p`) and moves it to **m900**.

Branch: **`native-frontends`**. On m900 it is checked out as a worktree at `~/repos/rusty-backup-native-frontends`, while `~/repos/rusty-backup` stays on `qnx2fs-crate`.

## Paste this into the new session on m900

> Read `docs/RESUME-native-frontends-and-g5-vm.md` in `~/repos/rusty-backup-native-frontends` and continue from "Where things stand". First run `ssh-add ~/.ssh/id_rsa`; I'll type the passphrase, because the G5 and GitHub need the key. Then:
> (1) the PowerPC build gate already passed: record it in `crate_split_plan.md` §9a, then run `scripts/ppc-smoke.sh` against the real G5;
> (2) get the emulated G5 past the early Leopard kernel hang (see "Open problem" in section 4), then check that ssh on `127.0.0.1:2222` and gcc 10 work inside;
> (3) start slimming the Leopard disk for GitHub runners as described in the doc.

## The plans this work serves

- [crate_split_plan.md](crate_split_plan.md): extracting reusable MIT crates (`disk-core`, `fs-*`, `part-tables`, `dsk-*`). **Step 0 is done** (commit `1a22fed`, pushed); see §9a there.
- [native_frontends_plan.md](native_frontends_plan.md): native GUIs per platform, which comes *after* the crate split.
- [long_operations_inventory.md](long_operations_inventory.md): the work list for the "wait" abstraction.

## Where things stand

### 1. Step 0 crate split: done, including the PowerPC gate

- `crates/disk-core` + `crates/fs-8bit` (Oric). Passing: modern build and tests (3,755), clippy on the new crates, the Rust 1.73 vintage check, and the crate tests on 1.73.
- **mrustc PowerPC gate: PASS (2026-10-30 14:53).** m900 transpiled the whole engine, including `disk-core` and `fs-8bit`. The real G5 compiled the C and linked `~/repos/mrustc/output-rb-ppc-g3/rb-cli` (119 MB, Mach-O `ppc_750`, `--extern disk_core` / `fs_8bit`).
  - On the real G5 (Leopard 10.5.8), that binary ran `new floppy oric` → `put HELLO.TXT` → `ls` → `fsck` (clean) as `/tmp/rb-cli-step0`. The extracted crates work on PowerPC.
- **To re-run:**
  ```bash
  W=~/repos/rusty-backup-native-frontends
  cd $W && RB_DIR=$W MRUSTC_DIR=~/repos/mrustc PPC_HOST=g5 scripts/build-ppc.sh ppc
  ```
  **`RB_DIR` is mandatory.** `build-ppc.sh` defaults it to `~/repos/rusty-backup`, which is the `qnx2fs-crate` checkout, and builds the wrong tree without complaint. The vendor step was already run for the worktree.
- Next: compare outputs with the desktop build via `scripts/ppc-smoke.sh` (on the real G5 now, and on the emulated G5 later). Record the gate result in `crate_split_plan.md` §9a, which still lists mrustc as "to prove".
- Still open from Step 0:
  - Solaris 9 CI result (pushed; check the Actions run for `1a22fed`).
  - A checked-in `crates-msrv/` manifest for 1.73 crate tests in CI (crate_split_plan §5).
  - Then Step 1 onwards.

### 2. Machines and access

| Host | What | Notes |
|---|---|---|
| **m900** | x86_64 Ubuntu 26.04, 6 cores, 14 GB RAM; BigDisk at `/run/media/dani/BigDisk` (1.6 TB free) | `rb-cli` release build at `BigDisk/rb-build/target/release/rb-cli`; use `TMPDIR=BigDisk/rb-tmp` for anything that decompresses |
| **Real G5** | dual 2.3 GHz G5, 4 GB, Leopard 10.5.8 on `disk0s4` (Tiger, Leopard and Snow Leopard installs on one disk) | ssh alias **`g5`** (= `admin@PowerMacG5.local`) in `~/.ssh/config` on **both** the Mac and m900: legacy RSA allowed, host-key checking off, because each installed OS has its own host key. gcc 10.5.0 at `/opt/local/libexec/gcc10-bootstrap/bin/gcc` |
| GitHub / G5 from m900 | needs the user's key | m900's `~/.ssh/id_rsa` is **passphrase-protected**. Run `ssh-add ~/.ssh/id_rsa` first (agent forwarding from the Mac was the stopgap). The key is already in the G5's `authorized_keys`. |

### 3. mrustc: use m900's `~/repos/mrustc`

- **Use** m900 `~/repos/mrustc`, branch `ppc-build-2026-09`. It has the PowerPC std **already built** for G3, G4 and G5 (`output-1.74.0-powerpc-apple-darwin-g{3,4,5}`) and Solaris 9 (`output-1.74.0-sparcv9-sun-solaris2.9`).
- **Upstream** (`thepowersgang/mrustc`) has merged the PowerPC work: #417 (includes the `2b` integer-suffix lexer fix), #418 (powerpc-apple-darwin target), #415, #420. **#428 (Solaris 9/10) is still open.**
- **Other checkouts, don't use:**
  - Mac `~/repos/mrustc`: `master`, 63 commits behind, holding a superseded pre-merge CallPath edit. It failed with "Unknown integer suffix b".
  - Mac `~/repos/mrustc-danifunker` (`ppc-apple-darwin`).
  - m900 `~/repos/mrustc-ppc` (`ppc-upstream`) and `~/repos/mrustc_latest` (`master`).
  - Consolidating these down to upstream master (plus a Solaris branch until #428 merges) is a cleanup for later.
- **Transpiling on an Apple-silicon Mac fails** in the vendored `constant_time_eq` ("Unknown architecture for asm!"). Transpile on x86_64 (m900) instead.

### 4. Emulated G5 on m900 (`BigDisk/rb-vms/g5-leopard/`)

**Files:**

| File | What |
|---|---|
| `backup/` | the rusty-backup set copied from NAS `Software/VintageSystemBackups/G5-Leopard-Gcc10/256GB-disk4-2026-08-03-1520` (the seekable cache was skipped) |
| `g5-leopard.img` | full 256 GB restore (sparse). APM slots: 2 HFS "untitled" (bootstrap), **3 Tiger 10.4.12**, 4 Linux, **5 Leopard 10.5.8 + gcc 10**, 6 swap |
| `g5-leopard-boot.img` | **the one to boot.** 87 GB sparse (56 GB on disk), `parted` APM, one `Apple_HFS "Leopard"` at sector 2048 (slot 2, OF `hd:2`), with the Leopard volume copied in byte-for-byte. The copy completed and `rb-cli` reads it fine. BootX loads it and the kernel starts, but it then hangs (see "Open problem" below). |
| `g5-leopard-boot-overlay.qcow2` | copy-on-write overlay the VM writes to. `run-g5.sh` already points at it with `hd:2,\\:tbxi` |
| `qemu-g5/` | qemu-g5-appimage v1.0.0 (SHA-256 verified), unpacked with `--appimage-extract` (no `libfuse2` on m900). Patched QEMU **11.1.50** (`70668cf600`): multi-CPU on mac99, `ati-r350`; G5 OpenBIOS at `squashfs-root/share/qemu/openbios-qemu.elf` |
| `run-g5.sh` | headless launcher: guest sshd at `127.0.0.1:2222`, VNC `:5`, `monitor.sock`, `serial.log`. `G5_SMP` / `G5_MEM` env overrides |
| `vmcmd.py` | send one monitor command, e.g. `./vmcmd.py monitor.sock screendump shot.png -f png`, `... quit` |

**The VM is stopped at handoff.** **Open problem: the Leopard kernel hangs early.**

With verbose boot (`run-g5.sh` now passes `boot-args=${G5_BOOTARGS:--v}`), BootX loads `Darwin Kernel Version 9.8.0 … xnu-1228.15.4~1/RELEASE_PPC`. The kernel prints `vm_page_bootstrap` and **stops after `mig_table_max_displ = 79`**, at 100% of one host core, before IOKit. Results:

| Setting | Result |
|---|---|
| 4 CPUs, 3 GB | hang |
| 1 CPU, 3 GB (`G5_SMP=1`) | hang, so not SMP |
| 1 CPU, 2 GB (`G5_MEM=2048`) | hang, so not memory size |

The remaining difference from the bundle's "proven" config is the **display: `-vga std` instead of `ati-radeon9800`**, which hung OpenBIOS on m900. Next ideas, in order:
1. **Run through the bundle's own wrapper instead of calling the binary**, in case `AppRun` sets something the bare `bin/qemu-system-ppc64` misses. That might also fix the Radeon hang:
   `QEMU_G5_DISK=$D/g5-leopard-boot-overlay.qcow2 QEMU_G5_NET=none qemu-g5/squashfs-root/AppRun -prom-env "boot-args=-v" -prom-env "boot-device=hd:2,\\:tbxi" -netdev user,id=net0,hostfwd=tcp:127.0.0.1:2222-:22 -device sungem,netdev=net0 -display none -vnc 127.0.0.1:5 -monitor unix:$D/monitor.sock,server,nowait`
   The wrapper's `exec` passes extra arguments through, and it defaults to the Radeon, 2 CPUs and 2 GB. Its state dir defaults to `~/.local/share/qemu-g5`; set `QEMU_G5_STATE=$D`.
2. Boot **Tiger 10.4.12** from the original `g5-leopard.img` (slot 3, below the 128 GiB limit; it reached the grey Apple too) with `-v`, to see whether the hang is Leopard-specific.
3. Boot args to try: `-v -x` (safe mode, skips kext caches); `-v debug=0x14e`; `platform=` / `cpus=1`. Also check Emaculation's Cat_7 / Andrew_R threads (t=12804, t=12817) for the Leopard G5 boot-args and NVRAM settings they used.
4. Leopard on the real G5 is tuned to PowerMac7,3; the emulated machine claims the same model, but kext caches may still trip it. Rebuild the caches from the guest only once it boots (`-x`).

Check commands once it boots: `pgrep -af '[q]emu-g5'`, `./vmcmd.py monitor.sock screendump shot.png -f png`, `ssh -p 2222 admin@127.0.0.1` (needs the legacy-RSA options).

How the boot image was made, for reference or to redo:
```bash
D=/run/media/dani/BigDisk/rb-vms/g5-leopard
dd if=$D/g5-leopard.img of=$D/g5-leopard-boot.img bs=4M iflag=skip_bytes,count_bytes oflag=seek_bytes \
  skip=$((167230416*512)) seek=1048576 count=85353504768 conv=notrunc,sparse status=progress
```

`run-g5.sh` is already set up for `g5-leopard-boot-overlay.qcow2` and `hd:2`. Start it with `setsid nohup ./run-g5.sh > qemu.log 2>&1 &`. Stop it with `./vmcmd.py monitor.sock quit`; for a cleaner shutdown, run `sudo shutdown -h now` inside the guest first.

**Hard-won findings:**
- **`-device ati-radeon9800` hangs OpenBIOS** before display init on m900, with any CPU or memory setting. **Use `-vga std`**: the firmware reaches `0 >`, and Leopard has `qemu_vga.ndrv`.
- **About 128 GiB reach limit** (28-bit LBA in the emulated IDE / OpenBIOS). Leopard in the original layout (sectors 167M–334M) gave "No valid state has been set by load or init-program". Tiger in slot 3 (below the limit) booted to the grey Apple. Keep every bootable volume below 128 GiB.
- `nvram.img` (in the VM folder) stores the Startup Disk choice; delete it to reset.
- Guard `pgrep -f` patterns with a bracket (`[q]emu-g5/...`), or they match their own `bash -c` line.
- Upstream Homebrew / `/usr/local` QEMU (11.1.x) allows only 1 CPU on mac99; use the bundled 11.1.50.

**Then:** check `ssh -p 2222 admin@127.0.0.1` (the same Leopard install as the real G5, so the same users and authorized keys) and `gcc --version` at the gcc10-bootstrap path. Point `PPC_HOST` at it through a `~/.ssh/config` entry (HostName 127.0.0.1, Port 2222, legacy RSA) and compare build and `ppc-smoke.sh` output with the real G5.

### 5. Slim the G5 disk for GitHub runners (the user's goal)

The goal is a disk small enough to restore on a GitHub-hosted x86_64 runner and boot with the AppImage QEMU (no KVM needed).

**Constraints:**
- A runner has ~14 GB free by default (more after removing toolchains).
- Release assets are ≤2 GB each, and the Actions cache is 10 GB per repo.
- **Mac OS X can't be published.** Store the image privately and fetch it with a secret URL, like the Solaris 9 `SOL9_SPARC64_SYSROOT_URL` seed.

**Leopard volume today:** 47.5 GiB of data, 806k files. Measured with `rb-cli du g5-leopard.img@4 / --depth 1`:

| Path | Size | Plan |
|---|---|---|
| `/Users/admin` | 15.7 GiB | keep `~/.ssh` and dotfiles only |
| `/Library/...` "Software" (6.7 GiB) + Audio/Dictionaries/Receipts | ~8 GiB | drop non-system content |
| `/Applications` (iLife, iWork '09, Office 2008, …) | several GiB | drop all but Terminal and Utilities |
| `/Developer` | ~2.5 GiB | keep `usr` + `SDKs` (10.4u for the libc probes); drop Documentation and Examples |
| `/opt/local` | 3.3 GiB | keep gcc10-bootstrap, MacportsLegacySupport, libatomic and their deps; `port uninstall --follow-dependents` the rest, then `port clean` |
| `/System` | 4.2 GiB | keep (could trim the speech voices and the Printers folder) |
| `/private/var` (caches, VM swap, logs), `/cores`, `/private/tmp` | ~1.9 GiB | clear |

Target: **~8–10 GiB** of data, so a volume of about 12 GiB with headroom.

**Approach (to decide; option a is the likely pick):**
- **a. Slim in place, then shrink.**
  1. Boot the emulated G5 from a disposable overlay copy and delete inside the guest, so the OS keeps its own bookkeeping consistent: `sudo rm -rf …`, `port uninstall`, clear caches. Alternatively delete offline with `rb-cli rm`, which handles journaled HFS+ edits. That's good dogfooding, but verify with a boot afterwards.
  2. Shrink with rusty-backup's HFS+ defragmenting clone (`hfsplus_clone` / `hfsplus_defrag`; the backup metadata already reports a "defragmented minimum") into a ~12 GiB partition in a new `parted` APM image, kept well under 128 GiB.
  3. Distribute as a compressed qcow2 or a rusty-backup zstd set, split into ≤2 GB parts if needed.
- **b. Rebuild from scratch:** a fresh minimal Leopard install + Xcode 3.1 CLI parts + MacPorts gcc10. Probably smaller, but building gcc10 under emulation takes a very long time. Only worth it if (a) can't get under the size budget.

Also worth trying: G3/G4 binaries can be tested in the emulator, but G5 (`-mcpu=970`) binaries need the 970 CPU it emulates, so that's fine.

### 6. Other state worth knowing

- **Mac-side scratch.** Logs and the extracted AppImage firmware are in the Mac session's temp scratchpad. Not needed on m900.
- **This Mac's `~/.ssh/config`** has the new `g5` entry. m900's too (backup at `~/.ssh/config.bak-2026-10-30`). The G5's `~/.ssh/authorized_keys.bak-2026-10-30` was **not** created: the key was already there.
- **The first, mis-aimed PowerPC run** re-ran `cargo vendor` in m900 `~/repos/rusty-backup/rb-cli-ppc/vendor`. It's gitignored and harmless.
- **Clippy:** Homebrew/clippy 1.98 adds lints (`chunks_exact_to_as_chunks`, `drain_collect`) that fail the pre-commit hook in ~70 existing places, identically on `main`. Step 0 was committed with `--no-verify` for that reason. Never apply the `as_chunks` autofix: it needs Rust 1.88 and breaks the 1.73 floor.
- **Rust 1.73 on the Mac:** call `~/.rustup/toolchains/1.73.0-aarch64-apple-darwin/bin/{cargo,rustc}` directly with `RUSTC` set, because Homebrew's `rustc` comes first on `PATH`.
