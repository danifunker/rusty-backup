# RESUME: get the emulated Power Mac G5 booting (m900)

A focused hand-off for a session running **directly on m900**. The background, machine details and file layout are in [RESUME-native-frontends-and-g5-vm.md](RESUME-native-frontends-and-g5-vm.md) §2, §4 and §5. This file is the short version, plus the boundaries for this session.

## Paste this into a new session on m900

> Read `docs/RESUME-g5-emulator.md` in `~/repos/rusty-backup-native-frontends`.
> First run `ssh-add ~/.ssh/id_rsa`; I'll type the passphrase.
> Goal: get the emulated G5 in `/run/media/dani/BigDisk/rb-vms/g5-leopard/` to boot Leopard 10.5.8 all the way to a working sshd on `127.0.0.1:2222`, with gcc 10 usable inside. Then make it a usable `PPC_HOST` and compare it against the real G5.
> Another session is doing the crate split on branch `native-frontends`. Don't edit `src/`, `crates/` or `Cargo.*`. Put any scripts or docs you add under `scripts/g5-vm/` and `docs/`, and commit them on a new branch `g5-emulator` in a separate worktree.

## Goal and done-criteria

1. Leopard boots to multi-user. **Done when** `ssh -p 2222 admin@127.0.0.1` works from m900 (legacy RSA options, as in m900's `g5` host entry). It's the same install as the real G5, so the same users and authorized keys.
2. `/opt/local/libexec/gcc10-bootstrap/bin/gcc --version` prints 10.5.0 inside the guest.
3. An ssh host entry (say `g5vm`: HostName 127.0.0.1, Port 2222, legacy RSA, no host-key checking) lets `PPC_HOST=g5vm` drive `scripts/build-ppc.sh`. Compare one build, and `scripts/ppc-smoke.sh`, against the real G5 (`PPC_HOST=g5`).
4. Write up what worked in `docs/` (a G5-VM page). Make `run-g5.sh` reproducible (check it into `scripts/g5-vm/`).
5. Stretch, the user's real goal: a **slim** Leopard disk for GitHub runners. See §5 of the main resume doc (target ~12 GiB volume; image must stay private).

## Where it stands (2026-10-30)

- **Files** in `BigDisk/rb-vms/g5-leopard/`:
  - `g5-leopard-boot.img`: 87 GB sparse, APM, Leopard in slot 2 at sector 2048. **Boot this one.**
  - `g5-leopard-boot-overlay.qcow2`: the VM's writes go here; recreate it freely.
  - `g5-leopard.img`: the original 256 GB layout. Tiger 10.4.12 is in slot 3.
  - `qemu-g5/squashfs-root/`: the extracted qemu-g5-appimage v1.0.0, a patched QEMU 11.1.50.
  - `run-g5.sh`, `vmcmd.py` (monitor helper: `./vmcmd.py monitor.sock screendump shot.png -f png`).
- **Working:**
  - G5 machine with the bundled OpenBIOS (970FX, up to 4 CPUs)
  - `-vga std`
  - BootX loads from `hd:2,\\:tbxi`
  - the Darwin 9.8.0 kernel starts
- **Stuck:** with `boot-args=-v`, the kernel prints `vm_page_bootstrap` and then **hangs after `mig_table_max_displ = 79`**, at 100% of one core. Not SMP (1 CPU hangs too), not memory size (2 GB hangs too).
- **Known traps:**
  - `-device ati-radeon9800` hung OpenBIOS before display init when calling the binary directly.
  - The emulated IDE / OpenBIOS can't reach past ~128 GiB, which is why the disk was re-laid out.
  - `nvram.img` stores the startup disk; delete it to reset.
  - Bracket `pgrep -f` patterns (`[q]emu-g5`), or they match themselves.
  - Use `setsid nohup … &` so the VM survives the ssh session.

## Next things to try, in order

1. **Launch through the bundle's wrapper**, not the bare binary. `AppRun` / `g5` may set environment the binary needs, which might also explain the Radeon hang:
   ```bash
   D=/run/media/dani/BigDisk/rb-vms/g5-leopard
   QEMU_G5_STATE=$D QEMU_G5_DISK=$D/g5-leopard-boot-overlay.qcow2 QEMU_G5_NET=none \
     $D/qemu-g5/squashfs-root/AppRun \
     -prom-env "boot-args=-v" -prom-env "boot-device=hd:2,\\:tbxi" \
     -netdev user,id=net0,hostfwd=tcp:127.0.0.1:2222-:22 -device sungem,netdev=net0 \
     -display none -vnc 127.0.0.1:5 -monitor unix:$D/monitor.sock,server,nowait
   ```
   It defaults to the Radeon 9800, 2 CPUs and 2 GB, and passes extra arguments straight to QEMU.
2. Verbose-boot **Tiger** (`g5-leopard.img`, `boot-device=hd:3,\\:tbxi`) to see whether the hang is Leopard-specific.
3. Boot args: `-v -x` (safe mode, skips kext caches); `-v debug=0x14e`; `cpus=1`.
4. Read the Emaculation threads the bundle credits, for the exact Leopard-on-G5 settings: Cat_7 / Andrew_R, t=12804 and t=12817.
5. Only if all else fails: build the bundle's QEMU from source (`v11.1.0-rc2-1511-g70668cf600`) to debug the guest with `-d` / gdbstub.

## Don't

- Don't touch the real G5's disks; it's only an ssh build host.
- Don't publish the disk image anywhere public: Mac OS X can't be redistributed.
- Don't edit the crate-split files (see the prompt).
