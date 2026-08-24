# Tier 7 — Emulator and On-Hardware Verification

Structural correctness is not the same as "the machine boots it". This tier
closes that gap. It is the hardest tier to automate and the one most likely
to stay partly manual forever, so it is explicitly split by what is
scriptable.

Related existing docs, which this tier builds on rather than replaces:

- `docs/mister-deployment-testing-plan.md` — the per-core MiSTer runbook.
- `docs/full_MiSTer_support_status.md` — per-core support matrix.

---

## Tier 7A — Automated

Targets where a run can produce a machine-readable pass/fail with no human
in the loop. Each entry lists its confidence: **proven** means we have done
it, **plausible** means the mechanism exists and needs a feasibility spike
before we depend on it.

### FS-UAE + Kickstart 3.1 — *proven*

Closed R-020 on 2026-08-14. `oracles/fsuae/affs_mount.py` boots a real
Kickstart 3.1, mounts the artifact as DH1:, and gets the verdict out through a
**host directory mounted as an Amiga volume** — the guest writes
`RESULTS:info.txt` and a `done.txt` sentinel, the host polls for it. No screen
scraping. Controlled against 2 MB of noise, which yields no DH1: unit at all,
so the harness demonstrably discriminates.

Covers AFFS today. PFS3 and SFS need their handlers staged into the guest's
`L:` (Kickstart has neither in ROM); the volumes themselves now come from
`rb-cli new volume pfs3` / `sfs`.

### Iris / IRIX 6.5 — *proven*

Found R-039 on 2026-08-13 and confirmed the fix on 08-15; also validated our
SGI volume header against IRIX's own `fx` and `prtvtoc` (F-006 step 1).
`iris-ci` drives it headlessly — `put` / `run` / `scratch write` / `scratch
read` / snapshot rollback, with `run` returning guest stdout. Same
guest-writes-a-result pattern as above. See `oracles/iris/README.md` for the
recipe and the five traps that cost time.

IRIX 5.3 hangs on boot (`Find Error: 10`), so 6.5 is the authority.

### Motion / IRIX 3.7 — *proven*

Wired 2026-08-24, and it is the first oracle this project has had for **EFS
v1** — the IRIS 2000/3000 filesystem, which no Linux kernel and no third-party
tool reads. `oracles/motion/efs_v1_mount.sh` drops our volume into the
reference IRIS 3130's `/usr` slot and boots; the guest's own `/etc/rc.s0`
mounts it and lists it, and the **serial console on stdout** is the verdict
channel — no control socket, no screen scraping. Controlled against 8 MB of
noise, which IRIX refuses with `Invalid argument`.

Weaker than Iris in one respect: the emulator holds the disk read-only and
attaches drive 0 only, so neither guest writes nor a whole disk we partitioned
can be checked yet. See `oracles/motion/README.md`.

### QEMU, headless — *plausible, spike first*

The strongest automation target for the mainstream filesystems. Boot the
image with `-nographic`, serial console redirected to a file, with a payload
on the volume that prints a sentinel and halts. Assert the sentinel appears.

Covers: FAT12/16/32, exFAT, NTFS, ext2/3/4, ISO 9660, UDF, and any container
`qemu-img` accepts (raw, VHD, QCOW2, VMDK).

Spike needed: settle the guest payload per OS family (FreeDOS `AUTOEXEC.BAT`,
a minimal Linux initramfs) and confirm the sentinel round-trips reliably
within a bounded timeout.

### MAME, `-autoboot_script` — *plausible, spike first*

MAME's Lua autoboot can write a file and exit, which gives a real assertion
channel for the CHD path and for the many vintage systems MAME emulates.
Pairs with `chdman info` as a cheap structural pre-check.

Covers: CHD output, plus per-system checks for machines where MAME's driver
is good enough to trust.

### MiSTer over SSH — *plausible, blocked on credentials*

The board is up at `mister.local`. Mechanism:

1. `scp` the built image into `/media/fat/games/<Core>/`.
2. Launch the core by writing an `.mgl` file and poking MiSTer's core-launch
   path.
3. Capture the framebuffer (`/dev/fb0`) after a settle delay.
4. Compare against a stored reference frame with a tolerance, or assert on a
   core-written save/config file where one exists.

The framebuffer-capture-and-compare step is the uncertain part and needs a
spike before the matrix depends on it. A screenshot diff is also the most
fragile assertion in the whole suite — prefer a file-based assertion
wherever a core writes something back.

**Access: resolved.** The board is reachable as `root` with a dedicated key.
Configure the host, user and identity file in the gitignored `local.toml`
(template: `local.toml.example`); no password channel is needed.

The gotcha worth knowing, because it costs an hour every time: `~/.ssh/config`
carries a global

```
Host *
  IdentitiesOnly yes
```

With `IdentitiesOnly yes` and no `IdentityFile` for the board, ssh offers
**no key at all** and fails with
`Permission denied (publickey,password,keyboard-interactive)` — which reads
exactly like "the key is wrong" when in fact the key was never presented.
Pass `-i` explicitly, or give the board its own `Host` block.

Verified 2026-08-02: `Linux MiSTer 5.15.1-MiSTer #6 SMP armv7l`.

### Structural oracles — *proven pattern, already used in `cargo test`*

Not emulators, but they belong to the same "someone else agrees with us"
family and are cheap, so they run first and catch most breakage before an
emulator is ever launched. These are tier 6; see `PLAN.md` phase 7.

---

## Tier 7B — Manual, checklist-driven

Targets where no scriptable assertion exists **yet**. Two former residents of
this section have been promoted to 7A — FS-UAE (2026-08-14) and Iris/IRIX
(2026-08-13) — so the list below is shorter than it was, and the pattern that
promoted them (a host directory the guest writes its verdict into) is the one
to try on the rest.

The runner generates a per-run checklist into `checklists/`, pre-filled with
exactly which artifact to load, what to do, and what a pass looks like. A human
works through it and the results are ingested back into `results.jsonl` with
`"source": "manual"`.

| Emulator | Covers |
|----------|--------|
| WinUAE (FS-UAE promoted to 7A) | PFS3, SFS and RDB *boot* checks; AFFS is automated in 7A |
| Basilisk II / SheepShaver | HFV, HFS, HFS+, APM, DC42, DART, resource forks |
| Mini vMac | MFS, early HFS, 400K/800K floppies |
| 86Box / PCem | FAT, NTFS, HPFS, real DOS/OS-2/Windows boots |
| VICE | CBM DOS across 1541/1571/1581/8050/8250, D64/D71/D81/G64/G71 |
| Hatari | Atari DOS, AHDI, ST/MSA floppies |
| Altirra | Atari DOS 2.0S / 2.5, ATR/XFD |
| Arculator / RPCEmu | Acorn ADFS / FileCore |
| MiSTer cores needing input | Anything requiring controller navigation |
| Real hardware (BlueSCSI, Gotek, CF adapters) | The final word for Mac/Amiga/PC vintage |

VICE, Hatari and Altirra all have batch or headless modes worth a feasibility
spike — any one of them that promotes from 7B to 7A is a large win, since
those three cover a lot of the floppy-format axis.

### Checklist format

One markdown file per emulator per run:

```markdown
## VICE — run 20260801-140000-HOSTNAME-windows

### case fs.cbm.1541.write-roundtrip
Artifact: failures/fs.cbm.1541.write-roundtrip/artifacts/out.d64
Load:     x64sc -8 out.d64
Do:       LOAD"$",8 then LIST
Pass if:  directory lists HELLO.TXT and SUBDIR, blocks free is 658
Result:   [ ] pass  [ ] fail  [ ] not run
Notes:
Screenshot: (drop into screenshots/fs.cbm.1541.write-roundtrip.png)
```

The generator fills in artifact path, load command, expected observation and
the case ID. The human fills in the result. An ingest command reads the
completed checklists back into the run bundle.

---

## Promotion path

A 7B target promotes to 7A when someone finds a scriptable assertion for it.

**Two have promoted**, and both used the same trick — mount a host directory as
a guest volume and have the guest write its verdict into it, so nothing is
screen-scraped:

- **FS-UAE** (2026-08-14) — closed R-020. `oracles/fsuae/affs_mount.py`.
- **Iris / IRIX** (2026-08-13) — found R-039 and validated the SGI volume
  header. `oracles/iris/README.md`.

Try that pattern first on anything below. The remaining order worth attacking,
by coverage gained per unit of effort:

1. QEMU — unlocks the whole mainstream-filesystem axis at once.
2. VICE — unlocks the entire CBM sub-axis, which is otherwise five drive
   models times two container families of manual work every month.
3. MAME — unlocks CHD plus a long tail of vintage systems.
4. MiSTer — high value because it is the real target hardware for much of
   this project, but the assertion channel is the weakest.

---

## Honest scope note

A full manual pass over Tier 7B is a multi-hour human task even with good
checklists, and it will not happen every month. The realistic cadence:

- Tiers 0-6 monthly, automated.
- Tier 7A monthly, automated, once the spikes land.
- Tier 7B before a release, or when tiers 0-6 flag a change in an area a
  7B target covers.

The checklist generator should therefore support filtering to "only cases
whose underlying code changed since the last green run", so a human is never
asked to re-verify forty untouched cases.
