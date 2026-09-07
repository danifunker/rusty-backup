# rb-cli and the TUI on Solaris 9 SPARC, via mrustc

Adding `sparcv9-sun-solaris2.9` to the vintage-style builds, alongside
`rb-cli-vintage` (macOS 10.7 / Windows 7, real rustc 1.73) and `rb-cli-ppc`
(PowerPC Mac OS X, mrustc). Scope is the **CLI and the ratatui TUI** - the same
surface `rb-cli-ppc` ships. No GUI, no CHD.

This is the build guide for the target, grown out of the original scope
notes. It records what has been verified, what has not, and how to rebuild it. The companion manifest is
[`../rb-cli-sol9/Cargo.toml`](../rb-cli-sol9/Cargo.toml); the PowerPC build guide
[`build-ppc-mrustc.md`](build-ppc-mrustc.md) is the template for everything here
and should be read first - most of its traps are mrustc traps, not PowerPC ones,
and apply unchanged.

**Status (2026-09-07): shipped and verified on the hardware.** `rb-cli` runs on
the Sun Blade 2500 under Solaris 9 (`SunOS 5.9 sun4u sparc SUNW,Sun-Blade-2500`),
and **both parity gates pass**: `ppc-smoke.sh` agrees with the desktop build on
`inspect` / `ls` / `fsck` across FAT, HFS and ext and produces byte-identical
backup payloads and checksums, and `ppc-newcode-smoke.sh` agrees on every write
verb, including the big-endian-sensitive AFFS protection longword. Getting there
cost two real runtime bugs, both in finding 10 - a Solaris 10-only `fcntl`
command behind `File::try_clone`, and an **mrustc codegen bug that only appears
on 64-bit big-endian**. The TUI works too, once crossterm is
built with `use-dev-tty` (finding 4).

## What already exists, and is verified

Do not re-derive any of this. It was established by the mrustc Solaris 9 port
and the RustDesk agent that runs on top of it (`~/repos/mrustc/SOL9-RESUME.md`).

| | |
|---|---|
| **mrustc target** | `sparcv9-sun-solaris2.9`, on branch `sparc-solaris-10` (5 commits over `109ddad1`). Handles the Solaris 9 compiler wall itself: `emulated_overflow_intrinsics`, `emulated_c99_math`, `emulated_posix2001`, `-static-libgcc`, and `-lsocket -lnsl -lrt -ldl` |
| **Cross toolchain** | gcc 4.9.4 + binutils 2.35.2 at `~/sol9-toolchain/opt/bin/sparcv9-sun-solaris2.9-*`, sysroot at `~/sol9-toolchain/sysroot`. Reproducible as the `mrustc-sol9-cross` docker image (built, 1.1 GB), which agrees with the native one byte-for-byte on `.text` |
| **Rust stdlib** | **Already built** for the target: 20 rlibs in `~/repos/mrustc/output-1.74.0-sparcv9-sun-solaris2.9/`, including `libstd`, `libpanic_unwind`, `libtest` and `liblibc-0.2.148`. A linked SPARC V9 executable runs on the hardware |
| **C libraries** | `~/sol9-deps/prefix` carries `libz.a` and `libzstd.a` cross-built for the target - the two rusty-backup actually wants - plus libsodium / mbedTLS / libvpx from the agent work |
| **Run target** | Sun Blade 2500 at `user@192.168.99.176`, SunOS 5.9. Only a run target now; nothing compiles there |

The build-script overrides directory `script-overrides/stable-1.74.0-solaris`
also already exists in the mrustc tree.

## The model: one machine, not two

The PowerPC build is a two-machine pipeline because no cross-gcc for
powerpc-apple-darwin exists, so `scripts/ppc-cc-remote.py` ships every `.c` to
the Mac over ssh and copies the `.o` back. **Solaris 9 needs none of that.** A
cross gcc targeting it exists, so the whole build - transpile *and* codegen -
runs here:

```
   This machine (fast)
   -------------------------------------------------------------
   Rust --mrustc--> C99 --sparcv9-sun-solaris2.9-gcc--> SPARC ELF
                                                            |
                                                     scp to the Blade
```

Consequences worth stating plainly, because they delete a large fraction of the
PowerPC build's complexity:

- No `ppc-cc-remote.py` / `ppc-ar-remote.py` equivalent. `CC_sparcv9_sun_solaris2_9`
  and `AR_sparcv9_sun_solaris2_9` point straight at the cross tools. (Note the
  variable name: mrustc commit `71910c7c` sanitises the *whole* triple, so the
  dots become underscores.)
- No orphan-compile reaping, no include-tree mirroring, no 64 MB rsync guard.
- No split-TU machinery. `scripts/ppc-split-tu.py` exists because ld64-85 cannot
  place a branch island inside an input object over ~32 MB of `__text`. GNU ld
  2.35 has no such limit, so the engine can stay one translation unit. **Verify
  this rather than assume it** - the engine's object is ~81 MB on PowerPC, and a
  64-bit SPARC ELF with `-mcpu=v9` will be larger.
- Build wall-clock is bounded by this machine, not by a 1.6 GHz UltraSPARC IIIi.

`~/repos/rustdesk-vintage/rustdesk-sparc-agent/scripts/build-sol9.sh` is a
working example of exactly this shape and is the model for `scripts/build-sol9.sh`.

## Why the engine side is easier here than on PowerPC

The single biggest simplification, and it is worth understanding before writing
any code:

**`powerpc-apple-darwin` reports `target_os = "macos"`.** That is why
`rb-cli-ppc` needs the `os-stub` feature - without it the real `src/os/macos.rs`
compiles for a 2005 Mac, dragging in objc2 bindings to IOKit and DiskArbitration
that cannot exist there.

**Solaris is an unknown `target_os` to this codebase.** `src/os/mod.rs` already
carries `#[cfg(not(any(target_os = "macos", target_os = "linux", target_os =
"windows")))]` arms for exactly this case: `enumerate_devices()` returns an empty
vec, `open_target_for_writing()` bails with "device write access not supported on
this platform", and `open_source_for_reading()` falls through to a plain
`File::open`. So:

- **No `os-stub` feature, and nothing to write.** The fallbacks are already there
  and already correct.
- **`objc2-*`, `windows`, `winreg` and `self-replace` all leave the graph** by
  their existing `[target.'cfg(...)']` gates. `rb-cli-ppc` had to drop the objc2
  stack by hand; here it drops itself. **`nix` does not leave** - it is gone as a
  *direct* dependency but arrives transitively through `rustyline`, and `rustix`
  arrives through `tempfile`. Both cost undefined symbols at the final link; see
  finding 7.
- Raw device access is unavailable; **disk images work normally**. That is the
  same deal the PowerPC build ships under, and it is the right one - a `.img`,
  `.hdf` or `.adf` on a Solaris box is the realistic use case.

Second simplification: SPARC V9 is **big-endian**, like PowerPC. Every
endian-sensitive path in the engine - the whole Amiga stack, HFS, the Sun disk
label in `src/partition/sun.rs`, big-endian UFS - was already made correct for
the PowerPC build and stays correct here. This target is a second independent
check on that work rather than a new frontier.

## Verified findings

Findings 1-6 were probed on 2026-09-03 against `~/sol9-toolchain/sysroot`,
before anything was built; 7 and 8 came out of the first real link on
2026-09-05. These are the things that actually bite.

### 1. `getifaddrs` does not exist on Solaris 9 - RESOLVED in the shim

`src/remote/service.rs:259` `local_ipv4_addrs()` is gated on plain
`#[cfg(unix)]` and calls `libc::getifaddrs` / `libc::freeifaddrs`. Neither symbol
is defined anywhere in `usr/lib/sparcv9/lib*.so.1`; they arrived in Solaris 11.

The trap is that **the libc crate declares them for solarish anyway**
(`libc/src/unix/solarish/mod.rs:2731`), so this compiles cleanly and dies at the
final link with an undefined symbol, after the whole engine has been transpiled
and codegen'd. Budget for it up front.

**Resolved without touching `src/`**: `rb-cli-sol9/shim/sol9-compat.c` implements
both over `SIOCGLIFNUM` / `SIOCGLIFCONF`, the ordinary pre-Solaris-11 idiom, and
`scripts/build-sol9.sh` puts the object on every link line. That keeps the engine
shared with every other build and makes this target pay for its own gap - the
same contract as `rb-cli-ppc/shim/ppc-compat.c`.

The struct layout is the contract with the libc crate and a mismatch would be
silent (the caller would read `ifa_addr` from the wrong offset), so the shim
carries a `sizeof(struct ifaddrs) == 56` compile-time assert rather than trusting
the reasoning. One trap on the way: Solaris' `<net/if.h>` defines `ifa_dstaddr`
as a macro for its own kernel-side `struct ifaddr`, which rewrites the field name
and will not compile until it is `#undef`'d.

### 2. Solaris 9 is missing several symbols std wants

Established by the mrustc port and recorded there: `posix_spawn`, `posix_spawnp`,
`pthread_condattr_setclock`, `sem_timedwait`, `dirfd`, and `pthread_setname_np`
are all absent (all Solaris 10+). mrustc's target spec already supplies what std
needs via `emulated_posix2001`, and the stdlib builds - so this is background,
not new work. It matters only if engine code starts calling one of them
directly.

`clock_gettime`, `nanosleep`, `sched_yield` are in **librt**, `sendfile` in
**libsendfile**, and pthreads are still a separate library on 9. The target spec
already passes `-lrt`.

### 3. libc-crate support for `sparc64` + `solaris` is thin

`libc/src/unix/solarish/` has `x86.rs`, `x86_64.rs` and `x86_common.rs` but **no
`sparc64.rs`**, and the module-selecting `cfg_if!` at the bottom of `mod.rs` has
no `else` arm - so on sparc64 those modules simply contribute nothing. There are
`#[cfg(target_arch = "sparc64")]` arms elsewhere in the file, so the crate is not
oblivious to SPARC, and the *stdlib's* trimmed libc built fine for the target.

**Resolved: it builds.** libc 0.2.155 compiled clean for `sparcv9-sun-solaris2.9`
with no patch, so the missing `sparc64.rs` costs nothing here - nothing in
`solaris.rs` or `mod.rs` reaches an x86-only item unconditionally.

### 4. crossterm's event source on Solaris is unverified

The PowerPC build needs `crossterm = { features = ["use-dev-tty"] }` because
Darwin 9's kqueue cannot watch a tty. Solaris' `poll(2)` on a terminal is native
and correct - it is a STREAMS system - so the default source may well work. But
crossterm 0.28's default path goes through **mio**, and mio's Solaris support for
this target is unverified. Note that `rb-cli-ppc/patches/signal-hook-mio.py`
already exists, so mio *is* in the graph.

**The compile-side risk is resolved**: `mio`, `crossterm` 0.28.1,
`signal-hook-mio` and `ratatui` 0.29 all build for the target unchanged, and
they link - which turned out to mean more than it sounds, because mio's `Waker`
is a self-pipe built on `pipe2`, a symbol Solaris 9 does not have (finding 7).
That is implemented in the shim rather than stubbed, so the event loop has a
working waker.

**RESOLVED on the hardware, 2026-09-07: mio was indeed the problem, and
`use-dev-tty` is indeed the fix.** With the default event source the TUI
renders correctly - alternate screen, cursor hidden, the menu bar painted -
and then aborts with `Failed to initialize input reader`. So Solaris' terminal
handling and ratatui's output were never at fault; only the input side was.

`crossterm = { features = ["use-dev-tty"] }` swaps mio for `poll(2)` through
the `filedescriptor` crate, and the TUI then works completely: three
right-arrows walk the selection Inspect -> New Disk -> Optical -> Archives,
each keypress produces its own repaint, and `q` exits 0. Cost one crate, as
the manifest predicted.

Worth noting for anyone testing a TUI over ssh: drive it with `ssh -tt` and
count *repaint batches* (splitting the capture on the cursor-hide sequence)
rather than trusting the exit code. A TUI that dies on startup and one that
quits on your keypress both exit when stdin closes, and reading that as
success was a wrong call made once here already.

### 5. A latent bug in the engine: a fallback arm nothing had ever compiled

`src/os/mod.rs` imports `anyhow::{Context, Result}` and then, at line 933, calls
bare `bail!`:

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    { bail!("device write access not supported on this platform") }

That arm is dead on every platform anyone has ever built for, so no compiler had
ever looked at it. Solaris is the first target to reach it, and mrustc says
`Unknown macro bail`. Real rustc would say the same. The fix is one token -
`anyhow::bail!`, which is exactly what the *same file* already writes at line
1147 - and it cannot affect any existing build, because no existing build
compiles the line.

The other three fallback arms in that file, plus the ones in
`rbformats/vhd.rs` and `privileged/mod.rs`, were checked at the same time and
are clean. So this class is exhausted rather than merely sampled - worth knowing,
because each instance costs a full engine transpile to discover.

### 6. `filetime` is missing from the mrustc manifests

`fs/fork_export.rs` names `filetime` directly. It reaches the graph as a
transitive dependency of `tar`, but minicargo passes `--extern` only for *direct*
dependencies, so the engine cannot see it unless the manifest declares it.
`rb-cli-vintage` does; **`rb-cli-ppc` does not**, and neither did `rb-cli-sol9`
until this port hit `Couldn't find path component 'filetime'`.

Diffing all three `[dependencies]` tables, `filetime` is the *only* drift from
`rb-cli-vintage` in either mrustc manifest. Fixed in both: `rb-cli-sol9` when
this port hit it, and `rb-cli-ppc` pre-emptively on 2026-09-05, so that build
does not pay for a full transpile to rediscover it.

### 7. Fourteen more missing symbols, and they only appear at the first link

`getifaddrs` was the *known* gap. The first successful transpile turned up
fourteen more, from four crates at once:

| symbol | wanted by | live? |
|---|---|---|
| `utimensat` | `filetime` (and `nix`, `rustix`) | **yes** - `fs/fork_export.rs` calls `filetime::set_file_times`, and filetime routes solaris through `utimensat(AT_FDCWD, ..)` |
| `pipe2` | `mio` (and `nix`, `rustix`) | **yes** - mio's `Waker` is a self-pipe wherever there is no eventfd, and crossterm 0.28's event source is built on mio |
| `getrandom` | `getrandom`, and the engine via zip's `AesWriter` | **yes** - tempfile names and AES salts |
| `dirfd`, `faccessat`, `fchmodat`, `linkat`, `mkdirat`, `mkfifoat`, `mknodat`, `readlinkat`, `symlinkat`, `accept4`, `dup3` | `nix`, `rustix` | no - paths rusty-backup never runs, but they still have to resolve |

Two things are worth internalising from this.

**`nix` and `rustix` are in the graph after all.** This port was scoped on the
belief that `nix` leaves by its `[target.'cfg(...)']` gate. That is right about
the *direct* dependency and wrong about the transitive one: `rustyline` pulls
`nix`, `tempfile` pulls `rustix`. Dropping a crate from `[dependencies]` does
not drop it from the link, and the earlier claim is corrected above.

**Solaris 9's only at-family primitive is `openat`.** That is the one symbol of
the set the sysroot does define - it arrived in 9 for extended attributes
(`O_XATTR`) - and it cannot express a directory-relative `mkdir`, `link`,
`chmod` or `stat`. So there is no honest way to emulate the rest for a real
directory fd short of a `save-cwd`/`fchdir`/restore dance that races every other
thread in the process.

The shim therefore resolves only the two cases needing no directory fd -
`AT_FDCWD`, and an absolute path, which ignores the fd by definition - and
returns `ENOSYS` for anything else. Likewise `AT_SYMLINK_NOFOLLOW` on
`utimensat` and `fchmodat`, which Solaris 9 has no `lutimes`/`lchmod` to serve.
A stub that lies is worse than the link error it replaces; every entry point in
`rb-cli-sol9/shim/sol9-compat.c` is either a faithful emulation or a loud
refusal, never a quiet wrong answer.

Two constants to get right, both taken from the *libc crate* rather than from
the Solaris headers, because the caller is Rust: `AT_FDCWD` is `0xffd19553` (the
sysroot agrees, and types it `unsigned` - comparing it against an `int` fd works
by conversion but warns, so the shim names an `int` form once), and `UTIME_OMIT`
/ `UTIME_NOW` are `-2` / `-1`.

### 8. The whole link can be verified without the hardware

A binary that links can still die on the target: Solaris resolves lazily, so a
missing function shows up when it is first *called*, which for a shim gap can be
a long way into a session. That is checkable here, and it is cheap:

```sh
T=~/sol9-toolchain/opt/bin/sparcv9-sun-solaris2.9
B=~/repos/mrustc/output-rb-sol9/rb-cli
$T-readelf --dyn-syms -W $B | awk '$7=="UND" && $8!=""{print $8}' | sed 's/@.*//' | sort -u > undef
for l in libc libm librt libpthread libsendfile liblgrp libsocket libresolv libnsl libdl; do
  nm -D --defined-only ~/sol9-toolchain/sysroot/usr/lib/sparcv9/$l.so.* 2>/dev/null
done | awk '{print $NF}' | sed 's/@.*//' | sort -u > provided   # plus libgcc_s
comm -23 undef provided
```

Both sides must have the `@@SUNW_1.1` version suffixes stripped, or every symbol
in libc looks missing and the check is worthless.

Result on 2026-09-05: of 251 imports, the only three unresolved are
`_ITM_registerTMCloneTable`, `_ITM_deregisterTMCloneTable` and
`_Jv_RegisterClasses` - all `WEAK UND`, the standard GCC transactional-memory
and GCJ hooks, harmless by construction. All sixteen shim entry points show as
`T` (locally defined) rather than imported.

This does not prove the binary *behaves*; it proves it will start, and it
retires the entire class of "linked here, dies on the hardware with a symbol
error" without the Blade.

### 9. The binary is baseline SPARC V9, so any V9 host will do

Worth knowing before choosing a test machine, and the SPARC counterpart of the
PowerPC "does this need AltiVec?" question. Disassembling the finished binary
gives 162 distinct mnemonics and **not one** VIS or UltraSPARC-specific
instruction (no `fpadd16`, `alignaddr`, `edge*`, `pdist`, `bmask`, no crypto
unit ops), and `e_flags` is `0x0` - no hardware-capability bits demanded:

```sh
T=~/sol9-toolchain/opt/bin/sparcv9-sun-solaris2.9
$T-objdump -d --no-show-raw-insn ~/repos/mrustc/output-rb-sol9/rb-cli \
  | awk '{print $2}' | grep -oE '^[a-z][a-z0-9_]*' | sort -u
```

That follows from the target spec compiling everything `-m64 -mcpu=v9`,
including the cc-rs-built `-sys` crates, but it is cheap to confirm and it
would be an expensive surprise. The practical consequence: the Blade's
UltraSPARC-IIIi, QEMU's emulated IIi and a T1 are all equally valid hosts, so
the choice of test machine is a question about the *OS*, never about the CPU.

### 10. Runtime findings from the hardware (2026-09-07)

The binary was finally run on the Blade. It works - `new` / `put` / `ls` /
`inspect` / `fsck` and `raw` / `vhd` backups all behave - and the parity gate
immediately earned its keep by finding two bugs nothing at link time hints at.

**`File::try_clone()` fails on Solaris 9.** Rust's std implements it as
`fcntl(fd, F_DUPFD_CLOEXEC, 0)`; libc spells that command `47` for solaris,
but it arrived in Solaris **10**. Nine's `fcntl` knows only `F_DUPFD` (0) and
answers `EINVAL`, so every backup died with "failed to clone local source
handle: Invalid argument (os error 22)". The call is inside mrustc-built
libstd, so it cannot be reached by patching this crate's dependencies - the
shim now interposes `fcntl`, handling that one command as `F_DUPFD` plus
`FD_CLOEXEC` and delegating everything else through `dlsym(RTLD_NEXT)`.

**mrustc passes fieldless enums wrongly on 64-bit big-endian.** This is the
important one, and it is an mrustc bug rather than a Solaris one. mrustc lowers
a fieldless `#[repr(u32)]` enum to a one-field C struct and emits `extern "C"`
signatures taking it **by value**:

```c
struct e_..ZSTD_cParameter.. { uint32_t TAG; };
extern uintptr_t ZSTD_CCtx_setParameter(struct ZSTD_CCtx_s *, struct e_..ZSTD_cParameter.., int32_t)
    asm("ZSTD_CCtx_setParameter");
```

The real function takes a plain C enum, i.e. an `int`. On SPARC V9 a 4-byte
struct is passed **left-aligned** in the 8-byte parameter slot while an `int`
goes in the low half, so `ZSTD_c_compressionLevel` (100) arrives as
`0x64_00000000`. zstd answers "Unsupported parameter" and every `--format
zstd` backup fails. Measured, not deduced - a linker `--wrap` probe on
`ZSTD_CCtx_setParameter` printed the argument.

**It cannot appear on 32-bit big-endian**: on powerpc-apple-darwin the
parameter slot is 4 bytes, so the struct and the int occupy the same bits.
Solaris 9 is the first 64-bit big-endian mrustc target, which is why the
PowerPC gate stayed green through it. Any `extern "C"` fn taking a fieldless
enum by value is affected, not only zstd's eight entry points - so the real
fix belongs in mrustc's codegen and this is worth an upstream report.

Worked around with two vendor patches, `zstd-sys-ffi-enum` and
`zstd-safe-ffi-enum`, declaring the affected parameters as the underlying
integer with matching `as u32` casts. Note that the zstd **decoder** was never
affected (our restore path calls neither `DCtx_setParameter` nor `DCtx_reset`),
which is what isolated the bug to the encoder's parameter call.

### 11. Three staleness traps that make a fix look like it failed

All three cost time on 2026-09-07, all three present as "I fixed it and
nothing changed", and all three are the same shape: an artifact that is out of
date but looks current.

- **A rebuilt shim does not relink.** minicargo decides staleness from Rust
  inputs only, so editing `sol9-compat.c` recompiles the object and then ships
  the *previous* executable, silently. `scripts/build-sol9.sh` now removes
  `rb-cli` when the shim is newer.
- **`include!`d files are not in the depfile.** `zstd-sys/src/bindings_zstd.rs`
  is pulled in with `include!`, so patching it leaves minicargo believing
  `libzstd_sys.rlib` is current. Delete the rlibs of any crate whose *included*
  sources you patch.
- **A partially-applied vendor patch wedges the runner.** Its three-state
  check assumes all-or-nothing, so `APPLIED` must name the **last** edit of the
  set. Marking an earlier one strands the rest, and then neither `APPLIED` nor
  `MATCH` matches and the run aborts naming the wrong crate.

### 12. Reaching the Blade

Both facts recorded here were wrong by 2026-09-07 and cost a wrong conclusion
("the hardware is unavailable"), so: **find the machine, do not trust the
address.** It moved to **192.168.99.175**, identified by SSH banner
(`SSH-2.0-Sun_SSH_1.1.1`) among the hosts answering on port 22 - that sweep is
the reliable way to find it again. The login is **`user`**.

SunSSH 1.1.1 offers only SHA-1 era algorithms, so a modern client needs
`KexAlgorithms`, `HostkeyAlgorithms`, `PubkeyAcceptedAlgorithms`, `Ciphers` and
`MACs` re-enables; there is now a `Host 192.168.99.175 sunblade` block in
`~/.ssh/config` carrying them, which is what lets the gate scripts call plain
`ssh`. The key must be **unlocked in the desktop session** - until it is, the
handshake reaches `Server accepts key` and then fails
`sign_and_send_pubkey: signing failed ... from agent`, which reads like a
missing key and is not.

### 13. The parity gate compared merged streams

`ppc-smoke.sh` captured `2>&1` on both sides. Over ssh the remote merge
interleaves stdout and stderr on one channel with different buffering than a
local pipe, so byte-identical output reported as a diff - a lost newline
between the stderr banner and the stdout body - and produced six false
failures on the first Solaris run. Verified identical when the streams are
captured separately; the gate now labels and compares them apart.

## What the work is

Phased, in dependency order. Each phase is verifiable on its own. Phases 1-7
are **done** as of 2026-09-07; 8 (doc sync) is the remainder.

1. **Manifest and vendor.** DONE. `rb-cli-sol9/Cargo.toml`, then `cargo vendor`
   into `rb-cli-sol9/vendor/` (gitignored by the `rb-cli-*/vendor/` rule). The
   lockfile holds `libc` at `=0.2.155`; that pin only works because `nix` is not
   declared - see finding 7 for why it arrives anyway.
2. **Vendor patches.** DONE. `scripts/apply-vendor-patches.py --vendor-dir
   rb-cli-sol9/vendor` applied the existing eight unchanged, 13 target files:
   every one of them is an *mrustc* gap, not a PowerPC one. The three-state
   runner fails loudly if a crate bump has broken a pattern, so re-running it
   before every transpile is cheap insurance rather than an act of faith.
3. **Build the engine's C for the host first.** NOT APPLICABLE here, and
   `stage_hostc` refuses with the reason. The PowerPC build has this stage
   because it catches mrustc transpile failures without needing a target libc -
   but it builds for the *host*, which on this machine is Linux, so it compiles
   `os/linux.rs` and needs `nix`, which this manifest drops. There is no
   cheap-proxy stage on this port and none is needed: the target stdlib already
   exists, so `sol9` builds the real thing directly.
4. **`scripts/build-sol9.sh`.** DONE, and far smaller than the PowerPC driver -
   no remote compiler, no split-TU, no orphan reaping. Stages: `check vendor
   sol9libs hostc sol9 dist smoke`. The engine stayed one translation unit as
   predicted - 28.7 MB of `.text` in a 56 MB object, linked in one piece. GNU ld
   2.35 has no ld64-85 branch-island limit, so `ppc-split-tu.py` has no
   counterpart here and the guess that a 64-bit SPARC object would be too big
   for one unit was wrong in the direction that costs nothing.
5. **First link.** DONE, in two rounds rather than one. `getifaddrs` was already
   shimmed and did not reappear; fourteen *other* symbols did, from `nix`,
   `rustix`, `mio`, `filetime` and `getrandom` (finding 7). Note what this cost:
   both rounds needed the whole engine transpiled first, which is exactly why
   finding 7 lists the symbols rather than just saying "extend the shim".

6. **Parity gates on the Blade.** BLOCKED on the hardware; the scripts are
   ready. The PowerPC port proved one gate is not enough: `scripts/ppc-smoke.sh`
   covers reads (`inspect` / `ls` / `fsck` + a byte-identical backup) and
   `scripts/ppc-newcode-smoke.sh` covers the write verbs, and the read gate
   stayed green through a bug that mis-sized **every** PowerPC backup.

   Rather than fork Solaris copies, both were made target-agnostic on
   2026-09-05: they take `SOL9_HOST` (or `PPC_HOST`, or `RB_SMOKE_HOST`), the
   remote binary via `RB_SMOKE_BIN` or `$1`, and the ssh agent socket via
   `RB_SMOKE_SSH_AUTH_SOCK`. Setting only `PPC_HOST` behaves exactly as before,
   so the PowerPC gate is unchanged. Everything else in them was already
   portable - the remote side is plain POSIX sh. To run once the Blade is up:

   ```sh
   cargo build --release --bin rb-cli
   scp -r dist/rb-cli-sol9.tar.gz user@192.168.99.176:~/     # unpack there
   SOL9_HOST=user@192.168.99.176 \
     RB_SMOKE_SSH_AUTH_SOCK=/run/user/$(id -u)/gcr/ssh \
     scripts/ppc-smoke.sh ./rb-cli-sol9/rb-cli
   ```

   Both scripts exit with the mismatch count, so either can gate a build.

   **QEMU: investigated 2026-09-06, and the answer is `-M niagara` or nothing.**
   Three machines matter and only one is a candidate.

   - **`-M SS-5` (sun4m, 32-bit)** - the machine the existing Solaris 9 fixture
     boots on. Useless here whatever else is true: a 32-bit sun4m kernel cannot
     exec a SPARC V9 binary. The fixture is also on the NAS rather than local.
   - **`-M sun4u` (UltraSPARC-IIi, 64-bit)** - the machine *works*: stock QEMU
     8.2 brings up OpenBIOS v1.1 on an emulated IIi and reaches the `ok` prompt.
     What does not work is Solaris on it, and **a newer QEMU does not change
     that** - which was checked properly rather than assumed. QEMU 11.1.50 was
     built here from `~/nextstep-test/qemu-src` (`--target-list=sparc64-softmmu`,
     into `build-sparc64/` so the NeXTSTEP m68k build dir is untouched), and its
     sun4u boot output is identical to 8.2's apart from the OpenBIOS build
     timestamp - same OpenBIOS v1.1, same `SUNW,UltraSPARC-IIi`, same four
     machines. 11.1's own `docs/system/target-sparc64.rst` still claims only
     "Linux, NetBSD and OpenBSD", word for word with 8.2's, and upstream issue
     [#293](https://gitlab.com/qemu-project/qemu/-/issues/293) (`BOP_ALLOC
     failed` booting Solaris on sun4u) was still open at 2026-03-19. The gap is
     OpenBIOS's OBP, not the QEMU version, so upgrading buys nothing here.

     One trap if anyone rebuilds it: QEMU's `configure` makes its own pyvenv and
     needs `ensurepip`, which this box lacks (`python3-venv`, and no sudo). It
     works anyway *provided* the plain system `python3` is used, because
     `mkvenv.py` skips ensurepip when it can fall back to system site-packages
     and pip is importable there. Putting a venv on `PATH` first is what breaks
     it, with a misleading "python venv creation failed".
   - **`-M niagara` (UltraSPARC T1, sun4v)** - the one documented path. QEMU
     says it "is able to boot the disk.s10hw2 Solaris image" from the OpenSPARC
     T1 bundle, and the machine is already present and functional in the stock
     8.2 here (it fails cleanly with "Unable to load a firmware" when the
     `S10image/` directory is absent). The bundle,
     `OpenSPARCT1_Arch.1.5.tar.bz2`, is still live at download.oracle.com -
     199 MB, HTTP 200, no login. Not yet downloaded.

   **Can a real Sun OBP replace OpenBIOS? Investigated 2026-09-06: available,
   but historically fails.** Worth writing down because it is the obvious next
   thought once OpenBIOS is identified as the blocker, and because one part of
   it is a trap.

   Mechanically QEMU allows it: `sun4u`'s `prom_init` tries `load_elf` and then
   falls back to `load_image_targphys`, exactly like the sparc32 machine that
   already boots a real `ss5.bin` here, so `-bios <raw-prom>` is supported (4 MiB
   cap, loaded at `0x1fff0000000`).

   What does not work is the firmware itself. Real OBP has been tried on QEMU's
   sun4u - Ultra 5 (`u5_v3.19.4.bin`) and Ultra 1 - and fails *before the serial
   console initialises*, with unassigned memory accesses and RAM undetected,
   because the emulated machine is a "generic PC-like" sun4u rather than a
   faithful Ultra 5.

   **Tried here on 2026-09-06, and it got much further than 2010 did.** Two
   separate problems had been conflated: getting a correct image out of the
   patch, and running it. The first is solved; the second is now itemised
   rather than mysterious.

   *Extracting the image.* `106121-18.zip` (5.2 MB, from oldsilicon.com's
   archive) contains no firmware file. The payload is
   `flash-update-Ultra510-latest`, a 2.4 MB **SPARC32PLUS Solaris executable**
   that flashes the PROM from inside a running system, with the firmware
   embedded in its `.data`. Do not go looking for it by grepping version
   strings - those are the flasher's own banners, and the regions around them
   are its string-pointer tables. The images are held in a **chain of `OBMD`
   descriptors**, which is the thing worth writing down:

   | offset | field |
   |---|---|
   | +0 | magic `OBMD` |
   | +4 | big-endian data length |
   | +16 | 4-char module name (`OBP`, `POST`, `keyb`, `font`, `pci1`, `clas`, `obdi`) |
   | +20 | build year, then month, day, hour, minute as bytes |
   | +28 | version major, minor, micro |

   Data follows the 32-byte header, and the next descriptor begins exactly at
   `data + length`, so the chain walks cleanly from the first `OBMD`. In this
   patch it yields **OBP 3.31.0 (243,268 bytes)**, **POST 3.1.0 (291,856)**,
   `obdi` (obdiag) and the FCode drivers - each set stored twice.

   *Running it.* The OBP module is a complete flash image: it opens with the
   UltraSPARC reset trap table, and QEMU's `env->pc = prom_addr + 0x20`
   (`hw/sparc64/sparc64.c:126`) correctly lands on the POR vector, TT=1 being
   the second 32-byte entry. Booted as `-bios`, real OBP executes: it disables
   the LSU via `ASI_LSU_CONTROL_REG`, checks `%g7` for the POST handoff magic
   `0x504f5354afb0acab` (`"POST"` plus a cookie), takes the no-POST path since
   we supplied none, and tries to print.

   Then it hangs - and *where* is the whole answer. It writes its early console
   output to a hardcoded physical address, `0x1fff1400000`, polling bit `0x40`
   at `+0x20` for transmitter-ready, with `%asi` set to `0x15`
   (`ASI_PHYS_BYPASS_EC_WITH_EBIT`), long before any PCI enumeration. That is
   the Ultra 5 **ebus MMIO window** at PCI-MMIO `0xf1000000`. QEMU maps nothing
   there: its sun4u puts ebus devices in ISA **I/O port** space at small
   offsets instead (`0x60` i8042, `0x3f8` serial, `0x2000` NVRAM - visible in
   `info mtree`). Different model, so the poll never completes.

   A 70-line throwaway stub covering that window (kept at
   `scratchpad/obp/qemu-sun4u-obp-probe.patch`; the tree was restored
   afterwards) answers the poll and echoes the data register. With it, **real
   Sun OBP 3.31.0 prints to the console under QEMU** -

   ```
   Watchdog Reset
   ```

   - which is further than the 2010 attempts, where it never reached serial
   output at all. ("Watchdog Reset" is correct behaviour, not a fault: no POST
   ran, so `%g7` lacks the handoff magic and OBP reports the reset that way.)
   Execution goes from 10 translated blocks to 86, and the remaining
   requirements are now a short list rather than a mystery: a 16550 at window
   `+0x400000`, twelve bytes written at `+0x710000`, and reads at `+0x30002e`,
   `+0x30015c`, `+0x300398` - `0x2e` being the classic PC87317 SuperIO config
   index port.

   *Pushed further, 2026-09-06.* An iterate-and-observe loop (stub a device,
   rebuild, watch which address OBP touches next) mapped the rest. Two results
   are worth keeping even though OBP still does not boot.

   **The reset vector table is not what the V9 book implies.** SPARC V9 says
   the RED_state entry is `RSTVaddr + TT*0x20` with TT=1 for POR, so QEMU uses
   `prom_addr + 0x20` (`hw/sparc64/sparc64.c:126`). Sweeping all six entries
   against real OBP says otherwise:

   | entry | what OBP prints |
   |---|---|
   | +0x00 | *(silent - runs, probes, hangs)* |
   | +0x20 | `Watchdog Reset` |
   | +0x40, +0x60 | `SIR Reset` |
   | +0x80 | `RED State Exception` + TL/TT dump |
   | **+0xa0** | **`Power On Selftest Completed`** + status words |

   So `+0x20` - the entry QEMU uses - is where this OBP puts its *watchdog*
   handler, and the POST-completed handoff is at `+0xa0`. That does not by
   itself prove QEMU wrong (OpenBIOS is built to match it, so the pair is
   self-consistent), but it does mean real firmware and QEMU disagree about the
   table, and anyone doing this properly needs to settle it against the
   UltraSPARC IIi manual first.

   **The Ultra 5 ebus map, confirmed.** OBP drives these at PCI-MMIO
   `0xf1000000` (physical `0x1fff1000000`), before any PCI enumeration:

   | ebus offset | device | QEMU today |
   |---|---|---|
   | `0x000000` | `eeprom@14,0` - M48T59 NVRAM/TOD | exists, but at ISA port `0x2000` |
   | `0x3062f8` | `su@14,3062f8` - 16550 (ttyb) | exists, at ISA port `0x2f8` |
   | `0x3083f8` | `su@14,3083f8` - 16550 (ttya) | exists, at ISA port `0x3f8` |
   | `0x300000`+ | ISA I/O window; OBP probes `0x2e`, `0x15c`, `0x398` for a **PC87317 SuperIO** | no model |
   | `0x400000` | `se@14,400000` - **SAB 82532 ESCC2** | no model |
   | `0x710000`-`0x71000b` | twelve bytes written `0xff`; unidentified | no model |

   Aliasing NVRAM to `+0` and the two 16550s to their true offsets is a few
   lines and demonstrably moves OBP along.

   The SAB 82532 console is **fully understood and the stub is correct**: reads
   at `+0x20` are STAR (`XFW`=0x40 ready, `CEC`=0x04 busy), writes at `+0x00`
   are XFIFO, and each character is committed by writing `0x08` (the XF
   transmit-frame command) to CMDR, which shares offset `+0x20` on write. A
   ~20-line stub carries the whole console path. (An earlier note here claimed
   the stub "diverged once OBP initialises the port" - that was wrong, and
   worth recording as a lesson: the characters were flowing correctly and a
   `| cat -v | head` pipeline was discarding them when `timeout` killed QEMU.
   Redirect to a file and read the file.)

   **Where it actually stops is PCI, not the console.** Both live paths end
   spinning on a `retl` at `+0x238` that returns to itself, and the registers
   say why: `%g1` = `0xf1000000` (the ebus window) and `%g2` =
   `0x1fe01010814`, a Sabre PCI *config* address - bus 1, device 1, function 0,
   register `0x14`, i.e. a BAR. OBP is enumerating PCI and programming the ebus
   BAR itself, which is the correct thing to do; it loses its return address
   because QEMU's sun4u has no device at that bus/device, the config read comes
   back all-ones, and OBP mishandles it. So the hardcoded window this whole
   section is about is only OBP's *bootstrap* console; the real dependency is a
   PCI topology shaped like an Ultra 5's.

   POST is not a way around it. `mod-POST.bin` has its own six-entry reset trap
   table and boots as `-bios`, but it is the most hardware-dependent code in
   the machine - it drives the memory controller and caches directly - and
   QEMU aborts it at `tl: 5` (MAXTL exceeded) within moments on every vector.

   So the wall is precisely located and the shape of the fix is known:
   **QEMU's sun4u needs to be a faithful Ultra 5** - not just the ebus MMIO
   offsets and a SAB 82532 model, but a PCI topology OBP recognises. That last
   part is the big one, and it is why "add the missing devices" understates the
   job. The experiment that established all of this is kept at
   `scratchpad/obp/qemu-ultra5-ebus-experiment.patch` (144 lines against
   `hw/sparc64/{sun4u,sparc64}.c`; the tree was restored afterwards) and is the
   seed of an upstream PR, not the PR itself - the address-map half is close to
   submittable, the two device models are a project. Nothing about rusty-backup
   is blocked on any of it.

   Two further traps if anyone retries it. **The Blade 2500 is the wrong
   source**: its OBP (4.17.1, patch `119232-02`) is UltraSPARC IIIi / Tomatillo,
   and QEMU models UltraSPARC IIi / Sabre, so the machine to match is an Ultra
   5/10 (patch `106121-18`, OBP 3.31.0) - which needs no hardware at all, being
   a public Sun patch. And **there is nothing to dump**: from UltraSPARC onward
   Sun shipped firmware as a `patchadd` package rather than a raw PROM image, so
   obtaining one means unpacking a patch, not reading a chip.

   Two caveats before anyone treats Niagara as equivalent to the Blade. It runs
   **Solaris 10**, so it tests the "does Solaris 10 come along free?" assumption
   below rather than confirming it independently. And the shim's `dirfd` reads
   Solaris 9's `DIR` layout, which Solaris 10 does not share - it is dead code
   (`rustix` only), so it should not bite, but a green Niagara run would not
   have validated it. Everything else in the shim - `utimensat` over `utimes`,
   `pipe2` over `pipe`+`fcntl`, `getrandom` over `/dev/urandom` - is exercised
   as written, because the executable's own definitions win over libc's.

7. **TUI verification.** DONE - see finding 4. The default mio event source
   fails at `Failed to initialize input reader`; `use-dev-tty` fixes it and the
   TUI navigates and exits cleanly on the Blade.
8. **Docs sync.** README's platform/build sections, and this file promoted from
   scope to build guide.

## Putting it in the pipeline

The build is already containerised - `docker/sol9.Dockerfile` clones mrustc,
builds it, fetches the rustc source, builds the host and target stdlibs and
runs `scripts/build-sol9.sh`. Everything in that chain is fetched from source
at build time **except one file**, and there is one blocker.

### What it needs

| Component | Where | Reproducible? |
|---|---|---|
| `rb-cli-sol9/` manifest + `shim/sol9-compat.c` | this repo | yes, committed |
| `scripts/build-sol9.sh`, vendor patches | this repo | yes, committed |
| `docker/sol9.Dockerfile` | this repo | yes, committed |
| mrustc fork, branch `sparc-solaris-10` | `danifunker/mrustc` | **blocked - see below** |
| Base image `mrustc-sol9-cross` | mrustc's `docker/sol9-cross/` | all but the sysroot |
| binutils 2.35.2, gcc 4.9.4 | ftp.gnu.org, at build time | yes |
| rustc 1.74.0 source | fetched by `make RUSTCSRC` | yes |
| Crate sources | `cargo vendor`, at build time | yes (needs crates.io) |
| **Solaris 9 sysroot** (`sysroot.tar.gz`, 109 MB) | `docker/sol9-cross/`, **gitignored** | **no - not redistributable** |

Two things on this machine are *not* needed and should not be mistaken for
dependencies: `~/sol9-deps/prefix` (nothing references it - zstd and zlib are
compiled from source by cc-rs for the target), and the Blade itself, which is
needed only for the parity gates, never for the build.

### The blocker

**The five mrustc commits are local-only.** `origin/sparc-solaris-10` is at
`109ddad1`, the base commit, so `docker/sol9.Dockerfile`'s
`git clone --branch sparc-solaris-10` fetches a branch with no Solaris target
and the image build fails at `make -f minicargo.mk LIBS MRUSTC_TARGET=...`.
Pushing that branch is the single prerequisite for the container building
anywhere but here.

### The sysroot

Sun does not permit redistributing Solaris 9, so `sysroot.tar.gz` cannot go in
a public image or the repo (it is gitignored for that reason). For CI it has to
arrive out of band - a private registry holding the pre-built
`mrustc-sol9-cross`, or the tarball as a secret artifact restored before
`docker build`. Building the base image is a one-off; only the layer above it
needs to re-run per commit.

### Cost

The engine transpile dominates: a from-scratch container build is tens of
minutes and wants ~25 GB of disk (mrustc's tree plus the rustc source plus the
generated C). Cache the base image and the `output-1.74.0-<target>` stdlib and
a normal commit rebuilds only the engine and the link. Cap parallelism at 4 -
see `docs/build-memory-crashes.md`, which applies to this build too.

### Gating a release on the hardware

`scripts/ppc-smoke.sh` and `scripts/ppc-newcode-smoke.sh` both take
`SOL9_HOST`, so a self-hosted runner that can reach a Solaris 9 SPARC box can
gate on them; both exit with the mismatch count. They need the ssh setup in
finding 12. Without such a runner the container can still build and package -
it just cannot prove parity, which is exactly the gap that let the PowerPC
size bug ship once.

## mrustc: what needs upstreaming

Five commits exist on `sparc-solaris-10`; a sixth is not yet written. Grouped
as they should be proposed, smallest and most general first - three of the four
are not Solaris-specific at all.

| PR | Commits | Scope |
|---|---|---|
| **1. Signed overflow helpers** | `64551250` | Pure correctness, no new target. The emitted `__builtin_mul_overflow_i*` reported overflow for almost any negative operand; fuzzing found 561,004 mismatches against GCC's own builtins, plus reachable UB (`INT_MIN/-1`, signed wrapping). Stands alone and is worth landing regardless of Solaris. |
| **2. `emulate-overflow-intrinsics`** | `73c570f3` | Lifts MSVC's existing type-suffixed stand-ins into a target flag so the GNU backend can use them, for compilers older than GCC 5. MSVC output unchanged. Depends on PR 1. |
| **3. `CC_${TRIPLE}` sanitisation** | `71910c7c` | One-line class of bug: the variable name replaced only `-`, so a triple with a `.` produced a name no shell can export. Unnoticed because no triple had a dot before. Fully general. |
| **4. The Solaris 9 target** | `6421bfef`, `b3aa8ae6` | `sparcv9-sun-solaris2.9` plus `emulate-c99-math` / `emulate-posix2001`, and the `docker/sol9-cross` toolchain container (including Solaris 9's empty `INTPTR_MAX`/`UINTPTR_MAX`, which breaks any C99 `#if` test). The one genuinely target-specific PR. |
| **5. Fieldless enums across FFI** | **not yet written** | Finding 10. mrustc lowers a fieldless `#[repr(u32)]` enum to a one-field struct and passes it **by value** in `extern "C"` signatures; the callee expects a scalar. On any 64-bit big-endian target the value lands in the wrong half of the register. Should emit the underlying integer type in extern signatures and at call sites. |

PR 5 is the most valuable and the only one still to write. It is a real
codegen bug rather than a missing feature, it affects every `extern "C"` fn
taking such an enum, and it is structurally invisible on 32-bit big-endian -
so mrustc's existing PowerPC users cannot have hit it. A regression test wants
a 64-bit big-endian target, which PR 4 supplies.


## Open questions - decisions to make, not tasks

- **Is `rb-cli serve` in scope?** It decides finding 1. Networked backup is the
  strongest argument for this target existing at all, which argues for the real
  `SIOCGLIFCONF` implementation over the cheap gate.
- **32-bit as well?** The target is `sparcv9` (64-bit) only. A Blade 2500 runs
  64-bit happily; older SPARC hardware (Ultra 1/2, SPARCstation) does not. Adding
  `sparc-sun-solaris2.9` would be a new mrustc target spec, a second stdlib build
  and a second toolchain. Recommendation: **no** - out of scope until someone has
  the hardware.
- **Does Solaris 10 come along free?** The `sparcv9-sun-solaris` target (no `2.9`)
  already exists in mrustc and is the less constrained one. A Solaris 9 binary
  should run on 10 unchanged, so this is probably a packaging note rather than a
  build.
- **Packaging shape - ANSWERED: the bundle must ship `libgcc_s.so.1`.**
  `-static-libgcc` does *not* remove it. Rust's `unwind` crate names the library
  in a `#[link]` attribute, which puts it on the link line after the flag has had
  its say, and `readelf -d` on the finished binary lists `libgcc_s.so.1` among
  the `NEEDED` entries - Solaris 9 has none of its own, so a stock machine could
  not start it. `scripts/build-sol9.sh` already handles this: `RPATH` is
  `$ORIGIN` and `stage_dist` copies the library in beside the binary, giving a
  16 MB relocatable tarball that needs no install step. Confirmed on the real
  artifact, not reasoned about.

## Inherited traps

From `build-ppc-mrustc.md` and the PowerPC port. All are mrustc- or
process-level, so they apply here unchanged:

- **`MRUSTC_TARGET_VER=1.74` must be exported** or mrustc parses in 1.29 mode and
  reports itself as `rustc 1.29.100`, version-gating build scripts wrongly.
- **`OVERRIDE_SUFFIX` is picked from the host OS**, so it must be passed
  explicitly: `OVERRIDE_SUFFIX=-solaris`. Likewise `STD_ENV_ARCH=sparc64`.
- **A killed build leaves a 0-byte `build_<crate>.txt`**, which minicargo then
  trusts as an up-to-date empty result; the crate compiles with no cfgs and fails
  many crates later with no hint.
- **Never `tail` the build log.** Redirect to a file and grep it.
- **Never edit the driver script while it is running it.** bash reads a script
  lazily by byte offset; inserting lines mid-file makes it resume at a stale
  offset and execute garbage.
- **Editing `src/` mid-build silently produces a binary from the old source.**
  mrustc reads the engine once, at the start of the long transpile.
- **mrustc mis-lowers `leading_zeros`/`leading_ones` on `u8`/`u16`** - a wrong
  answer, not a compile error. Already fixed in engine code by widening; it is
  a codegen bug, so any suspected arch bug reproduces on the host in a minute.
  Do that before spending a build cycle.
- **`MINICARGO_NO_DEBUG_ASSERTIONS=1` for the stdlib** was required on PowerPC
  because the power-alignment rule makes libcore's `assert_unsafe_precondition!`
  abort before `main`. SPARC uses natural alignment, so this is probably not
  needed - but the stdlib here was built by someone else's script, so check what
  it was built with rather than assuming.
- **ssh to the Blade needs `SSH_AUTH_SOCK=/run/user/1000/gcr/ssh`** and the login
  is `user`, not `dani`. The inherited gnome-keyring agent holds the key but
  refuses to make the SHA-1 RSA signature SunSSH wants, and fails as
  `Permission denied (publickey)` - which reads like a missing key and is not.
