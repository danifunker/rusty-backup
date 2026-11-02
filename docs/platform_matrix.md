# Platform matrix

The single list of every build target: what each ships, which toolchain builds
it, and what it can't do. When you add a feature, walk this table (CLAUDE.md
§ "Feature fan-out"). Plan background: [native_frontends_plan.md](native_frontends_plan.md) §3 and §7.

`scripts/check-platform-matrix.py` (run in CI) fails if the **CI target**
column and the `ALL=` list in the `plan` job of `.github/workflows/release.yml`
disagree. A row with CI target `-` is built outside CI.

**Worlds:**
- **modern:** current stable Rust and current crates.
- **vintage:** Rust 1.73 (`rb-cli-vintage/`) or mrustc (`rb-cli-ppc/`,
  `rb-cli-sol9/`), with frozen dependencies. Engine, model and view-model code
  must build here: see CLAUDE.md § "Engine code must compile on Rust 1.73".

**Features:** `default` is `gui, chd, optical, audio, native-zlib, native-zstd,
seekable-zstd, remote, tui, crypto, yaml` (root `Cargo.toml`).

## Targets

<!-- matrix:begin -->
| Target | CI target | World | Ships today | Planned shell | Toolchain | Features | Limits |
|---|---|---|---|---|---|---|---|
| Windows 10/11 x64, i686 | windows | modern | egui GUI + rb-cli, Inno installer | Win32 (Phase 5) | stable | default | |
| Windows 11 ARM64 | windows-arm64 | modern | egui GUI + rb-cli | Win32 (Phase 5) | stable | default | |
| macOS 10.13+ arm64, x64 | macos | modern | egui .app + rb-cli, DMG | AppKit (Phase 3, first) | stable | default | |
| Linux x64, arm64 (AppImage) | appimage | modern | egui AppImage + rb-cli | GTK4 (Phase 4); GTK3 for old distros | stable | default | |
| Linux x64, arm64 (deb, rpm, Arch) | linux-packages | modern | egui GUI + rb-cli packages | GTK4 (Phase 4) | stable | default | |
| Linux riscv64 GUI | riscv64 | modern | egui GUI + rb-cli, deb + rpm | GTK4 (Phase 4) | stable | default | |
| Linux static musl x64, arm64, i586, i686, ppc64, ppc32 | musl | modern | rb-cli + TUI | TUI | stable; i586 pinned 1.98.1; ppc32 nightly + build-std; ppc via cargo-zigbuild | pure-zstd, remote, tui | no GUI, no CHD, no optical, no crypto |
| Linux glibc ppc64, ppc, riscv64 (extra-arch) | extra-arch | modern | rb-cli + TUI | TUI | stable | pure-zstd, remote, tui | no GUI, no CHD, no optical, no crypto |
| MiSTer armv7 | mister | modern | rb-cli-mini + TUI, MiSTer Downloader db | TUI | stable | chd, pure-zstd, remote, optical, tui | no GUI; framebuffer console, no GL |
| Buildroot appliance i586 / i686 | appliance | modern | static i586-musl rb-cli + crossterm menu, hybrid ISO | TUI or menu (open decision 4) | stable (Docker cross, `docker/cross-i586-musl.Dockerfile`) | pure-zstd, remote | no GUI, no TUI, no CHD, no optical, no crypto |
| macOS 10.7+ x64 | macos-vintage | vintage | rb-cli + TUI | AppKit on 1.73 bindings or ObjC over a C ABI (Phase 6) | 1.73 | native-zstd, remote, tui, rust173-polyfill, yaml | no CHD, no optical, no crypto (edition-2024 deps) |
| Windows 7/8 x64, i686 | windows-vintage | vintage | rb-cli + TUI | Win32 on `windows` 0.58 (Phase 6) | 1.73 | native-zstd, remote, tui, rust173-polyfill, windows-legacy, yaml | no CHD, no optical, no crypto |
| Solaris 9 SPARC | sol9 | vintage | rb-cli + TUI | Motif/CDE over a C ABI (deferred, §8) | mrustc (sparcv9-sun-solaris2.9) | native-zstd, remote, tui, rust173-polyfill | no GUI; mrustc gaps: no proc-macros, no inline asm |
| Mac OS X 10.4/10.5 PowerPC | - | vintage | rb-cli + TUI (G3 build) | Carbon/Cocoa over a C ABI (deferred, §8) | mrustc, C compiled on a G5 (`scripts/build-ppc.sh`) | native-zstd, remote, tui, rust173-polyfill, os-stub | no physical devices (os-stub); not in CI |
| DOS (crusty-backup) | cbdos | n/a | separate C program, FreeDOS floppy + CD | conio (out of scope) | DJGPP | - | not the Rust engine |
<!-- matrix:end -->

Not a platform: `crates-msrv` (every crate's tests on Rust 1.73, and qnx2fs
for wasm32) is a CI target too; the check script knows it.

## Shells

| Shell | Code | Targets |
|---|---|---|
| egui | `src/gui/` | the modern GUI rows, until Phase 7 retires it |
| TUI | `src/cli/verbs/tui*` (ratatui) | every rb-cli row |
| AppKit, GTK4, Win32 | `frontends/{appkit,gtk4,win32}/` (not yet) | Phases 3-5 |
| Vintage shells | `frontends-vintage/` (not yet) | Phase 6 |

Every shell renders `src/controller` view-models only (`scripts/check-boundaries.sh`).

## Screens on the shared controllers

| Screen | Controller | egui | TUI | Gaps |
|---|---|---|---|---|
| Commander | `controller::commander` (`CommanderController`) | yes | yes (Phase 2 slice 8): staged edits + Apply (F10) with preflight, review and report; unlock; image, host, backup (native / Clonezilla + partclone caches), archive and optical sources; wrapper expansion; rename (F2); new folder (Ctrl+N); export; host-copy conflict review; rename notice; remote (Ctrl+K); checksums; File Info with mode / owner / xattr prompts | TUI: no clipboard (the text is shown to select instead); the type/creator, ProDOS and date editors and the content preview are egui-only |
