# Front-end flows (tier 8)

A **flow** is an end-to-end case: it drives a real front-end the way a user
would, then checks what the front-end produced with ordinary `rb-cli` steps.
Flows prove that a shell is *wired* to the shared controllers. The logic itself
is tested further down: the controller scenarios (`tests/ui_scenarios/`) test it
without any shell, and tiers 1-7 test the engine.

| Layer | Where | Proves |
|---|---|---|
| Unit / crate tests | `cargo test --workspace` | format code |
| Controller scenarios | `tests/ui_scenarios/*.toml` | the view-model logic, no shell |
| Tiers 0-7 | `regression-tests/cases/tier0..7` | the engine through `rb-cli` |
| **Tier 8 flows** | `regression-tests/cases/tier8` | a front-end, end to end |

Every row in [docs/capabilities.md](../docs/capabilities.md) should name at
least one flow that proves it. A capability with no flow is untested.

## Running them

```bash
rb-regress run --tiers 8                      # every flow on every shell with a driver
rb-regress run --tiers 8 --level smoke        # the high-level set, minutes
rb-regress run --area inspect --fs hfs        # after working on HFS in Inspect
rb-regress run --area files --fs ufs          # one filesystem through Commander
rb-regress run --shell gtk                    # one front-end
rb-regress list --tiers 8 --level smoke       # what would run
```

`--level smoke` keeps only cases tagged `level = "smoke"`; the default, `full`,
runs everything. `--area`, `--fs` and `--shell` take comma lists.

## Writing one

```toml
[meta]
tier = 8
group = "flow.backup"

[[case]]
id = "flow.backup.image-zstd"
description = "Back up a {fs} image to zstd from the Backup screen"
level = "smoke"
smoke_fs = ["fat"]              # the other fs values run under full only
area = ["backup"]
fs = ["fat", "hfs"]             # one instance per value: id.fat.tui, ...
shells = ["tui", "gtk"]         # one instance per shell
capabilities = ["backup.run"]

[[case.step]]                   # setup through rb-cli
args = ["new", "floppy", "{fs}", "--size", "1440K", "{scratch}/src.img"]
expect_exit = 0

[[case.step]]                   # the front-end
ui = [
  { tab = "Backup" },
  { pick = "{scratch}/src.img" },
  { activate = "backup.open_image" },
  { focus = "backup.destination" },
  { fill = "{scratch}/bk" },
  { focus = "backup.format" },
  { choose = "Zstd" },
  { activate = "backup.start" },
  { wait = "backup.done", timeout_ms = 120000 },
]

[[case.step]]                   # verify the output, never the screen alone
args = ["restore", "{scratch}/bk/job", "{scratch}/out.img"]
expect_exit = 0
```

Placeholders: `{fs}` (the matrix value), plus the usual `{scratch}`,
`{fixture}`, `{fixture_copy}`, `{cases}`.

### Controls

A flow names a control by its **controller id** (the `ids` module of the screen's
controller, e.g. `backup.format`), never by what one shell happens to call it.
Each controller says what its controls are called in a `labels` module, and
`rb-cli ui-labels` prints every controller's table; a driver that finds controls
by their text uses it. Where a shell still differs, the driver looks the id up
in [`drivers/labels.toml`](drivers/labels.toml) first:

```toml
[tui]
"backup.open_image" = { key = "o" }          # the TUI opens it with a key
[gtk]
"backup.open_image" = { label = "Open File...", via = "Select a device or image..." }
"backup.destination" = { label = "Folder", pick = "Browse..." }
"backup.format" = "Output Format"
```

An entry is a label, or a table with:

| Key | Meaning |
|---|---|
| `label` | the visible label |
| `key` | activate by these keys, space-separated (`"Tab F10"`): the TUI has keys where GTK has buttons |
| `pick` | the button whose file dialog fills this field (GTK path rows) |
| `via` | open this first: the dropdown a menu entry lives in |
| `nth` | the Nth same-named control, from 0 (on the `via` dropdown when there is one) |
| `skip` | `true`: this shell has no such step (GTK copies to a host folder at once; the TUI stages it) |

`wait`, `wait_gone` and `expect` names resolve the same way; `<task>.done`
(`backup.done`) is the controller's outcome message. The order is the shell's
section, then `[all]`, then the controllers' labels; a name none of them lists
is used as a literal label.

**Every label entry in `labels.toml` is drift** from the controllers' words
(D8): a label (`Folder` against `Destination`), a message (`Backup finished.`
against `Backup complete.`), or a behaviour (`skip`). The TUI keeps only its
keys; `[gtk]` empties once GTK reads the controllers' labels. AppKit and Win32
use the id itself as the accessibility identifier and need no section at all.

### UI ops

Each op sets exactly one action.

| Op | Meaning | TUI driver | GTK driver |
|---|---|---|---|
| `tab = "Backup"` | switch screen | counts Left / Right from the tab bar | clicks the page tab |
| `focus = "id"` | move to a control | Down, then Up, until the focused row (`> `) holds the label | finds the showing node with that name |
| `choose = "Zstd"` | set the focused control's value | Right, then Left, until the row shows it | opens the combo and picks the entry (case-insensitive); `on` / `off` for a switch |
| `activate = "id"` | press a control | its `key`, or focus it and Enter | `via` first, then the button's action; a popover entry by keyboard |
| `pick = "path"` | answer every later file or folder dialog (`RUSTY_BACKUP_TEST_PICKS`, read by `controller::picks`); put it **before** what opens one | writes `*<TAB>path` to the test-picks file; the picker confirms it at once | writes `*<TAB>path` to the test-picks file |
| `fill = "text"` | clear the focused field and type | 512 Backspaces, then the text | sets the entry's text; a `pick` field goes through its dialog |
| `key = "Enter"` / `keys = [..]` | named keys: `Enter Tab Esc Up Down Left Right F1..F12 Ctrl+K` or one character | key bytes | xdotool; Enter on a focused button presses it |
| `type = "text"` | type text | bytes | xdotool |
| `wait = "text or id"` | wait up to `timeout_ms` (default 30 s) | polls the screen | polls node names, descriptions and entry text |
| `wait_gone = "text or id"` | wait until it is no longer shown (a staged count clearing) | polls the screen | likewise |
| `wait_file = "path"` | wait until the file exists and its size holds for half a second | the runner, the same for every shell | likewise |
| `expect` / `expect_not` | the screen shows / does not show it now | after a short settle | likewise |

Prefer `wait_file` for a job's end: it is the output itself, the same on every
shell, and GTK's log panel is not in its accessibility tree, so a log line is
unreadable there.

`only = "tui"` on an op runs it on that shell only. Keep these rare: one is a
sign the two shells don't offer the same thing.

### Rules

- **Check the bytes, not the screen.** End a flow with `rb-cli` steps that open
  what the front-end wrote (`inspect`, `ls`, `get` + `files_identical`, `fsck`).
  The same check then holds for every shell.
- **Build inputs with `rb-cli`** (`new`, `put`) rather than by hand in the UI,
  unless building them is the flow.
- **Smoke stays small:** about one or two flows per area, one representative
  filesystem each. Everything else is `full`.
- **One `ui` step can hold many ops.** The shell starts at the case's first `ui`
  step and keeps running across later steps, so a flow can drive, check with
  `rb-cli`, and drive again.
- **A daemon is a background step.** `background = true` starts that `rb-cli`
  and leaves it running until the case ends; `{port}` is a free local port, and
  the runner waits until it takes connections. `network.toml` serves an image
  this way.
- **A bug a flow finds is pinned, not worked around.** Record it in
  `docs/Regression_Bugs.md` and list the case in `data/known-failures.toml`; it
  then reads `xfail` until the fix turns it `XPASS`.

## Shells and drivers

| Shell | Driver | Needs | Status |
|---|---|---|---|
| `tui` | `runner/src/tui.rs`: `rb-cli tui` in a pseudo-terminal at 120x40, `RB_TUI_ASCII=1`, a scratch `HOME` with the update check off; a small screen model reads what ratatui draws | nothing | Linux, macOS, and Windows 10 1809+ over ConPTY (`RUSTY_BACKUP_CONFIG_DIR` keeps its config in the scratch home there). About 4 s a flow on Linux, 5 s on Windows |
| `gtk` | `drivers/gtk_driver.py` over AT-SPI, run by `runner/src/gtk.rs` under `dbus-run-session -- xvfb-run` (1600x2300, scratch `HOME`, `RUSTY_BACKUP_TEST_PICKS`, ALSA into a null sink so playback is silent) | `xvfb-run`, `dbus-run-session`, `xdotool`, python3 with the Atspi typelib, a built `frontends/gtk4` (`--gtk <path>`) | Linux. About 15 s a flow |
| `appkit` | `drivers/ax_driver.py`: PyObjC over AX, controls found by `AXIdentifier` (the controller id), so no `labels.toml` section | the AX (TCC) permission | planned: [docs/appkit_shell_plan.md](../docs/appkit_shell_plan.md) |
| `win32` | `drivers/uia_driver.py` over UI Automation, run by `runner/src/uia.rs` (`--win32 <exe>`, default `frontends/win32/target/release/rusty-backup-win32.exe`): controls found by `AutomationId`, the controller id, else by name; scratch config and picks file as for the other shells | Python with `comtypes` | Windows, CI target `win32`. The flows that list `win32` run on it as the screens land; its `labels.toml` section holds only behaviour drift (a copy into a host folder needs no Apply) ([docs/win32_shell_plan.md](../docs/win32_shell_plan.md)) |

A case whose shell has no driver, or whose driver's tools are missing, resolves
to `skip-tool` with the reason.

Without sudo, Xvfb can be unpacked: `apt-get download xvfb` then
`dpkg-deb -x xvfb_*.deb <dir>`, and put `<dir>/usr/bin` on `PATH`.

### What the GTK driver works around

Each of these failed silently until it was handled; keep them in mind before
"fixing" the driver.

- AT-SPI's screen coordinates are wrong under Xvfb: clicks use window
  coordinates plus the X window's origin.
- A popover is its own X surface, so a click into one misses. Popover rows (a
  dropdown's entries, a combo's choices) are reached by keyboard: a combo steps
  the counted number of rows from its current value, a menu walks until the row
  is focused.
- GTK 4 has no AT-SPI `grab_focus`, and some menu buttons ignore their action:
  a `via` dropdown is opened with a real click.
- A label exposes clipboard actions, so "has an action" doesn't mean "is a
  control": controls are ranked by role first.
- A combo row's title and subtitle are labels too; its value is the label in
  its list row.
- An unnamed field beside its caption (the size spin in Expand HFS Volume) is
  found from the caption; a spin button is set through its Value interface, as
  a click to commit typed text can land on its "-" button (8 MiB came out 7).
- The AT-SPI bus launcher's daemon outlives a plain kill: every child runs in
  its own process group.

## Finding what to press

`rb-regress tui-shot` starts the TUI the way a flow does, runs the ops given on
its command line (key names, `tab:NAME`, `type:TEXT`, `fill:TEXT`, `pick:PATH`,
`wait:TEXT`), and prints the screen after each one (reverse-video text is shown
beside its row):

```bash
rb-regress tui-shot Right Right Down type:/tmp/x.img Enter wait:partition
```

For GTK, the driver takes JSON ops on stdin, plus four that only help writing
flows: `{"op":"text"}` prints every showing node, `{"op":"tree","arg":"Label"}`
a node and its descendants with their states, `{"op":"ancestors","arg":"Label"}`
a node's parents with their actions, and `{"op":"click","arg":"Label"}` a real
click.

```bash
printf '%s\n' '{"op":"tab","arg":"Backup"}' '{"op":"text"}' |
  dbus-run-session -- xvfb-run -a -s "-screen 0 1600x2300x24" \
  python3 -I drivers/gtk_driver.py ../frontends/gtk4/target/release/rusty-backup-gtk /tmp/gtk-probe
```

## The flows

| File | Flows | Shells | Checks |
|---|---|---|---|
| `backup.toml` | a FAT image to zstd from the Backup screen; to one whole-disk VHD through the VHD options window (win32) | tui, gtk, win32 | restore + byte-compare; the .vhd read back |
| `restore.toml` | a backup folder to a new image file | tui, gtk, win32 | byte-compare |
| `disk-write.toml` | Write Image File through the disk writer window, with a synthesized MBR | win32 | inspect + byte-compare; GTK and the TUI can join |
| `commander.toml` | a file out of a zip in a host folder into an image; opening a zip directly (R-090, xfail) | tui, gtk, win32 | byte-compare |
| `files.toml` | inject and extract on 18 filesystems through Commander | tui, gtk, win32 | byte-compare + fsck; UFS inject is R-089, xfail |
| `inspect-hfs.toml` | Expand HFS Volume growing 1.4 MB to 8 MiB and shrinking 32 MiB to 8 MiB | tui, gtk | size, byte-compare, fsck |
| `build-cd.toml` | an IRIX EFS CD from a host folder on Build Disk; an MBR disk with a labelled FAT partition through the Partitions and Options steps (win32) | tui, gtk, win32 | byte-compare out of the ISO; table + label |
| `network.toml` | an `rb-cli serve` image opened in Inspect and backed up locally | tui, gtk | restore + byte-compare |
| `optical.toml` | browse an ISO and a CD CHD, extract a file through the UI | tui, gtk | against `rb-cli optical extract` |
| `audio.toml` | play a CD-DA track | gtk (no TUI player by design) | the position advances |
| `browse.toml` | Inspect's Browse opens the volume, Extract All writes it to a host folder, Close returns to Inspect; Edit Mode stages a new folder through the name prompt and Apply writes it; Edit as text replaces a file's text and Apply writes it | win32 | the TUI and GTK can join once their extract-to-folder step is checked |
