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
rb-regress run --shell tui                    # one front-end
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
id = "flow.backup.zstd-roundtrip"
description = "Back up a {fs} image to zstd from the Backup tab, restore it, compare"
level = "smoke"
smoke_fs = ["fat16"]            # the other fs values run under full only
area = ["backup", "restore"]
fs = ["fat16", "hfs", "ext2"]   # one instance per value: id.fat16.tui, ...
shells = ["tui"]                # one instance per shell
capabilities = ["backup.run", "restore.full-disk"]

[[case.step]]                   # setup through rb-cli
args = ["new", "hd", "{fs}", "--size", "32M", "{scratch}/src.img"]
expect_exit = 0

[[case.step]]                   # the front-end
ui = [
  { tab = "Backup" },
  { focus = "Source" },
  { key = "Enter" },
  { type = "{scratch}/src.img" },
  { key = "Enter" },
  { wait = "1 partition" },
  { focus = "Format" },
  { choose = "zstd" },
  { key = "s", only = "tui" },
  { wait = "Backup complete", timeout_ms = 120000 },
]

[[case.step]]                   # verify the output, never the screen alone
args = ["inspect", "{scratch}/out/b"]
expect_exit = 0
```

Placeholders: `{fs}` (the matrix value), plus the usual `{scratch}`,
`{fixture}`, `{fixture_copy}`, `{cases}`.

### UI ops

Each op sets exactly one action. Controls are addressed by the **label the user
sees**, so one flow serves every shell that labels a control the same way (a
label that differs between shells is a parity bug in itself).

| Op | Meaning | TUI driver |
|---|---|---|
| `tab = "Backup"` | switch screen | Left / Right until the tab bar shows it in reverse video |
| `focus = "Format"` | move to a control | Down, then Up, until the focused row (`> ` or reverse video) holds the label |
| `choose = "zstd"` | set the focused control's value | Right, then Left, until the focused row holds the value |
| `key = "Enter"` / `keys = [..]` | named keys: `Enter Tab Esc Up Down Left Right F1..F12 Ctrl+K` or one character | key bytes |
| `type = "text"` | type text | bytes |
| `fill = "text"` | clear the focused text field, then type | 512 Backspaces, then the bytes |
| `wait = "text"` | wait for text, up to `timeout_ms` (default 30 s) | polls the screen |
| `expect = "text"` / `expect_not` | the screen shows / does not show it now | after a short settle |

`only = "tui"` on an op runs it on that shell only: the escape hatch for a key
that is the TUI's own (the GTK shell clicks a button instead). Keep these rare.

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

## Shells and drivers

| Shell | Driver | Status |
|---|---|---|
| `tui` | `runner/src/tui.rs`: `rb-cli tui` in a pseudo-terminal at 120x40, `RB_TUI_ASCII=1`, a scratch `HOME` with the update check off; a small screen model reads what ratatui draws | Linux, macOS. Windows needs ConPTY: skip-platform for now |
| `gtk` | AT-SPI, from `scripts/gtk4-gui-suite.py` | planned |
| `appkit` | Accessibility API (`accessibilityIdentifier`) | with the AppKit shell |
| `win32` | UI Automation (`AutomationId`) | with the Win32 shell |

A case whose shell has no driver here resolves to `skip-tool`.

## Finding what to press

`rb-regress tui-shot` starts the TUI the way a flow does, runs the ops given on
its command line (key names, `type:TEXT`, `fill:TEXT`, `wait:TEXT`), and prints the screen after each one (reverse-video text is
shown beside its row):

```bash
rb-regress tui-shot Right Right Down type:/tmp/x.img Enter wait:partition
```
