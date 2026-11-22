# Codebase review: rubric

The checklist every slice of the whole-codebase review is held to. It restates
the rules in CONTRIBUTING.md and CLAUDE.md as questions, plus the bug classes
this codebase has actually shipped. Findings go in [findings.md](findings.md).
The review only records findings. Fixes land afterwards, one cluster at a time,
tested with `scripts/regress-changed.py`.

## Severity

| Level | Meaning |
|---|---|
| **S1** | Loses or corrupts user data: a wrong write, a write past a partition, a restore that silently drops content. |
| **S2** | Wrong answer or crash on input we accept: a panic on a malformed image, a wrong size, a hang, an unbounded allocation. |
| **S3** | A rule broken, with no user-visible effect yet: layering, 1.73, streaming, error handling. |
| **S4** | Maintainability: duplication, dead code, unclear structure, misleading names or comments. |

## Questions per slice

### Correctness on hostile input (crates read images we did not write)

- Can any offset, length or count read from disk index a slice without a bounds
  check? (`buf[off..off + n]`, `u16::from_le_bytes(buf[x..x+2])`)
- Is arithmetic on on-disk values checked or saturating where an overflow is
  possible? (`start + len`, `block * block_size`, `count - 1`)
- Can an on-disk count size an allocation? (`vec![0; n]`, `Vec::with_capacity(n)`)
- Can a chain or tree walk loop forever? (cycle detection, a depth or count cap)
- Do `unwrap` / `expect` / `panic!` / `unreachable!` sit on a path that input reaches?

### Writes (S1 territory)

- Does every write stay inside the partition or volume it was given?
- Is the bitmap polarity right? (Amiga filesystems: set = free.) Is the endianness right?
- Are on-disk structs round-tripped with every field kept (format struct fidelity)?
- Is a multi-step edit atomic, or does it leave a recoverable state on error?

### Streaming and memory

- Does create / format / clone / repack write through `Write + Seek` with memory
  bounded by metadata? Is a volume-sized `Vec` built anywhere above 3 MB?
- Is I/O in 64 KiB to 1 MiB chunks and sector-aligned where it may hit a raw device?
- Does long work take `cancel_check` and check it at chunk boundaries?

### Portability

- Is any std API newer than Rust 1.73 called without the compat shims?
- Is `leading_zeros` / `leading_ones` called on a `u8` or `u16`? (mrustc gets it
  wrong; widen to `u32`.)
- Is a path or separator assumption Unix-only or Windows-only?

### Layering (MVC)

- Does the crate stay free of UI, app config and controller types?
- In `src/`: does view code (`src/gui`, the TUI, `frontends/`, `src/cli/verbs`)
  hold domain logic or validation that belongs in `src/model` or a controller?
- Does it bypass the routing helpers in `src/fs/mod.rs` with its own `match` ladder?

### Errors and logging

- Library code returns a typed error, application code `anyhow`. Does an
  unsupported operation return `Err(Unsupported)` rather than silently no-op?
- `eprintln!` outside tests? Unicode glyphs in user-visible strings?
- Is an error swallowed (`let _ =`, `.ok()`, `unwrap_or_default()`) where the
  caller needs to know?

### Maintainability

- Is this the same job as a function elsewhere? (Check
  [similar-functions.md](similar-functions.md) for the slice first.) Shared format
  helpers go in `disk-core`, app helpers in `src/model`.
- Does the code re-litigate a rejected pattern (CONTRIBUTING.md § Patterns to avoid)?
  That is not a finding unless there is new evidence.
- New comments longer than two lines; files past ~3000 lines with more than one job;
  dead code; names or comments that say something the code does not do.

## How a slice is reviewed

1. Run the mechanical checks first:
   `scripts/similar-fns` with `--unit <slice>`, clippy, and the greps in
   [findings.md](findings.md#mechanical-checks).
2. Read the code against the questions above, writers and parsers of untrusted
   input first.
3. Confirm each candidate before recording it: a failing test, a concrete input,
   or a quoted line that shows the defect. If a finding can't be confirmed, it is
   marked *plausible* and says what would confirm it.
4. Record it in the ledger with an ID, file:line, severity and status.
