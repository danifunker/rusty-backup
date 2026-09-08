# PPC split-TU: the promoted-name suffix has to be per translation unit

`scripts/ppc-split-tu.py` and `docs/build-ppc-mrustc.md` are **modified in the
working tree and not committed**. That is deliberate for now — the change is
carried here rather than in history. If you are picking this up, the code is
already in place; read this to know why, and what it costs to get wrong.

The change is the one that used to live as `../rusty-backup-ppc-mrustc-fixes.patch`
(a loose file next to the repo, dated 2026-08-07 21:30, never applied). That patch
is now redundant — its content is in the tree, plus one repair: it had joined a
new docstring line onto the following paragraph without a newline.

## What the bug is

The splitter promotes crate-local `static` monomorphisations to external linkage
so unit 3 can see unit 5's, and renames each one so the promoted copy does not
collide with the *owning* crate's own global definition. Until now that rename
was a constant suffix, `RENAME_SUFFIX = b"__rbsplit"`.

A constant suffix is not enough. It separates a promoted copy from the crate that
owns the symbol, which is what it was written for — but it does **not** separate
two *split crates in the same link*. Each promotes its own copy of the same shared
`alloc` monomorphisation, both to the same new name:

```
ld: duplicate symbol _ZRG2cF10alloc0_0_05alloc_B0g__rbsplit in
    target/ppc-g4/librustdesk_ppc_agent-1_0_0.rlib.o
    and target/ppc-g4/libprotobuf-3_0_0.rlib.split/u2.o
```

`rename_suffix(base)` now returns `__rbsplit_<sha256(basename)[:8]>`, and the
suffix is part of `.split/.stamp` so that changing the scheme invalidates splits
made by an older version of the script — the *source* is unchanged, so nothing
else in the pipeline would notice.

## Why this looked like a regression, and was not

The RustDesk PowerPC agent shipped a working `1.0.0` on 2026-08-18. A clean
rebuild of the same code then failed on the link above, which reads like
something underneath had changed — a different mrustc, a bigger protobuf. It had
not. Measured, not reasoned about:

| claim | check | result |
|---|---|---|
| the newer mrustc transpiles protobuf big enough to split | md5 of `libprotobuf-3_0_0.rlib.c` in both trees | **identical**, 84,446,548 bytes |
| only one crate split in the working build | `*.rlib.o.parts` in the 08-18 tree | **both** split, 4 units each |

The working build linked because of *stale artifacts*, not because it was
correct:

| build | crate | promoted names | suffix |
|---|---|---|---|
| 08-18 (linked) | protobuf | 8468 | `__rbsplit_129751b8` |
| 08-18 (linked) | agent | 11426 | `__rbsplit` |
| clean rebuild (failed) | protobuf | 8468 | `__rbsplit` |
| clean rebuild (failed) | agent | 11434 | `__rbsplit` |

`129751b8` is `sha1("libprotobuf-3_0_0.rlib.c")[:8]` — the same per-TU scheme,
an earlier hash function. Protobuf had been split on 08-07 by a salted build of
the script; the script was reverted to the committed constant-suffix version
half an hour later; the agent was split on 08-18 by that reverted script. Because
protobuf's `.split/.stamp` was still current it was never re-split, so it kept
its 08-07 salted headers *and* its salted objects. One crate salted, one not — no
clash. It was luck. Any clean rebuild of that same tree would have failed
identically.

The lesson worth keeping: a constant `RENAME_SUFFIX` is only safe while exactly
one crate on the link line is oversized. Do not "simplify" it back.

## The trap when you change the splitter

`.stamp` invalidating the split is necessary but not sufficient, because the
splitter only runs when `ppc-cc-remote.py` is invoked, and that only happens when
minicargo decides to build the crate. **minicargo's staleness check is the
0-byte `.rlib` metadata stub, not `.rlib.o`.** Deleting just the objects makes
minicargo report the crate cached (`38c/39t`) and go straight to the link:

```
gcc: error: .../libprotobuf-3_0_0.rlib.o: Input/output error
gcc: error: .../librustdesk_ppc_agent-1_0_0.rlib.o: Input/output error
```

To force a re-split of an oversized crate, remove its **`.rlib`** (and, for
tidiness, the `.rlib.o`, `.rlib.o.parts` and `.rlib.split/`). That re-runs mrustc,
which re-emits the C and calls the wrapper, which re-splits with the current
suffix. Confirmed working: `.split/.stamp` then reads

```
1788402848 84446548 4 __rbsplit_f36a03ee
```

## Invariants — do not lose these when editing the splitter

`docs/build-ppc-mrustc.md` (~lines 895–1000) is the source of truth. The short
version, because each of these once produced a binary that linked and then died:

- **No symbol may be defined in more than one unit at all, weak included.** Weak
  tentative definitions are the silent one: four equally-eligible copies, the
  linker keeps a zero-filled one, and the binary dies on the first vtable call.
- `panic_fmt` cannot be renamed — mrustc's own `#define` comes later and wins. It
  is made weak instead.
- Tentative definitions go `extern` in the header with storage in exactly one
  unit; gcc 10 is `-fno-common`.
- The pre-flight Mach-O collision sweep must exclude weak duplicates
  (`n_desc & N_WEAK_DEF`) or the ~188 coalesced vtables read as failures.
- Do **not** reach for `PPC_SPLIT_UNITS=1` or a raised `PPC_BIG_TU_BYTES` to dodge
  a link error. Both put an oversized `__text` back in one object, which fails
  later with an out-of-range branch — the exact problem splitting solves. The
  ceiling is per input object and sits between 30 and 61 MB of `__text`.

## Related

- `docs/build-ppc-mrustc.md` — the full reasoning, and the measurements behind it.
- The consumer that surfaced this: the RustDesk PowerPC agent, built from
  `/home/dani/repos/rustdesk-vintage/rustdesk-ppc-agent` against the isolated
  `~/repos/mrustc-ppc` toolchain. See the `[[mrustc-per-target-worktrees]]` memory
  for why each vintage target has its own mrustc worktree.
