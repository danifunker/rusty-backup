# mrustc-workaround patches for the vendored sources

Each file here is one patch to one crate in `../vendor/`, worked around
because mrustc cannot digest the crate as shipped. This directory *is* the
mrustc gap list for this project: docs/build-ppc-mrustc.md records the 21
gaps already fixed in mrustc itself; these are the deliberate leftovers.
They are applied by `scripts/apply-vendor-patches.py`, which the build
driver runs after `cargo vendor` and again before every transpile (the
vendor tree is regenerated, so patches must re-apply cheaply and safely).

## The contract

A patch is a Python module named after the crate it serves, exposing:

```python
CRATE   = "crc"                       # the crate, and the file's basename
TARGETS = ["crc/src/crc8.rs", ...]    # files to patch, relative to the vendor dir
GAP     = """why mrustc needs this -- keep it good, it is the documentation"""
UPSTREAM = None                       # mrustc issue/PR URL once filed, else None

APPLIED = r"..."   # regex present once patched   (searched with re.MULTILINE)
MATCH   = r"..."   # regex present when unpatched (searched with re.MULTILINE)

def patch(text: str, path: str) -> str:
    ...            # return the new text; NEVER write files yourself
```

## The three rules (the runner enforces them -- do not re-implement)

1. **Never write a file from `patch()`.** minicargo is timestamp-driven: an
   unconditional rewrite bumps the mtime, marks the crate dirty, and silently
   re-transpiles the ~800 MB engine unit on every build. The runner compares
   content and writes only on change; a transform stays a pure function.
2. **Detection is three-state per target file.** `APPLIED` matches: already
   patched, skip. Else `MATCH` matches: apply. Else: **hard failure** naming
   the crate -- a crate version bump that breaks a pattern must fail here,
   not surface as a baffling mrustc error 40 minutes into the transpile.
3. **No ordering in filenames.** All patches touch different crates, so there
   is no order to encode; the runner applies them `sorted()` for determinism.
   If a real inter-patch dependency ever appears, declare it in the module
   and teach the runner -- do not smuggle it into a numeric prefix.

A missing target file is skipped quietly: feature flags change which crates
are vendored at all.

## Adding a patch

Copy the shape of an existing module, write the `GAP` text as carefully as
the transform (it is why the next person doesn't re-derive your debugging),
then verify:

```sh
scripts/apply-vendor-patches.py --vendor-dir rb-cli-ppc/vendor   # applies
scripts/apply-vendor-patches.py --vendor-dir rb-cli-ppc/vendor   # all "already applied"
find rb-cli-ppc/vendor -newer /tmp/stamp                         # second run moved NO mtimes
```

## If we ever fix mrustc instead

Triage as of 2026-08:

- **Cheap upstream wins:** `rustversion` (make mrustc's `--version` put the
  version line last or alone) and `signal-hook-mio` (the `use ... as` parser
  gap -- also unblocks libyml, which would restore the `yaml` feature on
  PowerPC).
- **Real inference gaps, substantial work:** `crc`, `chrono`, `zstd-safe`
  (all "mrustc inference is weaker than rustc's"), plus `signal-hook`
  (arbitrary self types).
- **Workarounds forever:** `instability` (architectural to the proc-macro
  bridge) and `zstd-sys` (async is a missing language feature; dropping the
  `parallel` feature flag is simply correct).
