# similar-fns

Finds Rust functions that do the same job under different names: the
duplication that copy-paste detectors miss, because nothing was copied. Each
session wrote its own helper instead of finding the one that already existed.

It is a review aid. It is not shipped, not part of the workspace, and not run in
CI.

## Run it

```bash
cargo run --release --manifest-path scripts/similar-fns/Cargo.toml -- \
    --repo . --out docs/review/similar-functions.md
```

| Flag | Default | Meaning |
|---|---|---|
| `ROOT ...` | `crates src frontends` | directories to scan, relative to `--repo` |
| `--min-tokens N` | 60 | ignore bodies shorter than this many shape tokens (one-line getters) |
| `--threshold J` | 0.8 | Jaccard similarity a near-shape cluster needs |
| `--top N` | 60 | groups listed per section |
| `--unit PREFIX` | all | keep only groups that touch one unit, e.g. `crates/fs-mac`, for a slice review |
| `--include-tests` | off | also scan `#[cfg(test)]`, `#[test]` and `tests/` |
| `--keep-tables` | off | also compare table-like bodies (an enum-to-string `match`), which all look alike |
| `--out FILE` | stdout | where the markdown goes |

## How it decides two functions are alike

Each body (with its parameters and return type) is reduced to a *shape*:

- **kept:** keywords, primitive types (`u32`, `usize`), common std names (`Vec`,
  `Option`, `Read`), and every name that is called: functions, methods, macros,
  and lowercase path heads such as `u32::`;
- **replaced:** local names and parameters (`$i`), fields (`$f`), user types
  (`$T`), numbers (`$n`) and strings (`$s`).

Called names stay because they carry the meaning. `from_be_bytes` and
`from_le_bytes` keep two readers apart, as they should be.

Then it reports:

1. **Exact-shape groups:** identical shapes, meaning the same code with other names.
2. **Near-shape clusters:** the overlap of 6-token shingle sets is at least
   `--threshold`, found with MinHash (32 hashes, 8 LSH bands) and confirmed with
   exact Jaccard. That keeps the run linear in the number of functions.
3. **Same-name free functions:** a free function whose name is defined in more
   than one unit (crate, top-level `src/` module, front-end), such as eight
   separate `read_u32`s.

Groups that span more than one unit are listed first, then by the lines a merge
would remove (all members minus the longest).

## Reading the report

A match is a lead, not a verdict. Before merging, check what format code often
varies on purpose:

- endianness, and bitmap polarity (Amiga filesystems use set = free);
- error types that only look alike once the names are abstracted;
- trait impls across filesystems: identical bodies there usually mean the trait
  wants a default method, not a free helper.

Shared helpers for format code go in `disk-core`. App code goes in `src/model/`
or a controller. Merges follow the crate rules in CONTRIBUTING.md (version bump,
Rust 1.73).
