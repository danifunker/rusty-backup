# rb-cli: add a `--literal` flag so paths with glob metacharacters work

## Problem

`rb-cli` treats the positional path argument to `ls` (and `get`, `get-binhex`,
`rm`, and any other verb that takes an in-image source path) as a **glob
pattern**. Per `ls --help`:

> A plain path lists that directory's contents; patterns containing `*`, `?`,
> `[`, or `{` walk the volume and emit one line per match.

This means there is **no way to address a file or directory whose name contains
a glob metacharacter** (`*`, `?`, `[`, `]`, `{`, `}`). Such names are common on
classic-Mac HFS volumes — e.g. game folders like `Columns ][ 1.1`,
`Spaceward Ho! ƒ`, set/data files using brackets, etc. A "plain path" that
happens to contain `[` is mis-parsed as a glob and fails:

```
$ rb-cli ls -q boot.vhd "/Games/1992/Columns ][ 1.1"
error: invalid glob pattern "/Games/1992/Columns ][ 1.1":
  error parsing glob '/Games/1992/Columns ][ 1.1':
  unclosed character class; missing ']'
```

There is no escape hatch: there's no `--literal`/`--no-glob` flag, and escaping
the brackets is not documented/supported.

### Who hits this

External tooling (the MacAtrium `atrium harvest` pipeline) shells out to rb-cli
to **recursively copy exact files and folders** out of donor HFS images:
`ls <dir>` to enumerate, then `get-binhex <file>` per file, `mkdir`, `rm`, etc.
Every path it passes is an **exact literal path it already knows** — it never
wants glob expansion. A single bracket-named file anywhere in a game's folder
tree currently aborts the whole harvest.

## Fix

Add a `--literal` (alias `-L`, or `--no-glob`) flag to every verb that accepts
an in-image path argument — at least: `ls`, `get`, `get-binhex`, `rm`, `mkdir`,
`locate`, and `put` (for its destination path). When set, the path is matched
**verbatim** as an exact path, with no glob interpretation: `[`, `]`, `{`, `}`,
`*`, `?` are literal characters.

- Default behaviour (flag absent) is unchanged — paths with metacharacters still
  glob, plain paths still list a directory.
- With `--literal`, `*`/`?`/`[`/etc. never expand; the path is the path.
- It should compose with the existing `-q`/`--force`/etc. flags.

(An escaping convention — e.g. `[[]` for a literal `[` — would also work, but a
`--literal` flag is far simpler for programmatic callers, which always know they
want a literal path.)

## Acceptance criteria

```sh
# lists the exact directory, brackets and all:
rb-cli ls --literal boot.vhd "/Games/1992/Columns ][ 1.1"

# extracts the exact file even with a glob char in the name:
rb-cli get-binhex --literal boot.vhd "/Games/Foo/Bar [data].rsrc" out.hqx

# removes the exact path:
rb-cli rm --literal img.hda "/Apps/Maze Wars+ {old}"

# default (no flag) glob behaviour still works:
rb-cli ls boot.vhd "/Games/1990/**"      # still walks/expands
```

Add tests for names containing each of `[ ] { } * ?` (HFS allows all of these in
filenames). Keep the help text for the path argument noting the new flag.

## Notes for the consumer side

Once shipped, MacAtrium's `RbCli` wrapper (`tools/atrium-tool/src/rbcli.rs`) will
pass `--literal` on `ls`, `get`, `get-binhex`, and `rm`, since harvest always
addresses exact paths. Until then it works around this by skipping
bracket/brace-named source folders.
