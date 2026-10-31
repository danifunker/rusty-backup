# dsk-tape

Tape images as browsable, editable trees, one Cargo feature per tape format.

**`sun2tape`** (the first format): Sun QIC tape images (`SUN2TAPE` `.qic`). Each tape file appears as one tree: tar sections, `compress(1)`-ed tar sections, and UFS miniroots.
- **Editing:** add, mkdir, rename, delete, chmod and chown inside those sections.
- **How edits are saved:** a tape has no free space, so every edit rebuilds the tape beside the original, which is kept as `<name>.bak`.
- **What stays as it was:** untouched tape files stay byte-identical, and the table of contents is copied unchanged.

The `sun2tape` feature uses `arc-mac` for the `compress(1)` codec. `arc-mac`'s decoder is LGPL-2.1-or-later, and it depends on `fs-mac`, which brings in the GPL-2.0-only `fs-mac-unicode`; a build with `sun2tape` is therefore GPL as a whole. This crate's own code is MIT.

Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
