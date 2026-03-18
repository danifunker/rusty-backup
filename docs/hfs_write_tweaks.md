# Edit Mode Stabilization Plan — Staged Writes

Redesign the editing flow across all filesystems to stage operations before
committing, replacing the current immediate-write-per-operation model.

---

## Current Behavior (problems)

Today, each Add File / New Folder / Delete action in the GUI:
1. Opens a fresh `EditableFilesystem` handle (read+write)
2. Mutates the filesystem immediately (allocates blocks, inserts catalog
   records, writes data to disk)
3. Calls `sync_metadata()` to flush metadata to disk
4. Drops the handle

This means:
- No preview of what will happen before committing
- No space validation until the write is already underway
- No way to batch operations (each one opens/syncs/closes independently)
- No undo — once committed, the disk image is modified
- Partial state can be flushed if one operation in a batch fails
- The user sees no "Saving..." indicator during writes
- Exiting edit mode silently keeps all changes (no confirmation)

---

## Implementation Steps

### Step 1: Remove `sync_metadata` from Individual FS Operations

**Goal:** Change the `EditableFilesystem` contract so individual mutation
methods only modify in-memory state. The caller becomes responsible for
calling `sync_metadata()` when ready to flush.

**Files:**
- `src/fs/filesystem.rs` — update trait doc comment
- `src/fs/hfs.rs` — remove `self.do_sync_metadata()?` from `create_file`,
  `create_directory`, `delete_entry`, `set_type_creator`,
  `write_resource_fork`, `set_blessed_folder`
- `src/fs/hfsplus.rs` — same
- `src/fs/fat.rs` — same
- `src/fs/ntfs.rs` — same
- `src/fs/exfat.rs` — same
- `src/fs/ext.rs` — same (if editing exists)
- `src/fs/prodos.rs` — same (if editing exists)

**Changes:**

1. Update the `EditableFilesystem` trait doc in `filesystem.rs`:

```rust
/// Trait for filesystems that support write operations (add/delete files).
///
/// Individual mutation methods (create_file, delete_entry, etc.) modify
/// in-memory state only. The caller MUST call `sync_metadata()` after all
/// mutations are complete to flush changes to disk. This enables batching
/// multiple edits into a single atomic write.
pub trait EditableFilesystem: Filesystem {
```

2. In each filesystem's `create_file`, `create_directory`, `delete_entry`,
   `set_type_creator`, `write_resource_fork`, `set_blessed_folder`:
   remove the `self.do_sync_metadata()?` call at the end. Leave all other
   logic unchanged.

3. In `src/gui/browse_view.rs`, the existing callers (`add_host_file`,
   `add_host_directory`, `perform_delete`, bless folder click handler)
   already call `efs.sync_metadata()` explicitly after the operation.
   Verify these still work — they should, since the explicit sync is
   already there. The only difference is we're no longer double-syncing.

**Tests:** Run `cargo test` — all existing editing tests should pass.
The tests that call `create_file` etc. and then verify state should still
work because the in-memory state is correct; tests that verify on-disk
state may need an explicit `sync_metadata()` call added.

**Verification:** Run the app, enter edit mode, add a file, verify it
appears. Delete it, verify it's gone. This confirms the GUI's explicit
`sync_metadata()` calls are sufficient.

---

### Step 2: HFS Snapshot/Rollback for Atomic In-Memory Mutations ✅ DONE

**Goal:** Protect in-memory state so that a failed HFS B-tree operation
(e.g. out of free nodes during a split) doesn't leave the catalog in an
inconsistent state for subsequent operations in the same session.

**Files:**
- `src/fs/hfs.rs`

**Changes:**

1. Add `#[derive(Clone)]` to `HfsMasterDirectoryBlock` (~line 102).
   This struct contains fixed-size fields and arrays, so Clone is trivial.

2. Add two helper methods on `HfsFilesystem<R: Read + Write + Seek>`:

```rust
fn snapshot(&self) -> (Vec<u8>, Option<Vec<u8>>, HfsMasterDirectoryBlock) {
    (self.catalog_data.clone(), self.bitmap.clone(), self.mdb.clone())
}

fn restore_snapshot(
    &mut self,
    snap: (Vec<u8>, Option<Vec<u8>>, HfsMasterDirectoryBlock),
) {
    self.catalog_data = snap.0;
    self.bitmap = snap.1;
    self.mdb = snap.2;
}
```

3. Wrap each mutating `EditableFilesystem` method (`create_file`,
   `create_directory`, `delete_entry`, `set_type_creator`,
   `write_resource_fork`, `set_blessed_folder`) in a snapshot guard:

```rust
fn create_file(&mut self, ...) -> Result<FileEntry, FilesystemError> {
    let snap = self.snapshot();
    let result = (|| {
        // ... all existing mutation logic, unchanged ...
        Ok(file_entry)
    })();
    if result.is_err() {
        self.restore_snapshot(snap);
    }
    result
}
```

Note: `write_data_to_blocks` writes block data to disk before the catalog
insert. On rollback, those blocks become orphaned on disk but the bitmap
says "free" so they'll be reused on next allocation. This is harmless.

**Tests:**
- Existing edit tests should still pass unchanged.
- Add a new test: `test_hfs_rollback_on_failed_create` — create a minimal
  HFS image with a nearly-full B-tree, trigger a create that fails due to
  no free B-tree nodes. Verify the catalog is unchanged from before the
  failed attempt. Run fsck — zero issues.

---

### Step 3: HFS B-tree Index Rebuild After Split ✅ DONE

**Goal:** Replace the fragile incremental index maintenance during B-tree
splits with a full bottom-up rebuild using the battle-tested fsck function.

**Background:** The editing code's `insert_catalog_record` tries to
incrementally insert separator keys into parent index nodes after a leaf
split. This can leave stale separators, missing entries, or broken sibling
links — all issues that fsck's `rebuild_index_nodes` was written to fix.

**Files:**
- `src/fs/hfs_fsck.rs` — make `rebuild_index_nodes` `pub(crate)`
- `src/fs/hfs.rs` — rewrite the split path in `insert_catalog_record`

**Changes:**

1. In `hfs_fsck.rs`, change the visibility of `rebuild_index_nodes` (~826):

```rust
pub(crate) fn rebuild_index_nodes(
    catalog_data: &mut [u8],
    node_size: usize,
    report: &mut RepairReport,
) {
```

2. Rewrite the `Err` arm of `insert_catalog_record` in `hfs.rs` (~776):

```rust
Err(_) => {
    // Leaf full — split, insert into correct half, rebuild index
    let mut h = BTreeHeader::read(&self.catalog_data);
    let (new_idx, split_key) =
        btree_split_leaf(&mut self.catalog_data, node_size, leaf_idx, &mut h)?;

    let target = if Self::catalog_compare(key_record, &split_key)
        == Ordering::Less
    {
        leaf_idx
    } else {
        new_idx
    };
    let t_offset = target as usize * node_size;
    let t_node = &mut self.catalog_data[t_offset..t_offset + node_size];
    btree_insert_record(t_node, node_size, key_record, &Self::catalog_compare)?;

    h.leaf_records += 1;
    h.write(&mut self.catalog_data);

    // Full index rebuild replaces incremental parent chain insertion
    let mut dummy_report = super::fsck::RepairReport {
        fixes_applied: Vec::new(),
        fixes_failed: Vec::new(),
        unrepairable_count: 0,
    };
    super::hfs_fsck::rebuild_index_nodes(
        &mut self.catalog_data, node_size, &mut dummy_report,
    );

    Ok(())
}
```

This removes all the `_parent_chain` / `btree_insert_into_index` /
`btree_grow_root` logic from the split path.

3. Also call `rebuild_index_nodes` after any `remove_catalog_record` that
   empties a leaf (in `delete_entry`), to clean up stale separator keys in
   parent index nodes. Add the rebuild call after the thread record removal:

```rust
// After both remove_catalog_record calls, rebuild index if tree has depth > 1
let header = BTreeHeader::read(&self.catalog_data);
if header.depth > 1 {
    let node_size = header.node_size as usize;
    let mut dummy_report = super::fsck::RepairReport {
        fixes_applied: Vec::new(),
        fixes_failed: Vec::new(),
        unrepairable_count: 0,
    };
    super::hfs_fsck::rebuild_index_nodes(
        &mut self.catalog_data, node_size, &mut dummy_report,
    );
}
```

**Tests:**
- Add `test_hfs_many_inserts_with_splits` — create 50+ files on a small
  HFS image to trigger multiple leaf and index splits. Run fsck after —
  zero issues.
- Add `test_hfs_delete_empties_leaf` — fill a leaf, trigger a split, delete
  all entries from one leaf. Insert a new entry. Run fsck — zero issues.

---

### Step 4: HFS `update_parent_valence` Error on Miss ✅ DONE

**Goal:** Make `update_parent_valence` return an error when the parent
directory record is not found, instead of silently returning `Ok(())`.

**Files:**
- `src/fs/hfs.rs` — `update_parent_valence` (~696)

**Changes:**

Change the final fallthrough from:

```rust
    }   // end while
    Ok(())
}
```

to:

```rust
    }   // end while
    Err(FilesystemError::NotFound(
        format!("parent directory CNID {} not found in catalog", parent_id)
    ))
}
```

With Step 2 (snapshot/rollback) in place, this error triggers a clean
rollback of the individual operation.

**Tests:**
- Existing tests should still pass (the parent is always findable in
  normal operation).
- Add `test_hfs_valence_miss_rolls_back` — manually corrupt a leaf chain
  fLink, attempt to create a file. Verify the error is returned and the
  snapshot is fully restored.

---

### Step 5: `StagedEdit` Data Structures and Staging Logic ✅ DONE

**Goal:** Add the `StagedEdit` enum and staging queue to `BrowseView`.
Rewire Add File / New Folder / Delete to append to the queue instead of
writing immediately. No visual overlay yet — just the queue and apply.

**Files:**
- `src/gui/browse_view.rs`

**Changes:**

1. Add the `StagedEdit` enum (at module level):

```rust
#[derive(Debug, Clone)]
enum StagedEdit {
    AddFile {
        parent: FileEntry,
        name: String,
        host_path: PathBuf,
        size: u64,
        options: CreateFileOptions,
    },
    AddFileData {
        parent: FileEntry,
        name: String,
        data: Vec<u8>,
        options: CreateFileOptions,
    },
    CreateDirectory {
        parent: FileEntry,
        name: String,
        options: CreateDirectoryOptions,
    },
    DeleteEntry {
        parent: FileEntry,
        entry: FileEntry,
    },
    DeleteRecursive {
        parent: FileEntry,
        entry: FileEntry,
    },
    SetTypeCreator {
        entry: FileEntry,
        type_code: String,
        creator_code: String,
    },
    BlessFolder {
        entry: FileEntry,
    },
}
```

2. Add `StagedEditSummary`:

```rust
struct StagedEditSummary {
    files_added: usize,
    dirs_added: usize,
    entries_deleted: usize,
    bytes_added: u64,
    free_after: Option<u64>,
    errors: Vec<String>,
}
```

3. Add fields to `BrowseView`:

```rust
staged_edits: Vec<StagedEdit>,
show_unsaved_dialog: bool,
```

Initialize in `Default`: `staged_edits: Vec::new()`,
`show_unsaved_dialog: false`.

4. Rewire `add_host_file` — instead of opening an `EditableFilesystem` and
   writing, push a `StagedEdit::AddFile` to the queue:

```rust
fn add_host_file(&mut self, host_path: &Path, parent: &FileEntry) -> Result<(), String> {
    let name = host_path.file_name()
        .and_then(|n| n.to_str())
        .ok_or("invalid filename")?
        .to_string();
    let size = std::fs::metadata(host_path).map_err(|e| e.to_string())?.len();

    self.staged_edits.push(StagedEdit::AddFile {
        parent: parent.clone(),
        name,
        host_path: host_path.to_path_buf(),
        size,
        options: CreateFileOptions::default(),
    });
    Ok(())
}
```

5. Rewire `add_host_directory` — push `CreateDirectory` + recurse children
   as `AddFile` entries. Each becomes a staged edit.

6. Rewire `perform_delete` — push `DeleteEntry` or `DeleteRecursive` instead
   of immediately calling `efs.delete_entry()`.

7. Rewire bless folder click — push `StagedEdit::BlessFolder`.

8. Add `apply_staged_edits` method:

```rust
fn apply_staged_edits(&mut self) {
    let summary = self.validate_staged_edits();
    if !summary.errors.is_empty() {
        self.edit_result = Some(format!(
            "Cannot apply: {}", summary.errors.join("; ")
        ));
        return;
    }

    let mut efs = match self.open_editable_fs() {
        Ok(fs) => fs,
        Err(e) => {
            self.edit_result = Some(format!("Error opening filesystem: {e}"));
            return;
        }
    };

    let edits: Vec<StagedEdit> = self.staged_edits.drain(..).collect();
    let total = edits.len();

    for (i, edit) in edits.into_iter().enumerate() {
        let result = match edit {
            StagedEdit::AddFile { ref parent, ref name, ref host_path, size, ref options } => {
                match File::open(host_path) {
                    Ok(mut file) => efs.create_file(parent, name, &mut file, size, options)
                        .map(|_| ()),
                    Err(e) => Err(FilesystemError::Io(e)),
                }
            }
            StagedEdit::AddFileData { ref parent, ref name, ref data, ref options } => {
                let mut cursor = std::io::Cursor::new(data);
                efs.create_file(parent, name, &mut cursor, data.len() as u64, options)
                    .map(|_| ())
            }
            StagedEdit::CreateDirectory { ref parent, ref name, ref options } => {
                efs.create_directory(parent, name, options).map(|_| ())
            }
            StagedEdit::DeleteEntry { ref parent, ref entry } => {
                efs.delete_entry(parent, entry)
            }
            StagedEdit::DeleteRecursive { ref parent, ref entry } => {
                efs.delete_recursive(parent, entry)
            }
            StagedEdit::SetTypeCreator { ref entry, ref type_code, ref creator_code } => {
                efs.set_type_creator(entry, type_code, creator_code)
            }
            StagedEdit::BlessFolder { ref entry } => {
                efs.set_blessed_folder(entry)
            }
        };

        if let Err(e) = result {
            self.edit_result = Some(format!(
                "Error on edit {}/{}: {e}", i + 1, total
            ));
            // Remaining edits lost — user must re-stage them
            return;
        }
    }

    if let Err(e) = efs.sync_metadata() {
        self.edit_result = Some(format!("Error saving to disk: {e}"));
        return;
    }

    self.edit_result = Some(format!("Applied {total} edit(s) successfully"));
    self.invalidate_all_caches();
}
```

9. Add `validate_staged_edits` method:

```rust
fn validate_staged_edits(&self) -> StagedEditSummary {
    let mut summary = StagedEditSummary {
        files_added: 0, dirs_added: 0, entries_deleted: 0,
        bytes_added: 0, free_after: None, errors: Vec::new(),
    };

    let mut bytes_needed: u64 = 0;
    let mut bytes_freed: u64 = 0;
    let mut names_seen: HashSet<(String, String)> = HashSet::new(); // (parent_path, name)

    for edit in &self.staged_edits {
        match edit {
            StagedEdit::AddFile { parent, name, host_path, size, .. } => {
                summary.files_added += 1;
                summary.bytes_added += size;
                bytes_needed += size;
                if !names_seen.insert((parent.path.clone(), name.clone())) {
                    summary.errors.push(format!("Duplicate name '{name}' in {}", parent.path));
                }
                if !host_path.exists() {
                    summary.errors.push(format!("File not found: {}", host_path.display()));
                }
            }
            StagedEdit::AddFileData { parent, name, data, .. } => {
                summary.files_added += 1;
                summary.bytes_added += data.len() as u64;
                bytes_needed += data.len() as u64;
                if !names_seen.insert((parent.path.clone(), name.clone())) {
                    summary.errors.push(format!("Duplicate name '{name}' in {}", parent.path));
                }
            }
            StagedEdit::CreateDirectory { parent, name, .. } => {
                summary.dirs_added += 1;
                if !names_seen.insert((parent.path.clone(), name.clone())) {
                    summary.errors.push(format!("Duplicate name '{name}' in {}", parent.path));
                }
            }
            StagedEdit::DeleteEntry { entry, .. }
            | StagedEdit::DeleteRecursive { entry, .. } => {
                summary.entries_deleted += 1;
                if !entry.is_directory() {
                    bytes_freed += entry.size;
                }
            }
            _ => {}
        }
    }

    // Check free space
    if let Ok(mut efs) = self.open_editable_fs() {
        if let Ok(free) = efs.free_space() {
            let projected = free + bytes_freed;
            if bytes_needed > projected {
                summary.errors.push(format!(
                    "Not enough space: need {} but only {} available",
                    partition::format_size(bytes_needed),
                    partition::format_size(projected),
                ));
            }
            summary.free_after = Some(projected.saturating_sub(bytes_needed));
        }
    }

    summary
}
```

10. Clear `staged_edits` in the `close()` method and when toggling edit mode
    off (without the unsaved dialog — that comes in Step 7).

**Tests:** Enter edit mode, add a file, verify it doesn't appear on disk
yet. Click Apply, verify it appears. Delete a file, verify it's still on
disk until Apply.

---

### Step 6: Edit Toolbar with Apply Button and Space Summary ✅ DONE

**Goal:** Update the edit toolbar to show the "Apply Edits (N)" button,
current free space, and projected free space after staged edits.

**Files:**
- `src/gui/browse_view.rs` — `render_edit_toolbar`

**Changes:**

1. Rewrite `render_edit_toolbar`:

```rust
fn render_edit_toolbar(&mut self, ui: &mut egui::Ui) {
    ui.horizontal(|ui| {
        let parent = self.current_parent_entry();
        let parent_name = if parent.path == "/" {
            "root".to_string()
        } else {
            parent.name.clone()
        };
        ui.label(
            egui::RichText::new(format!("Editing: /{parent_name}"))
                .color(egui::Color32::from_rgb(100, 180, 255)),
        );

        ui.add_space(8.0);

        if ui.button("Add File...").clicked() {
            self.add_file_dialog();
        }
        if ui.button("New Folder...").clicked() {
            self.new_folder_name.clear();
            self.show_new_folder_dialog = true;
        }

        // Delete button
        let has_selection = self.selected_entry.as_ref()
            .map(|e| e.path != "/").unwrap_or(false);
        if ui.add_enabled(has_selection, egui::Button::new("Delete")).clicked() {
            // ... existing delete staging logic ...
        }

        // Bless folder (HFS only)
        if self.is_hfs_type() {
            // ... existing bless logic, rewired to stage ...
        }

        ui.add_space(8.0);
        ui.separator();
        ui.add_space(8.0);

        // Apply button — only shown when edits are staged
        if !self.staged_edits.is_empty() {
            let label = format!("Apply Edits ({})", self.staged_edits.len());
            if ui.button(label).clicked() {
                self.apply_staged_edits();
            }
        }

        ui.add_space(8.0);

        // Free space display
        if let Ok(mut efs) = self.open_editable_fs() {
            if let Ok(free) = efs.free_space() {
                ui.label(format!("Free: {}", partition::format_size(free)));

                // Projected free space after staged edits
                if !self.staged_edits.is_empty() {
                    let summary = self.validate_staged_edits();
                    if let Some(after) = summary.free_after {
                        let color = if after < free / 10 {
                            egui::Color32::from_rgb(255, 100, 100) // red
                        } else if after < free / 4 {
                            egui::Color32::from_rgb(255, 200, 100) // yellow
                        } else {
                            egui::Color32::from_rgb(100, 200, 100) // green
                        };
                        ui.colored_label(color,
                            format!("After: {}", partition::format_size(after)));
                    }
                }
            }
        }
    });
}
```

2. Update the archive editing info banner:

```
Current: "Editing temporary copy. Changes saved when you exit edit mode."
New:     "Editing temporary copy. Click 'Apply Edits' to write changes."
```

**Tests:** Manual — enter edit mode, stage some edits, verify the Apply
button appears with count, verify free space and projected space show
correctly. Verify Apply works.

---

### Step 7: Unsaved Changes Dialog ✅ DONE

**Goal:** When the user exits edit mode (or clicks Close) with pending
staged edits, show a confirmation dialog with three options.

**Files:**
- `src/gui/browse_view.rs`

**Changes:**

1. Intercept the edit mode toggle. Where the user clicks "Edit Mode ON"
   to exit (~line 494):

```rust
if btn.clicked() {
    if self.archive_edit_ctx.is_some() {
        if !self.edit_mode {
            self.start_archive_extract();
        } else {
            // Exiting edit mode — check for unsaved edits
            if !self.staged_edits.is_empty() {
                self.show_unsaved_dialog = true;
            } else {
                self.start_archive_compress();
            }
        }
    } else {
        if self.edit_mode && !self.staged_edits.is_empty() {
            // Has pending edits — show dialog instead of toggling
            self.show_unsaved_dialog = true;
        } else {
            self.edit_mode = !self.edit_mode;
            if !self.edit_mode {
                self.edit_result = None;
                self.show_new_folder_dialog = false;
                self.pending_delete = None;
            }
        }
    }
}
```

2. Also intercept the "Close" button (~line 534):

```rust
if ui.button("Close").clicked() {
    if self.edit_mode && !self.staged_edits.is_empty() {
        self.show_unsaved_dialog = true;
    } else {
        self.close();
        return;
    }
}
```

3. Add `render_unsaved_dialog` method:

```rust
fn render_unsaved_dialog(&mut self, ui: &mut egui::Ui) {
    if !self.show_unsaved_dialog {
        return;
    }

    let summary = self.compute_staged_summary();

    egui::Window::new("Unsaved Changes")
        .collapsible(false)
        .resizable(false)
        .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
        .show(ui.ctx(), |ui| {
            ui.label(format!(
                "You have {} unsaved edit(s).",
                self.staged_edits.len()
            ));
            ui.add_space(4.0);

            // Summary line
            let mut parts = Vec::new();
            if summary.files_added > 0 {
                parts.push(format!("{} file(s) added", summary.files_added));
            }
            if summary.dirs_added > 0 {
                parts.push(format!("{} folder(s) added", summary.dirs_added));
            }
            if summary.entries_deleted > 0 {
                parts.push(format!("{} item(s) deleted", summary.entries_deleted));
            }
            if !parts.is_empty() {
                ui.label(parts.join(", "));
            }
            if summary.bytes_added > 0 {
                ui.label(format!(
                    "Net data: +{}", partition::format_size(summary.bytes_added)
                ));
            }

            ui.add_space(8.0);
            ui.horizontal(|ui| {
                if ui.button("Discard Edits").clicked() {
                    self.staged_edits.clear();
                    self.show_unsaved_dialog = false;
                    self.exit_edit_mode();
                }
                if ui.button("Apply Edits").clicked() {
                    self.show_unsaved_dialog = false;
                    self.apply_staged_edits();
                    if self.edit_result.as_ref()
                        .map(|r| !r.starts_with("Error"))
                        .unwrap_or(true)
                    {
                        self.exit_edit_mode();
                    }
                }
                if ui.button("Cancel").clicked() {
                    self.show_unsaved_dialog = false;
                }
            });
        });
}
```

4. Add `exit_edit_mode` helper:

```rust
fn exit_edit_mode(&mut self) {
    self.edit_mode = false;
    self.edit_result = None;
    self.show_new_folder_dialog = false;
    self.pending_delete = None;
    self.staged_edits.clear();

    if self.archive_edit_ctx.is_some() {
        self.start_archive_compress();
    }
}
```

5. Add `compute_staged_summary` (lightweight version of `validate` for
   display only — no error checking, no filesystem open):

```rust
fn compute_staged_summary(&self) -> StagedEditSummary {
    let mut s = StagedEditSummary {
        files_added: 0, dirs_added: 0, entries_deleted: 0,
        bytes_added: 0, free_after: None, errors: Vec::new(),
    };
    for edit in &self.staged_edits {
        match edit {
            StagedEdit::AddFile { size, .. } => { s.files_added += 1; s.bytes_added += size; }
            StagedEdit::AddFileData { data, .. } => { s.files_added += 1; s.bytes_added += data.len() as u64; }
            StagedEdit::CreateDirectory { .. } => { s.dirs_added += 1; }
            StagedEdit::DeleteEntry { .. } | StagedEdit::DeleteRecursive { .. } => { s.entries_deleted += 1; }
            _ => {}
        }
    }
    s
}
```

6. Call `render_unsaved_dialog(ui)` in the main `show()` method, after the
   delete dialog render.

**Tests:** Manual — stage edits, click edit mode toggle, verify dialog
appears. Test Discard (edits gone, mode off), Apply (edits committed, mode
off), Cancel (dialog closes, mode stays). Test with archive context too.

---

### Step 8: Virtual Overlay for Pending Edits in Browse View ✅ DONE

**Goal:** Show staged adds with a visual indicator and staged deletes with
dimmed/strikethrough text in the directory listing, so the user sees a
preview of what will change.

**Files:**
- `src/gui/browse_view.rs` — directory listing rendering

**Changes:**

1. Add a helper to check if an entry is pending deletion:

```rust
fn is_pending_delete(&self, entry: &FileEntry) -> bool {
    self.staged_edits.iter().any(|edit| match edit {
        StagedEdit::DeleteEntry { entry: e, .. }
        | StagedEdit::DeleteRecursive { entry: e, .. } => e.path == entry.path,
        _ => false,
    })
}
```

2. Add a helper to collect pending adds for a given parent path:

```rust
fn pending_adds_for(&self, parent_path: &str) -> Vec<FileEntry> {
    self.staged_edits.iter().filter_map(|edit| match edit {
        StagedEdit::AddFile { parent, name, size, .. }
            if parent.path == parent_path =>
        {
            let path = if parent.path == "/" {
                format!("/{name}")
            } else {
                format!("{}/{name}", parent.path)
            };
            Some(FileEntry::new_file(name.clone(), path, *size, 0))
        }
        StagedEdit::CreateDirectory { parent, name, .. }
            if parent.path == parent_path =>
        {
            let path = if parent.path == "/" {
                format!("/{name}")
            } else {
                format!("{}/{name}", parent.path)
            };
            Some(FileEntry::new_directory(name.clone(), path, 0))
        }
        _ => None,
    }).collect()
}
```

3. In the directory listing render code, when iterating over entries:
   - If `is_pending_delete(entry)`: render with strikethrough or dimmed
     color (e.g. `egui::Color32::from_rgb(150, 150, 150)`)
   - After rendering real entries, append `pending_adds_for(parent_path)`
     and render those with a green tint or "+" prefix
   - Pending-add entries should not be selectable for content preview
     (they don't exist on disk yet)

4. Prevent the user from staging a delete on a pending-add entry (it
   doesn't exist yet — just remove it from staged_edits instead). Similarly,
   prevent staging an add with a name that conflicts with a pending add
   in the same parent.

**Tests:** Manual — add 2 files (staged), verify they appear in green/with
indicator. Delete an existing file (staged), verify it appears dimmed.
Click Apply, verify everything normalizes.
