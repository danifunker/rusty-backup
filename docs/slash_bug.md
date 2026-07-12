  You are working on MacAtrium, a classic-Macintosh game-launcher appliance, in the
  repo at /home/dani/repos/MacAtrium (on this same machine; everything below is
  local). Fix a bug in its build tooling.

  == BUG (one-line) ==
  When the build tool harvests a title whose classic-Mac file/app name contains a
  "/", it silently SKIPS that file during inject, so the built disk ends up one
  title short of what was selected.

  == SYMPTOM / how I hit it ==
  Building a 256-colour System 7.1 disk with a 15-title selection that included
  Oxyd 3.6 (id "oxyd-3-6") produced only 14 titles. The build log printed:
      warning: /Games/1992/Oxyd™ 3.6: skipping 'Oxyd™ b/w' (name contains '/')
  Oxyd's launchable application is literally named "Oxyd™ b/w" (b/w = black/white),
  and the "/" makes it get dropped. (I worked around it by swapping in a different
  title, but the tooling should handle this.)

  == THE TWO REPOS INVOLVED ==
  1. MacAtrium build tool (Rust):  /home/dani/repos/MacAtrium/tools/atrium-tool
     Build:  cargo build --release --manifest-path tools/atrium-tool/Cargo.toml
     Binary: tools/atrium-tool/target/release/atrium
  2. rb-cli (the HFS reader/writer it shells out to), in:  /home/dani/repos/rusty-backup
     Build:  (cd /home/dani/repos/rusty-backup && cargo build --release)
     Binary: /home/dani/repos/rusty-backup/target/release/rb-cli
     (MacAtrium build configs point rb_cli at that absolute path.)

  == EXACT CODE LOCATION (the skip) ==
  tools/atrium-tool/src/harvest.rs, in harvest_tree(), ~line 190:

      for e in &entries {
          if e.name.contains('/') {
              warnings.push(format!("{src_folder}: skipping '{}' (name contains '/')", e.name));
              continue;
          }
          // (next block skips names with glob metachars * ? [ ] { })
          ...
          let child_src = format!("{}/{}", src_folder.trim_end_matches('/'), e.name);
          ...
          rb.get_binhex(image, &child_src, &hqx)...      // extract both forks
          rb.put_binhex(target, &hqx, &dst_dir)...       // inject into the target image

  The RbCli wrapper is tools/atrium-tool/src/rbcli.rs (ls / get_binhex / mkdir_p /
  put_binhex). The same "/" name also flows into harvest_one() (app_dir =
  "{apps_root}/{app}") and into the catalog's "app" path, so a real fix touches all
  the places the name becomes part of a slash-joined path.

  == ROOT CAUSE (why it's skipped, not a lazy guard) ==
  rb-cli addresses an in-image file by a single slash-delimited path STRING
  (e.g. "/Games/1992/Oxyd™ 3.6/Oxyd™ b/w"). A "/" inside a filename is
  indistinguishable from a directory separator, so rb-cli would parse "Oxyd™ b/w"
  as ".../Oxyd™ b" then "w" and fail. The skip avoids that.

  IMPORTANT current state of rb-cli (verify, then build on it):
  - get-binhex ALREADY has -L/--literal and per its --help "always treats the source
    as an exact literal path (it never globs), so glob metacharacters in a name are
    addressed verbatim". So glob metachars (* ? [ ] { }) are NOT the problem anymore —
    the second skip block in harvest.rs (glob metachars) may now be removable/relaxable
    for get-binhex (check ls/mkdir/rm wrappers pass --literal too before relying on it).
  - An embedded "/" is STILL unaddressable, because "/" is the structural component
    separator, not a glob char. See /home/dani/repos/rusty-backup/PROMPT-literal-path-flag.md
    for the design history of literal addressing.

  == WHAT TO FIX (design it; here's the shape) ==
  This is likely a two-layer fix:
  (A) rb-cli: add a way to address a path COMPONENT that contains a literal "/".
      Natural option: honour a backslash escape "\/" as a literal slash within a
      component when --literal is set (glob.rs already does "\" escaping for glob
      chars), OR accept path components as a repeatable arg. Apply to the verbs the
      harvest pipeline uses: ls, get-binhex, mkdir, put-binhex, rm.
  (B) atrium-tool/src/harvest.rs: stop skipping "/"-names. Address the source with
      whatever escaping (A) provides, then SANITISE the destination on-disk name
      (map "/" -> "-" or "_"; do NOT use ":" — that's the HFS path separator) when
      building app_dir / dst_dir / mkdir / put_binhex, AND record the sanitised name
      everywhere the catalog "app" path and the files list are derived, so the
      launcher can find and launch it. The on-disk name will differ from the donor's
      original; that's fine and expected.
  If (A) turns out unnecessary (e.g. rb-cli can already address it some way you
  find), do the minimal correct thing. Keep the existing behaviour for normal names
  unchanged, and don't regress the glob-metachar handling.

  == REPRODUCE (fast: ~15s build, no emulator) ==
  Write /tmp/repro-slash.json:
      { "base_os": "7.1", "out": "/home/dani/repro-slash.hda",
        "selection": { "mode": "list", "ids": ["tetris-1-2","oxyd-3-6"] },
        "art_depths": ["1","8"], "app_mem_kb": [1024,768], "max_art_size": "384x384",
        "disk_size_mb": 120, "stage": "/tmp/repro-slash-stage",
        "rb_cli": "/home/dani/repos/rusty-backup/target/release/rb-cli" }
  Then (from the repo root):
      rm -rf /tmp/repro-slash-stage
      ./tools/atrium-tool/target/release/atrium image --config /tmp/repro-slash.json 2>&1 | grep -iE 'skip|item|warning'
  BEFORE the fix: the log warns about "Oxyd™ b/w" and the final line says "1 item"
  (tetris only). Oxyd's donor is "boot.vhd" (resolved from macpack_dir
  /home/dani/macpack-work), folder /Games/1992/Oxyd™ 3.6, offending app "Oxyd™ b/w".

  == VERIFY THE FIX ==
  1. Rebuild what you changed (rb-cli first if touched, then atrium-tool).
  2. Re-run the repro build. Expect the final line to say "2 items" (no skip warning
     for Oxyd).
  3. Confirm both landed and the catalog app-path is sanitised:
     - The presented set: /tmp/repro-slash-stage/dataset.present.jsonl should contain
       both "tetris-1-2" and "oxyd-3-6" (or "oxyd-b-w") — compare selected ids vs the
       present ids (e.g. with `comm -23`).
     - Grep that record's "app" field — it must point at the SANITISED on-disk name
       (e.g. "Apps/Oxyd 3.6/Oxyd b-w"), with no "/" inside the file component.
     - Optional but strong: confirm the file actually exists on the built disk:
       /home/dani/repos/rusty-backup/target/release/rb-cli ls /home/dani/repro-slash.hda "/MacAtrium/Apps/Oxyd 3.6"
  4. Optional end-to-end: boot /home/dani/repro-slash.hda on the Snow emulator
     (Mac II, 8-bit) and launch Oxyd to confirm it runs. See
     tools/snow-harness/README.md for the headless harness invocation (it needs a
     Mac II ROM + the MDC824 display-card ROM, both already on this box).

  == PROJECT CONVENTIONS ==
  - Commit straight to main (this project doesn't use branches/PRs yet).
  - Suspect our own code/data before the emulator or rb-cli; verify against reality.
  - Add a regression test where practical (atrium-tool and rb-cli both have unit
    tests; e.g. a harvest test with a synthetic "/"-named entry, and an rb-cli
    path-parse test for the escape).

  == DELIVERABLE ==
  A title whose Mac filename contains "/" harvests + injects + appears in the catalog
  (under a sanitised on-disk name) and launches, instead of being silently dropped.
  Report what you changed in each repo and paste the before/after of the repro build.

