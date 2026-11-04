# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.1.1

### Added
- `Host::on_error(entry, err) -> Option<ErrorChoice>`: a host can decide each
  entry that fails below the starting point, as a prompt to the user would.
  `ErrorChoice::Skip` skips it and asks again at the next failure, `SkipAll`
  skips it and every later failure of the run without asking, `Stop` ends the
  run with the error (what was written stays, and the error passes up through
  every folder without asking again). The answer overrides `Options::on_error`.
- The default implementation returns `None`, which follows `Options::on_error`
  exactly as 0.1.0 did, so existing hosts compile and behave unchanged.

### Notes
- The change is additive only. `OnError` gains no variant and `Options` and
  `Summary` gain no field: either would break code that matches `OnError`
  exhaustively or builds those structs with a literal, which 0.1.x promises
  not to do. Skipped entries reach the host through `Host::failed` (as in
  0.1.0, now also for `Skip` / `SkipAll` answers), so the host keeps the list
  for its summary; a `Stop` returns an error, which a list in `Summary` could
  not survive.
- `Options::finder_info_sidecar` keeps its default (`true`). rusty-backup now
  uses that default for every caller.

## 0.1.0

- First release: `extract`, `extract_contents`, `count`, `existing_outputs`,
  `Options`, `Host`, `Summary`.
