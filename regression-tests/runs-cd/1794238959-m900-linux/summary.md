# Regression run summary

- platform: `linux`
- host: `m900`
- rb-cli: `rb-cli 0.1.0`
- wall clock: 136.1s
- cases: 2

## Verdicts

| verdict | count |
|---------|------:|
| fail | 1 |
| pass | 1 |

## Failures by group

Clustered so a single root cause reads as one entry.

### `flow.network` — 1 failing

- `flow.network.remote-inspect-backup.gtk` -> `failures/flow.network.remote-inspect-backup.gtk/`

## Skips

- fixture gaps: 0 distinct IDs (see `missing-fixtures.md`)
- absent oracles: 0 tools (see `oracle-skips.md`)

## Next

Triage per REPORTING.md: read the clustered failures above, reproduce one
representative per cluster from its `cmd.txt`, and promote confirmed bugs to
issues by hand.
