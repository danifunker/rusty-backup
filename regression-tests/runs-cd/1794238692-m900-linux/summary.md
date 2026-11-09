# Regression run summary

- platform: `linux`
- host: `m900`
- rb-cli: `rb-cli 0.1.0`
- wall clock: 19.0s
- cases: 1

## Verdicts

| verdict | count |
|---------|------:|
| fail | 1 |

## Failures by group

Clustered so a single root cause reads as one entry.

### `flow.build.cd` — 1 failing

- `flow.build.efs-cd.gtk` -> `failures/flow.build.efs-cd.gtk/`

## Skips

- fixture gaps: 0 distinct IDs (see `missing-fixtures.md`)
- absent oracles: 0 tools (see `oracle-skips.md`)

## Next

Triage per REPORTING.md: read the clustered failures above, reproduce one
representative per cluster from its `cmd.txt`, and promote confirmed bugs to
issues by hand.
