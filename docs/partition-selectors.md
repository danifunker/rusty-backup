# Partition selectors — `@N`, `@sN`, `@DH0`

Every rb-cli verb that operates on one partition takes an `IMG@…` suffix (or the
equivalent `--partition` flag). There are three forms, and they answer different
questions.

| form | means | example |
|------|-------|---------|
| `@N` | the **position** in the list `inspect` prints — its `idx` column | `disk.img@2` |
| `@sN` | the partition table's **own slot**, spelled the way the platform spells it | `disk.img@s6` |
| `@NAME` | an **AmigaDOS device name** (RDB disks only), case-insensitive | `amiga.hdf@DH0` |

`rb-cli inspect` prints both numbers, so you can read either off the same row:

```
idx  slot  type                         start_lba            size  flags
  1     6  Apple_HFS (untitled)              1216        39.1 GiB  boot
  2     7  Apple_HFS (untitled 2)        81921216        72.7 GiB  boot
```

## Which to use

**`@N` is portable.** It works on every partition table, including the ones with
no slot concept, and it is what the error messages suggest.

**`@sN` is stable.** The position depends on which partitions rusty-backup
considers browsable, and that set changes as the tool learns about more
partition types — when `Apple_Driver_IOKit` was reclassified as a non-data
partition, `@1` on every Mac OS 9 ATA disk started meaning a different
partition. The slot names the table itself, so it does not move. Prefer it in
scripts and in anything you write down.

## How each table numbers its slots

The slot is spelled the way that platform's own tools spell it, so it matches
what you see elsewhere.

| table | slot numbering | matches |
|-------|----------------|---------|
| MBR | 1-4 primaries, 5+ logicals | `fdisk` |
| APM | from 1 | `diskutil`'s `disk4s6` |
| SGI | from 0 | IRIX `fx`, and the volume-header table `inspect` prints |
| Sun (SMI) | from 0 | `format(1M)` slices |
| Atari AHDI | from 1 | — |
| RDB | chain position from 0 | — (use the device name instead) |
| **GPT** | **none** | see below |
| X68000, superfloppy, BBC DSD | none | — |

### GPT has no slot form

The GPT parser drops unused entries as it reads the table, so by the time a
partition reaches the rest of the engine its original entry number is gone. A
GPT disk whose entries sit in slots 0, 1 and 4 — which is what you get after
deleting a partition — would report 1, 2, 3 while `gdisk` says 1, 2, 5.

Rather than invent a number, `@sN` on a GPT disk is refused and points at `@N`.
Recovering true GPT entry numbers means changing the parser and its writer, and
that is deliberately not part of this change.

## Where the numbers *aren't* selectors

Two places show a partition number that is neither `@N` nor `@sN`:

- **Backup folders** name their files `partition-N.<ext>` and key
  `metadata.json` by the source table's slot. `FOLDER@sN` addresses a backup
  partition directly; `FOLDER@N` counts down the recorded list.
- **Commander's multi-volume view** addresses volumes by position within its own
  browsable set (`/0/`, `/1/`), because that view also lists optical volumes
  which have no partition-table entry at all.
