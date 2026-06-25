# DOSLFN.COM — third-party component

`DOSLFN.COM` in this directory is **DOSLFN**, a real-mode TSR that adds the DOS
long-filename API (INT 21h / 71xxh) that the FreeDOS/MS-DOS kernel does not
provide on its own. cb-dos's backup-folder member names are not 8.3-clean
(`metadata.json`, `partition-N.gz`, `partition-N.gz.crc32`, `manifest-N.json`,
…), so the boot media loads DOSLFN at startup (see `cbdos-autoexec.bat` and
`mkmedia.sh`) and a local-folder backup/restore "on the DOS host by itself"
works without the user setting anything up.

- **Bundled version:** v0.42 (2025-08)
- **File:** `doslfn.com` from the release zip (the FreeDOS-compatible build; the
  `doslfnms.com` variant in the same zip targets MS-DOS 7 and is *not* used).
  Only the bare `.com` is vendored — the `.tbl` codepage tables are for
  non-ASCII Unicode/DBCS mapping, which cb-dos's ASCII member names don't need.
- **Author:** Jason Hood / adoxa (`jadoxa@yahoo.com.au`)
- **Source / releases:** https://github.com/adoxa/doslfn/releases
- **Home page:** http://adoxa.altervista.org/doslfn/
- **License:** freeware, distributed with full source (`doslfn.asm` ships in the
  upstream zip). Vendored here, unmodified, for redistribution on the cb-dos
  boot media with attribution.

To refresh: download the latest `doslfn.zip` from the releases page above,
extract `doslfn.com`, and replace `DOSLFN.COM` here (keep it uppercase to match
the 8.3 name written onto the FreeDOS media).
