# GHO password decryption — SOLVED

Status: **solved** (2026-05-31). Reverse-engineered from `ghostexp.exe` (Ghost
Explorer 11.5) and verified bit-exact against real password-protected fixtures.
Implemented in [`src/rbformats/gho_crypto.rs`](../src/rbformats/gho_crypto.rs);
wired through `GhoReader::open_with_password`, `source_reader`, the CLI
`--password` flag, and the GUI password prompt.

## The scheme

Ghost encrypts only the **bodies** of the inner record stream. The container
header, the 10-byte record headers, and inter-record bytes stay in clear text —
which is why an encrypted image's record skeleton is byte-identical to its
unencrypted twin. Each body is enciphered **independently**, with the cipher
state reset to the password seed at the start of every body.

### 1. Password -> 16-bit seed (`sub_4dc860`)

```
pw   = ascii_uppercase(password)[..11]    # case-insensitive, max 11 bytes
crc  = 0
for b in pw:
    crc = ((crc << 8) ^ CRC32_FWD[((crc >> 24) ^ b) & 0xff]) & 0xffffffff
seed = (crc ^ (crc >> 16)) & 0xffff
```

`CRC32_FWD` is the standard **forward** CRC-32 table (poly `0x04C11DB7`,
MSB-first, init 0, no reflection, no final XOR). The password is uppercased and
truncated to 11 bytes, so Ghost passwords are case-insensitive and only the
first 11 characters matter. Example: `"password"` -> `"PASSWORD"` -> seed
`0x9BE1`.

### 2. Verifier / wrong-password check (`SetPassword @ 0x4d14d0`)

When `password_flag` (header offset `0x0B`) is `1`, a 16-byte verifier follows
the prefix at offset `0x0C`. Decrypting it with the seed yields
`b"BinaryResearch\0"` in its first 15 bytes for the correct password (byte 16 is
framing). This is the fast wrong-password check (`gho_verify_seed`).

### 3. Body cipher (`decrypt @ 0x4dc920`, `encrypt @ 0x4dc8d0`)

A self-synchronising CRC-16-CCITT keystream cipher. The 256-entry 16-bit table
is built at runtime (`sub_4dc7d0` -> `sub_4dc710`) as a CRC-16-CCITT
(poly `0x1021`) **pre-seeded with `0x07A2`** and the index interleaved as it
shifts — a non-standard table.

```
state = seed                       # reset at the start of EACH body
for each ciphertext byte c:
    plain = c ^ (state & 0xff)
    state = ((state & 0xff) << 8) ^ TABLE16[(state >> 8) ^ c]
```

The feedback uses the ciphertext byte, so encrypt and decrypt share the same
state update.

## Why the earlier attempt failed

The 2026-05-26 attempt assumed CRC-16/**ARC** (poly `0xA001`) and brute-forced
only 6 fixed init constants. The real cipher is CRC-16-**CCITT** (poly `0x1021`)
with a **pre-seeded register (`0x07A2`)** producing a non-standard table, and the
register is seeded from a **16-bit password-derived value** (`0x9BE1` for
`password`) that fixed-init brute force never covered. The `nyarime/gho` Go
reference `CRC16Cipher` is unrelated to Symantec's real output.

## Validation

- **All 52,696 data-block bodies** (1.13 GB) of `11.5/GH11-password/GH11PW.GHO`
  decrypt bit-exact to the unencrypted twin `11.5/GH11/fulldisk.GHS`.
- Every record-body type (`0x0002 0x0004 0x0017 0x0102 0x0103 0x0104 0x0117
  0x0118`) decrypts correctly with per-body reset.
- The verifier decrypts to `BinaryResearch\0` with seed `0x9BE1`.
- High-compression (zlib, split) and raw-split fixtures decrypt bit-exact too.
- Unit tests in `gho_crypto.rs` pin the table, seed, verifier, and round-trip.

## Implementation notes

- Decryption is transparent: `SpanReader` (the single reader every GHO decode
  path uses) holds an optional body-range map + seed and decrypts body bytes on
  read while passing headers through. The map is built from a header-only scan
  (`collect_gho_body_ranges`) before decryption is switched on, so the complex
  file-aware / NTFS random-access read paths need no changes.
- **SECTOR-mode encrypted images are not yet supported** — all available
  encrypted fixtures are file-aware (`image_type=0x00`), so the SECTOR-mode body
  layout under encryption is untested. `open_with_password` errors clearly if the
  record stream can't be mapped.
