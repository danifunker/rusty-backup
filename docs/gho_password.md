# GHO password decrypt — open problem

Status: **unsolved**. Attempted in the GHO 5.6 slice (2026-05-26); cipher
implementation reverted because real-fixture verification revealed the
Go reference's cipher does not match Symantec's actual encryption.

## What we know (verified against real fixture pairs)

We have three encrypted fixtures, all created with password `"password"`,
all image_type=`0x00` (file-aware mode), one Fast-LZ and two zlib-high:

- `11.5/GH11-password/GH11PW.GHO` (single file, compression=None)
- `11.5/gh11-pwd-split/gh11pwd.GHO` + 11 `.GHS` spans (compression=None)
- `11.5/gh11-hicompression-with-password/hipwd.GHO` + 5 spans (compression=High)

The hipwd set has an unencrypted twin (`11.5/GH11-hicompression/High.GHO`)
with **identical record stream and identical body_lens** in the first
three `0x0002` records. That gives us known plaintext / ciphertext pairs
for reverse-engineering the cipher.

### Verified findings

1. **The record skeleton is plaintext.** `GhoRecordIter` walks an
   encrypted file cleanly — record headers carry the right magic
   (`0x012F18D8`), body_lens point past valid record boundaries. Only
   the data **inside** each `0x0002` (cluster-data) body is encrypted.
2. **The 16-byte password verifier at container offset 0x0C is
   identical across all three "password" fixtures.** It is purely a
   function of the password — usable as a fast wrong-password check
   once the cipher is known, but its plaintext content has not been
   reverse-engineered (decrypting it with the CRC-16/ARC model from
   `nyarime/gho/crypto.go` produces gibberish).
3. **The cipher is reset per data block, not per partition.** Encrypted
   `0x0002` bodies all start with the same two ciphertext bytes
   `0x99 0xa4`, which against the known plaintext `0x78 0x01` (zlib
   header) gives the same first two keystream bytes `0xE1 0xA5`
   regardless of block index in the file. This is the opposite of the
   `nyarime/gho/reader.go` model, which keeps cipher state across all
   blocks within a partition.
4. **The cipher state advances by plaintext, not by ciphertext or by
   stepping a counter.** Block 0 and block 1 share their first two
   plaintext bytes (`0x78 0x01`) and produce identical keystream bytes
   `0xE1 0xA5 0xDB`; they diverge at byte 3 because plaintext byte 2
   differs (`0xed` vs `0x9d`). Matches the "Decrypt: state = update(state,
   plain)" shape from the Go reference.
5. **The exact cipher polynomial/init/byte-ordering is NOT
   CRC-16/ARC.** Exhaustive brute force over 336 combinations
   (polynomials `{0xA001, 0x8408, 0xC002, 0x8005, 0x1021, 0x9C0B,
   0xC867}` × init `{0xFFFF, 0x0000, 0x8408, 0xA001, 0xFFFE, 0x1D0F}`
   × LSB/MSB shift × low-byte/high-byte key × single/double password
   processing) — **zero matches** for the target keystream
   `0xE1 0xA5 ...` from password `"password"`. The cipher must use
   something other than a standard CRC-16 table-driven update.

### What we ruled out

- CRC-16/ARC LSB shift with init 0xFFFF (the `nyarime/gho/crypto.go`
  model) — gives keystream starting `0x37 0x03 ...`, not `0xE1 0xA5
  ...`.
- All standard CRC-16 polynomial / init / byte-order combinations.
- Per-partition cipher state (decrypting block 1 with continued state
  from block 0 gives garbage; per-block reset gives a consistent
  first-two-bytes pattern).

## Why this matters

The `nyarime` Go library's `CRC16Cipher` looks like clean-room
reverse-engineering, but it was almost certainly written aspirationally
to match the **writer** (which round-trips against itself) without ever
being tested against real Symantec-encrypted output. Don't trust it.

The Ghost header cipher (`fixupPRNG` in `nyarime/gho/fixup.go`) is a
completely separate algorithm — a 32-bit state advanced by
`state += ROR(state, 7)`, used only on header bytes 2..n. Not the same
as the data cipher.

## Recommended next steps for a future "password" slice

1. **Reverse-engineer the verifier format first.** The 16-byte verifier
   is deterministic given the password; figure out what plaintext it
   encodes (likely a constant magic, or the password itself padded).
   Once known, the verifier lets us test cipher candidates without
   touching multi-MB block streams.
2. **Find the real cipher.** Reasonable starting hypotheses now that
   CRC-16/ARC is out:
   - A custom CRC variant with non-standard table (probe by computing
     forward differences in the keystream from short plaintexts).
   - A multiplicative LCG seeded by the password (state =
     state * mult + add).
   - The CRC-32 polynomial truncated to 16 bits.
   - Symantec's `ghofixup` PRNG (`state += ROR(state, 7)`) repurposed
     for data blocks but with password-derived seed.
3. **Disassemble the Symantec encryption routine.** `ghost.exe` from
   Ghost 11.5 (available in archive.org collections) carries the
   actual implementation. The `ghofixup.exe` PDB path quoted in
   `nyarime/gho/fixup.go`
   (`c:\depot\ghost\gsstrunk\ghost\utilityapps\ghofixup\`) suggests
   the main Ghost binary has comparable symbol coverage.
4. **End-to-end testing is blocked on file-aware reconstruction**
   regardless. All three encrypted fixtures are file-aware mode, so
   even with a correct cipher we cannot produce a mountable image
   until the file-aware filesystem rebuilder lands. A SECTOR-mode
   encrypted fixture would unblock end-to-end testing — created with
   `ghost.exe -ia -pwd=password` (need a Ghost 11.5 environment).
