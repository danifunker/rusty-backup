# WinImage IMZ encryption — reverse-engineering notes

Status: **format characterized, cipher NOT yet broken.** Standard Rijndael
constructions keyed by MD5(password) do not match a known-plaintext oracle.

## Samples (developer-local, not in repo)
`/Users/dani/new-fixtures/imz/`
- `w98_boot_nopassword.imz` — plain ZIP, deflate.
- `w99_boot_withpassword.imz` — encrypted, password = `password`.
- Both wrap the **same** 1,474,560-byte floppy (plaintext deflate CRC
  `0xd4c4bba0`). Decisive: gives a **true known-plaintext oracle**.

## What the format IS (established)
- Outer container is an ordinary ZIP. The encrypted entry sets the standard GP
  flag bit0 (`0x0001`), method = 8 (deflate).
- **NOT** PKWARE ZipCrypto, **NOT** WinZip-AES (no `0x9901` "AE" extra field).
  It is WinImage's own scheme. Per WinImage docs: MD5(password) → 128-bit key,
  then Rijndael — applied to the already-deflated bytes (compress-then-encrypt).
- Encrypted entry body = **135-byte header** followed by ciphertext whose length
  is **exactly** the plaintext deflate length (892,949 B).
  - Equal length ⇒ a length-preserving / streaming mode (OFB/CFB/CTR or a stream
    cipher); **rules out** padded CBC/ECB (892,949 is not a multiple of 16/32).
- WinImage adds a custom extra field `SI` (id `0x4953`, data `804d495210800000`)
  on the encrypted entry only. Both entries carry `WI` (`0x4957`,
  `0100000000801600`).
- 135-byte header (hex):
  `0457a6a8e2b0cdbd0080000008030388882d9943cb89d00994a0ab168c26ecab3cb070a81ecefd6f9460653400f5dbac3d4ab52b8e4aa365fc145bc9015bad5a0fa957b2d1523c9091057a82a2802c11a0a589bd76ed158d6e08c6a5c0bac1454a4f7901623b84b1cd8d5126158fb8bc878af743036c43de9a709a9f24c1f5b604f867b031f206`

## Known plaintext / keystream
Because the body is length-preserving, `KS = ciphertext XOR plaintext_deflate`
is the **actual 892,949-byte keystream**.
- `KS[0:16]  = ba846f747e402e6b3bbf6b6786adda96`
- `KS[16:32] = 12927f6550b34672694938e249e1288c`
- `KS[32:48] = 90bd7d1f006e7b25086e561bd1e4c863`

MD5("password") candidates:
- ascii        `5f4dcc3b5aa765d61d8327deb882cf99`
- ascii+nul    `5ce194693ff0cb6fb23bcbaaeae1fbec`
- utf16le      `b081dbe85e1ec3ffc3d4e7d0227400cd`
- utf16le+nul2 `cb870dd3c912b0dc03c7bbf7d81a6aee`

## What was tested and FALSIFIED
Tooling: a verified pure-Rijndael impl (FIPS-197 AES-128 enc+dec vectors pass),
supporting 128/192/256-bit blocks (`/tmp/rijndael*.py`). All tests use the real
keystream and IV-independent block relations (blocks 1→2→3), so a wrong IV cannot
hide a correct key/mode.

Ruled out (all returned "none"):
- **Modes**: ECB, CBC (also excluded by length), OFB, CFB-128, CFB-8, CTR
  (big/little-endian counter, and 32-bit low-word counter).
- **Block sizes**: 128-bit (AES) and 256-bit (pre-AES Rijndael reference).
- **Key derivation**: `MD5(password)` in ascii / ascii+nul / utf16le /
  utf16le+nul2 / uppercase; **and** salted `MD5(salt‖pw)` / `MD5(pw‖salt)` for
  every 4/8/12/16/32-byte salt window inside the 135-byte header
  (1,645 key candidates total).
- **Brute-IV inflate**: decrypt-then-inflate over every header offset as IV,
  both block sizes, OFB+CFB — no offset produced a valid ≥4 KiB deflate stream.
  (A naive openssl sweep reports spurious "OK 2-byte" hits — those are
  coincidental 2-byte valid deflate fragments, not the 1.44 MB image. Ignore
  anything that doesn't inflate to ≥1 MiB / CRC `0xd4c4bba0`.)

Conclusion: the scheme is not "plain MD5 key + textbook Rijndael mode." The doc
hint omits real construction details — candidates for what's non-standard:
custom key schedule, a custom keystream/feedback, a transformed plaintext, or
key material derived from the 135-byte header in a non-obvious way.

## WinImage 6.1 binary IS UNPACKED — crypto in cleartext (2026-05-30)
The real binary is `/Users/dani/Downloads/WinImage 6.1/winima61/winimage.exe`
(532,480 B, PE32 i386, ImageBase 0x400000). It is **NOT packed** (`upx -d` →
"NotPackedException"; EP 0x442470 is a normal MSVC `mov eax,fs:[0]` SEH prologue;
342 imports across CRTDLL/USER32/GDI32/comdlg32/ADVAPI32/etc; sections
.text/.rdata/.data/.rsrc/.reloc with sane entropy). NOTE: the *sibling*
`winima61.exe` (428 KB, one dir up) is the self-extracting installer — ignore it.
The 3 `wim*.dll` are 16-bit thunks — no crypto.

All the cipher primitives sit in plaintext in `winimage.exe` (file offsets; add
ImageBase-section delta for VAs):
- **MD5**, standard: init constants `01234567 89abcdef fedcba98 76543210` at file
  `0x3b8be`; sine table T[0]=`d76aa478` at `0x3b0b0`, T[1]=`e8c7b756` at `0x3b0d0`.
- **Rijndael with static S-boxes** (NOT the log/alog ref impl): forward S-box
  `63 7c 77 7b f2 6b 6f c5 …` at file `0x4d7a0`; inverse S-box
  `52 09 6a d5 …` at file `0x4f8a0`. (No Rcon array found — likely computed.)
- Crypto-routine name/UI strings: `KEYCRYPT` @`0x48b2c`, `PASSWORDOPEN` @`0x48b5c`,
  `ENTERNEWPASSWORD` @`0x48b6c`; `MD5 Checksum of uncompressed data` @`0x470d0`.

This is a straightforward **static disassembly** job — no unpacking needed. The
earlier "binary is PACKED" note in this file was WRONG (it analyzed the wrong
file / bad data) and has been deleted.

## To break it properly (next step)
Disassemble (capstone, x86-32) the routine referenced by `KEYCRYPT` /
`PASSWORDOPEN` and the S-box at `0x4d7a0`:
1. Find the function that consumes the forward S-box → that's the Rijndael block
   encrypt; read its round structure to confirm 128-bit block + key length.
2. Find the MD5 routine (init constants `0x3b8be`) → confirm password→key
   derivation (encoding, any salt from the 135-byte header).
3. Find the caller that ties password+header+deflate-stream together → that
   reveals the mode (the 135-byte header = IV/salt/verifier?) and how the
   keystream is applied.
4. Reproduce in Rust; validate against the KP oracle (decrypt
   `w99_boot_withpassword.imz` body → inflate → CRC `0xd4c4bba0`).
Then implement in `src/rbformats/imz.rs`. Until proven, the reader still rejects
encrypted IMZ with an accurate error.

## Static RE progress — crypto located; it's REFERENCE Rijndael (var. block) (2026-05-30)
Disassembled `winimage.exe` with capstone (x86-32). Confirmed map (VAs,
ImageBase 0x400000). NOTE: an earlier draft of this section claimed "AES-128
only / cipherInit ECB=1,CBC=2 / 0x43c805 = crypt routine" — those were
PREMATURE/WRONG (capstone landed mid-instruction at 0x43c805, which starts with a
bogus `inc ebp`; and the round count is data-driven, not fixed at 10). Corrected
findings below.

- **Rijndael block ENCRYPT** = function entry `0x43d915` (the `0x43d7e0` I cited
  before was wrong). Body confirmed by reading the disasm:
  - Round count comes from `[ebx+0x1c]` (the cipher-context's block-columns field
    `BC`), `sub edx,6` then loop — this is the **reference `rijndaelEncrypt()`**
    shift-row logic, which supports **variable block size**. The `cmp edx,8`
    branch (0x43d9fd/0x43da03) is the special ShiftRows case for **BC=8 = 256-bit
    block**. So the impl is the Daemen/Rijmen *reference* (variable 128/192/256
    block), NOT AES-128-fixed.
  - Forward S-box at `0x44f3a0` used as `xor al,[ecx+0x44f3a0]` (8 lookups per
    half-round). Round keys read from `0x4525a0` (`mov [ebp-8],0x4525a0`).
- **Rijndael block DECRYPT** ≈ `0x43dac6+` — uses 4 inverse T-tables
  `Td0..Td3` at `0x4519a0, 0x451da0, 0x4521a0` (+`0x4525a0`?) AND inverse S-box
  `0x4514a0`. (Optimized decrypt path.)
- **MD5** entry `0x43c460`, init constants inline at `0x43c4be`, sine table
  `0x43c0b0`. Reached indirectly (no direct E8 caller; called via the
  password→key path).
- Crypto region `0x43c460–0x43e300` is full of **CRT IAT calls** (malloc/memcpy/
  MultiByteToWideChar/etc — 90 indirect calls) and externally-entered functions
  at `0x43cb30` (7 callers — the general "crypt buffer" entry),
  `0x43dba2`/`0x43ded2` (the IMZ driver at 0x43e3xx–0x43e7xx calls these),
  `0x43e1d7`, `0x43e2ba`.

### Honest status of the BLOCK relation
Black-box already falsified Rijndael (128 AND 256 block) in ECB/CBC/OFB/CFB/CTR
with key=MD5(password)/5 encodings. Combined with "it's reference Rijndael," the
non-standard piece is the **key derivation** (how password+MD5 → the key bytes
and key length) and/or **mode/IV wiring via the 135-byte header**. Static reading
of the wrapper hit two walls this session: (a) the wrapper is heavy with CRT
calls, so pure reading is slow; (b) the terminal kept dropping multi-line output.

### Decisive next step = EMULATION (not more static reading)
Emulate with Unicorn, but correctly this time:
1. Map the on-disk image at 0x400000 (it's already unpacked — do NOT reuse the
   bogus `/tmp/wi_dump.bin`).
2. Stub the CRT IAT slots the crypto uses (malloc/calloc/free/memcpy/memmove/
   memset/strlen — slot VAs resolvable from the import dir).
3. Call the **externally-entered** crypt entry `0x43cb30` (7 app callers ⇒ the
   real API) with candidate arg layouts (buf, len, password, …), feeding the
   encrypted sample body; read back the buffer; validate by inflate→CRC
   `0xd4c4bba0`.
4. If that doesn't self-reveal the layout, trace one app caller of `0x43cb30`
   (e.g. `0x40b172`) to see exactly how it's invoked.
NOTE: macOS bash has no `timeout`; use Python `signal.alarm` or Unicorn's
`emu_start(..., timeout=…)` (microseconds) to bound runs. emu2/emu3 this session
faulted because they targeted the wrong entry (0x43c805) and didn't stub CRT.

### Key-derivation hypotheses worth testing (cheap, offline)
The reference `makeKey(key,dir,keyLen,char *keyMaterial)` takes keyMaterial as an
**ASCII-HEX string** (keyLen/4 hex chars). Candidates that the black-box pass did
NOT try (it only used raw MD5 bytes as the key):
- key = ASCII hex of md5(pw) (32 chars) → interpreted as keyLen=128 keyMaterial
  (this *should* equal raw-MD5, already failed) — but confirm endianness.
- keyLen=256 with the 32-hex-char md5 → different effective key bytes.
- Block size 256 (BC=8) paired with 128-bit key — since encrypt fn supports it.
Combine with the header as IV in CBC/CFB and re-run the offline Rijndael oracle.

### Tooling state
capstone 5.0.7, pefile, unicorn 2.1.4, upx 5.1.1 installed. macOS bash has NO
`timeout` cmd — bound emulation via Unicorn `emu_start(timeout=µs)` or Python
`signal.alarm`.

### Scripts (sessions 1-2)
`/tmp/find_crypto.py` (sig scan), `/tmp/find_sbox_xref.py` (S-box xrefs),
`/tmp/analyze.py` (fn boundaries+callers), `/tmp/disasm_enc.py` (+`enc_dis.txt`,
the encrypt-fn disasm that proved variable-block reference Rijndael),
`/tmp/cfb1.py` (CFB1 test, failed). emu2/emu3 targeted the wrong entry
(0x43c805) and lacked CRT stubs — rewrite against `0x43cb30` with IAT stubs.

## Repro
`/tmp/imz_full.py` (keystream + stream-relation + brute-IV),
`/tmp/imz_keybrute.py` (salted-key brute), `/tmp/imz_ctr.py` (CTR),
`/tmp/rijndael.py` + `/tmp/rijndael_dec.py` (verified primitive). Re-derive the
keystream/header from the two samples; everything else is offline.

## Session 2 FINAL state — algorithm proven, reproduction blocked on C++ object graph

What is now CERTAIN (from disassembly of the unpacked winima61/winimage.exe):
- Cipher = Daemen/Rijmen **reference Rijndael**, **128-bit key** (key length 0x80
  passed explicitly at the call site 0x40b172: `push edi; push 0x80; push edx;
  call 0x43cb30`).
- **Key derivation uses MD5**: `makeKey` (0x43cd9e) calls MD5 (0x43c460). So the
  AES key is MD5-derived (consistent with WinImage's docs and our black-box
  finding that the *block cipher* is standard).
- Crypto is implemented as a **C++ cipher class**:
  - `0x43cb30` = crypt method. `ecx`=this (context). `[this+0x60]` = key-instance
    pointer (0 ⇒ not yet keyed). On first use: `call 0x43cccd(0x10)` allocates a
    16-byte key object → `[this+0x60]`, then calls `makeKey` 0x43cd9e.
    Stack args: arg0=`[ebp+8]`=status-out byte, arg1=`[ebp+0xc]`=pointer that is
    dereferenced (`mov edx,arg1; mov ecx,[edx]`), arg2=`[ebp+0x10]`.
  - `makeKey` 0x43cd9e: arg0=direction (`cmp eax,1; ja error` ⇒ 0 or 1),
    arg1=esi=pointer to a **nested context struct** (`eax=*arg1; edx=[eax+4];
    edi=[eax+4+edx]` — walks an already-built object), arg2=output ptr.
- Round-key buffer global at 0x4525a0; fwd S-box 0x44f3a0; inv S-box 0x4514a0;
  decrypt T-tables 0x4519a0/0x451da0/0x4521a0; MD5 init 0x43c4be, fn 0x43c460.

THE BLOCKER: cold-calling any of these from emulation faults on null/garbage
fields because they expect a fully-constructed C++ object graph (context with
nested key-instance, length fields, etc.). Reconstructing that graph by hand is
possible but multi-step and error-prone.

### Recommended path to FINISH (pick one)
1. **Windows dynamic** (fastest, highest confidence): run winimage.exe under
   x64dbg/x32dbg on Windows, open `w99_boot_withpassword.imz` with password
   "password", breakpoint at MD5 (0x43c460) to read the exact hashed bytes, and at
   the crypt loop to read the 16-byte key + observe mode/IV use of the 135-byte
   header. 20 minutes on a Windows box/VM. We have none here (macOS).
2. **Emulate the password-open handler top-down**: entry via the `PASSWORDOPEN`
   string xref 0x430173 (or `KEYCRYPT` 0x42f869), which constructs the context
   naturally. Heavier (pulls in GUI/file plumbing) but self-contained on macOS.
3. **Build the C++ context by hand** in Unicorn and call 0x43cb30. Needs the exact
   struct layout (fields at +0x60 etc.) — reverse the constructor first.

### Working emulation infra (reusable, in /tmp)
- `/tmp/h.py` — flat-maps image 0x400000..0x500000, stubs CRT IAT
  (malloc/calloc/memcpy/memset/strlen), hooks reads/writes + unmapped, calls
  0x43cb30. Correct pattern; just needs a valid `this`.
- `/tmp/mk_emu.py` — calls makeKey 0x43cd9e, hooks MD5 0x43c460 to dump hashed
  bytes (this is the money hook — wire it into approach #2).
- CRITICAL gotcha fixed: unicorn register consts are **`UC_X86_REG_*`** not
  `UC_REG_*`; the wrong name silently NameErrors *inside hook callbacks*, which
  looked like crypto faults in earlier sessions.
