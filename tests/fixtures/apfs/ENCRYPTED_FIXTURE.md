# Encrypted APFS fixture

`tests/fixtures/test_apfs_encrypted.img.zst` — a 24 MiB APFS container (raw
`Apple_APFS` partition bytes) whose single volume **RBBASE** is
**FileVault-encrypted** with a **password crypto user**.

- **Passphrase:** `rbtest-apfs` (throwaway test constant — the fixture holds
  only PBKDF2-wrapped keys, so the passphrase is not recoverable from the image)
- **Volume:** RBBASE (case-insensitive), one "Disk" crypto user
  (UUID == the volume UUID `0B19FD7B-F5B5-4C46-A459-EB12F40E0986`)
- **Container UUID:** `C5570DDD-E6C4-48D0-8E7A-217C8F43E0E7`
- **Plaintext oracle:** `oracle_encrypted_checksums.txt` (same tree shape as the
  unencrypted fixture; `big.bin` here is 200 KiB)

A personal recovery key is cryptographically the same passphrase→KEK path, so
this single-password fixture exercises the recovery-key code path too. (Real
Apple-format recovery keys are an `fdesetup` / boot-volume feature and can't be
minted on a data volume via `diskutil apfs`, which only exposes
`changePassphrase` there — no add-user.)

## Creating an encrypted fixture (macOS)

Parameterized recipe — set `PW` to any passphrase. **Do not commit a real
personal password**; use an obvious throwaway. `hdiutil -encryption` is the
*wrong* tool (it encrypts the whole `.dmg` file, leaving the APFS inside
plaintext) — real APFS/FileVault volume encryption comes from `diskutil apfs`.

```sh
PW='choose-a-throwaway'                 # the volume password
DMG=apfs_enc.dmg
OUT=apfs_enc_container.img              # raw encrypted container bytes

# 1. Plain APFS container in a disk image (a full disk is NOT required).
hdiutil create -size 24m -fs APFS -volname RBBASE -layout GPTSPUD -type UDIF "$DMG"

# 2. Attach and find the device nodes.
ATTACH=$(hdiutil attach "$DMG" -nobrowse)
WHOLE=$(echo "$ATTACH" | awk '/GUID_partition_scheme/{print $1; exit}')  # /dev/diskN
PART=$(echo  "$ATTACH" | awk '/Apple_APFS/{print $1; exit}')             # physical store
VOL=$(echo   "$ATTACH" | awk '/RBBASE/{print $1; exit}')                 # APFS volume

# 3. Encrypt the *volume* in place with a "Disk" (password) crypto user.
printf '%s' "$PW" | diskutil apfs encryptVolume "$VOL" -user disk -stdinpassphrase
#    Wait until: diskutil apfs list  shows  FileVault: Yes (Unlocked)
#    (background conversion; near-instant on a tiny volume)

# 4. Populate the still-mounted volume with the test tree, then record the
#    plaintext SHA-256 oracle.
V=/Volumes/RBBASE
echo "hello apfs world" > "$V/hello.txt"
printf 'ABCDEFGHIJ%.0s' {1..500} > "$V/medium.txt"
mkdir -p "$V/docs/nested"
echo "readme contents here" > "$V/docs/readme.md"
echo "deep file"            > "$V/docs/nested/deep.txt"
dd if=/dev/urandom of="$V/big.bin" bs=1024 count=200
ln -s hello.txt "$V/link_to_hello"
sync
find "$V" -type f | sort | while read f; do
  printf '%s  %s\n' "$(shasum -a 256 "$f" | awk '{print $1}')" "${f#$V/}"
done > oracle_encrypted_checksums.txt

# 5. Note the UUIDs (needed to sanity-check keybag decryption), then capture the
#    raw encrypted container partition and detach.
xxd -s 72 -l 16 -p <(dd if="$PART" bs=1m 2>/dev/null)   # container UUID (nx_uuid)
diskutil info "$VOL" | awk -F: '/Volume UUID/{print $2}' # volume UUID
diskutil unmount "$V"
dd if=$(echo "$PART" | sed 's#/dev/disk#/dev/rdisk#') of="$OUT" bs=1m
hdiutil detach "$WHOLE"

# 6. Compress into the fixture slot.
zstd -19 -f "$OUT" -o tests/fixtures/test_apfs_encrypted.img.zst
```

## Confirmed on-disk crypto (from the bring-up spike)

Keybag block decryption is **AES-128-XTS**, **512-byte data units**, tweak =
`block_addr * (block_size/512) + unit_index` (the 512-byte logical-sector
index), XTS key = `UUID || UUID` (the 16-byte UUID used for both XTS subkeys):

- **Container keybag** at the `nx_keylocker` prange (offset 1296 in the NXSB;
  e.g. block 168 here), key = container UUID. Decrypts to a `kb_locker`
  (`kl_version=2`), obj_type `0x6b657973` "keys". Entries (`keybag_entry_t`:
  uuid[16], tag u16, keylen u16, pad[4], keydata[keylen]; 16-byte aligned):
  - tag **2** `KB_TAG_VOLUME_KEY` — the wrapped VEK (124-byte DER blob).
  - tag **3** `KB_TAG_VOLUME_UNLOCK_RECORDS` — a `prange` to the volume keybag
    (e.g. block 166 here).
- **Volume keybag** at that prange, key = **volume** UUID. Entry tag **3** =
  the KEK blob (DER `30 81 91 …`) for the crypto user: PBKDF2 salt + iteration
  count + wrapped KEK.

Unlock chain: passphrase → PBKDF2 (salt/iters from the KEK blob) → RFC-3394
AES-key-unwrap the KEK → RFC-3394 unwrap the VEK (from the container keybag
tag-2 blob) → AES-XTS-decrypt volume file data (per-block tweak).
