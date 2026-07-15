//! Crypto primitives for APFS FileVault decryption (read-only).
//!
//! All three pieces the keybag→VEK→data unlock chain needs, hand-rolled on the
//! `aes` (0.9) and `sha2` crates already in the dependency tree — no new deps:
//!
//! - **AES-XTS** decrypt/encrypt over 512-byte data units ([`xts_decrypt`]) —
//!   used for keybag blocks (key = `UUID‖UUID`) and volume data (key = VEK).
//! - **RFC 3394 AES key unwrap** ([`aes256_key_unwrap`]) — unwraps the KEK from
//!   a passphrase-derived key, then the VEK from the KEK.
//! - **PBKDF2-HMAC-SHA256** ([`pbkdf2_hmac_sha256`]) — derives the KEK-unwrap
//!   key from the passphrase using the salt + iteration count in the keybag.
//!
//! The exact parameters (512-byte XTS units, `UUID‖UUID` keys, the DER KEK/VEK
//! blob layout) were confirmed against a real macOS-encrypted fixture during the
//! Phase 6 bring-up spike — see `tests/fixtures/apfs/ENCRYPTED_FIXTURE.md`.

use aes::cipher::{BlockCipherDecrypt, BlockCipherEncrypt, KeyInit};
use aes::{Aes128, Aes256};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// AES single-block helpers (ECB core, used by XTS and RFC 3394)
// ---------------------------------------------------------------------------

fn aes128_encrypt(cipher: &Aes128, block: &mut [u8; 16]) {
    let mut b = aes::Block::from(*block);
    cipher.encrypt_block(&mut b);
    block.copy_from_slice(&b[..]);
}

fn aes128_decrypt(cipher: &Aes128, block: &mut [u8; 16]) {
    let mut b = aes::Block::from(*block);
    cipher.decrypt_block(&mut b);
    block.copy_from_slice(&b[..]);
}

fn aes256_decrypt(cipher: &Aes256, block: &mut [u8; 16]) {
    let mut b = aes::Block::from(*block);
    cipher.decrypt_block(&mut b);
    block.copy_from_slice(&b[..]);
}

// ---------------------------------------------------------------------------
// AES-XTS (decrypt + encrypt) over fixed-size data units
// ---------------------------------------------------------------------------

/// Multiply the XTS tweak by the primitive element α in GF(2^128), little-endian
/// byte order (the AES-XTS convention): shift left one bit, and on carry-out of
/// the top bit fold in the reducing polynomial `x^128 + x^7 + x^2 + x + 1`.
fn xts_mul_alpha(t: &mut [u8; 16]) {
    let mut carry = 0u8;
    for byte in t.iter_mut() {
        let next = *byte >> 7;
        *byte = (*byte << 1) | carry;
        carry = next;
    }
    if carry != 0 {
        t[0] ^= 0x87;
    }
}

/// AES-XTS transform `buf` in place, treating it as consecutive `unit_size`-byte
/// data units whose XTS "sector number" starts at `first_sector` and increments
/// by one per unit. `key1` ciphers the data; `key2` ciphers the tweak. When
/// `decrypt` is true the data cipher runs in decrypt mode (the read path); the
/// tweak is always derived by *encryption*.
///
/// `buf.len()` must be a whole number of `unit_size` units and `unit_size` a
/// multiple of 16; anything else leaves the trailing bytes untouched (APFS data
/// units are always 512 bytes, so this never trips in practice).
fn xts_transform(
    key1: &[u8; 16],
    key2: &[u8; 16],
    first_sector: u128,
    unit_size: usize,
    buf: &mut [u8],
    decrypt: bool,
) {
    let c1 = Aes128::new_from_slice(key1).expect("16-byte key");
    let c2 = Aes128::new_from_slice(key2).expect("16-byte key");
    for (u, unit) in buf.chunks_mut(unit_size).enumerate() {
        if unit.len() != unit_size || !unit_size.is_multiple_of(16) {
            break;
        }
        // Tweak for this unit = AES-encrypt(key2, sector_number_le).
        let mut tweak = (first_sector + u as u128).to_le_bytes();
        aes128_encrypt(&c2, &mut tweak);
        for block in unit.chunks_mut(16) {
            let mut b = [0u8; 16];
            b.copy_from_slice(block);
            for i in 0..16 {
                b[i] ^= tweak[i];
            }
            if decrypt {
                aes128_decrypt(&c1, &mut b);
            } else {
                aes128_encrypt(&c1, &mut b);
            }
            for i in 0..16 {
                b[i] ^= tweak[i];
            }
            block.copy_from_slice(&b);
            xts_mul_alpha(&mut tweak);
        }
    }
}

/// AES-XTS-decrypt `buf` in place. `key` is the 32-byte XTS key (`key1‖key2`);
/// data units are `unit_size` bytes with sector numbers starting at
/// `first_sector`. See [`xts_transform`].
pub fn xts_decrypt(key: &[u8; 32], first_sector: u128, unit_size: usize, buf: &mut [u8]) {
    let (k1, k2) = key.split_at(16);
    xts_transform(
        k1.try_into().unwrap(),
        k2.try_into().unwrap(),
        first_sector,
        unit_size,
        buf,
        true,
    );
}

/// AES-XTS-encrypt `buf` in place — the inverse of [`xts_decrypt`]. Not used by
/// the read path; kept for round-trip testing (and future write support).
#[cfg(test)]
pub fn xts_encrypt(key: &[u8; 32], first_sector: u128, unit_size: usize, buf: &mut [u8]) {
    let (k1, k2) = key.split_at(16);
    xts_transform(
        k1.try_into().unwrap(),
        k2.try_into().unwrap(),
        first_sector,
        unit_size,
        buf,
        false,
    );
}

// ---------------------------------------------------------------------------
// RFC 3394 AES key unwrap (AES-256 KEK)
// ---------------------------------------------------------------------------

/// The RFC 3394 default integrity check value (IV / initial value `A`).
const KW_IV: [u8; 8] = [0xA6; 8];

/// RFC 3394 AES key *unwrap* with a 256-bit KEK. `wrapped` is `8*(n+1)` bytes
/// (n 64-bit key blocks plus the 64-bit integrity value). Returns the `8*n`
/// plaintext key bytes, or `None` if the integrity check fails (wrong KEK /
/// corrupt data).
pub fn aes256_key_unwrap(kek: &[u8; 32], wrapped: &[u8]) -> Option<Vec<u8>> {
    if wrapped.len() < 24 || !wrapped.len().is_multiple_of(8) {
        return None;
    }
    let n = wrapped.len() / 8 - 1;
    let cipher = Aes256::new_from_slice(kek).ok()?;

    let mut a = [0u8; 8];
    a.copy_from_slice(&wrapped[0..8]);
    let mut r: Vec<[u8; 8]> = (1..=n)
        .map(|i| {
            let mut b = [0u8; 8];
            b.copy_from_slice(&wrapped[i * 8..i * 8 + 8]);
            b
        })
        .collect();

    for j in (0..6).rev() {
        for i in (1..=n).rev() {
            let t = (n * j + i) as u64;
            let tb = t.to_be_bytes();
            let mut block = [0u8; 16];
            for k in 0..8 {
                block[k] = a[k] ^ tb[k];
            }
            block[8..16].copy_from_slice(&r[i - 1]);
            aes256_decrypt(&cipher, &mut block);
            a.copy_from_slice(&block[0..8]);
            r[i - 1].copy_from_slice(&block[8..16]);
        }
    }

    if a != KW_IV {
        return None;
    }
    let mut out = Vec::with_capacity(n * 8);
    for block in r {
        out.extend_from_slice(&block);
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// PBKDF2-HMAC-SHA256
// ---------------------------------------------------------------------------

const SHA256_BLOCK: usize = 64;

/// HMAC-SHA256 of `msg` under `key` (RFC 2104).
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; SHA256_BLOCK];
    if key.len() > SHA256_BLOCK {
        let d = Sha256::digest(key);
        k[..32].copy_from_slice(&d);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; SHA256_BLOCK];
    let mut opad = [0x5cu8; SHA256_BLOCK];
    for i in 0..SHA256_BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(msg);
    let ih = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(ih);
    outer.finalize().into()
}

/// PBKDF2-HMAC-SHA256 (RFC 8018) producing a `dk_len`-byte derived key.
pub fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32, dk_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(dk_len);
    let mut block_index: u32 = 1;
    while out.len() < dk_len {
        // U1 = HMAC(pw, salt || INT_BE(block_index))
        let mut msg = Vec::with_capacity(salt.len() + 4);
        msg.extend_from_slice(salt);
        msg.extend_from_slice(&block_index.to_be_bytes());
        let mut u = hmac_sha256(password, &msg);
        let mut t = u;
        for _ in 1..iterations {
            u = hmac_sha256(password, &u);
            for i in 0..32 {
                t[i] ^= u[i];
            }
        }
        out.extend_from_slice(&t);
        block_index += 1;
    }
    out.truncate(dk_len);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hx(s: &str) -> Vec<u8> {
        (0..s.len() / 2)
            .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn hmac_sha256_rfc4231_case1() {
        // RFC 4231 test case 1.
        let key = [0x0bu8; 20];
        let mac = hmac_sha256(&key, b"Hi There");
        assert_eq!(
            mac.to_vec(),
            hx("b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7")
        );
    }

    #[test]
    fn pbkdf2_hmac_sha256_known_vectors() {
        // password="password", salt="salt".
        assert_eq!(
            pbkdf2_hmac_sha256(b"password", b"salt", 1, 32),
            hx("120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b")
        );
        assert_eq!(
            pbkdf2_hmac_sha256(b"password", b"salt", 2, 32),
            hx("ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43")
        );
    }

    #[test]
    fn aes256_key_unwrap_rfc3394_section_4_6() {
        // RFC 3394 §4.6: 256-bit KEK wrapping a 256-bit key.
        let kek: [u8; 32] = hx("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
            .try_into()
            .unwrap();
        let wrapped = hx(
            "28c9f404c4b810f4cbccb35cfb87f8263f5786e2d80ed326cbc7f0e71a99f43b\
             fb988b9b7a02dd21",
        );
        let key = aes256_key_unwrap(&kek, &wrapped).expect("unwrap ok");
        assert_eq!(
            key,
            hx("00112233445566778899aabbccddeeff000102030405060708090a0b0c0d0e0f")
        );
        // A wrong KEK must fail the integrity check.
        let mut bad = kek;
        bad[0] ^= 0xff;
        assert!(aes256_key_unwrap(&bad, &wrapped).is_none());
    }

    #[test]
    fn xts_round_trips() {
        let key = [7u8; 32];
        let orig: Vec<u8> = (0..512).map(|i| (i * 3) as u8).collect();
        let mut buf = orig.clone();
        xts_encrypt(&key, 42, 512, &mut buf);
        assert_ne!(buf, orig, "encryption must change the data");
        xts_decrypt(&key, 42, 512, &mut buf);
        assert_eq!(buf, orig, "decrypt(encrypt(x)) == x");
    }

    #[test]
    fn xts_decrypt_is_position_dependent() {
        // Same ciphertext at different sector numbers must decrypt differently
        // (guards against a dropped/constant tweak).
        let key = [9u8; 32];
        let ct = vec![0x5au8; 512];
        let mut a = ct.clone();
        let mut b = ct.clone();
        xts_decrypt(&key, 0, 512, &mut a);
        xts_decrypt(&key, 1, 512, &mut b);
        assert_ne!(a, b);
    }
}
