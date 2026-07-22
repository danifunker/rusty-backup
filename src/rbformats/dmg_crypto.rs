//! Apple **encrypted disk image** reader (`encrcdsa` v2 — "FileVault 1"
//! per-image encryption, Mac OS X 10.5+ and still produced by modern
//! `hdiutil create -encryption`).
//!
//! An encrypted DMG wraps the real disk image (a raw volume, or occasionally a
//! nested UDIF) in an AES-CBC stream keyed from the user's passphrase. This is
//! distinct from FileVault 2 (whole-volume, 10.7+) which is not a disk-image
//! container. The container has its own `encrcdsa` header at offset 0 — the
//! `koly` UDIF footer only becomes visible after decryption.
//!
//! ## Format (`encrcdsa`, version 2, all big-endian)
//!
//! ```text
//! 0x00  8   signature "encrcdsa"
//! 0x08  4   version (= 2)
//! 0x0C  4   blockIvLen (16)
//! 0x10  4   blockMode (5 = CBC)
//! 0x14  4   blockAlgorithm (0x80000001 = AES)
//! 0x18  4   keyBits (128 or 256)     -- the VOLUME AES key size
//! 0x34  4   bytesPerBlock (512)
//! 0x38  8   dataLen                  -- decrypted image size
//! 0x40  8   dataOffset               -- file offset of the first ciphertext block
//! 0x48  4   keyCount
//! 0x4C  20*n key-item descriptors: itemType(4) u64 offset(8) u64 size(8)
//! ```
//!
//! A passphrase key-item (itemType 1) points to a wrapped-key blob:
//!
//! ```text
//! +0x00 4   kdfAlgorithm (103 = PBKDF2)
//! +0x08 4   kdfIterationCount
//! +0x0C 4   kdfSaltLen
//! +0x10 32  kdfSalt (first kdfSaltLen bytes)
//! +0x30 4   blobEncIvLen (8)
//! +0x34 32  blobEncIv (first blobEncIvLen bytes)
//! +0x54 4   blobEncKeyBits (192)     -- the KEY-WRAP cipher key size
//! +0x58 4   blobEncAlgorithm (0x80000001 AES on modern macOS; 0x11 = 3DES on
//!            older/vintage images)
//! +0x64 4   encryptedKeyblobLen
//! +0x68 ..  encryptedKeyblob
//! ```
//!
//! ## Decryption
//!
//! 1. `deskey = PBKDF2-HMAC-SHA1(passphrase, salt, iterations, 32)[..blobEncKeyBits/8]`
//! 2. unwrap `encryptedKeyblob` with the key-wrap cipher in CBC mode, IV =
//!    `blobEncIv` zero-padded to the cipher block size.
//! 3. strip PKCS#7 padding; the plaintext ends with `b"CKIE\0"` (wrong
//!    passphrase => bad padding or missing magic).
//! 4. `keydata = plaintext[..-5]`; `aeskey = keydata[..keyBits/8]`,
//!    `hmackey = keydata[keyBits/8..]` (20-byte HMAC-SHA1 key).
//! 5. block `N` (512 B) at `dataOffset + N*512`: `iv = HMAC-SHA1(hmackey,
//!    be32(N))[..blockIvLen]`, plaintext = AES-CBC-decrypt(aeskey, iv, ct).
//!
//! Verified end-to-end against `hdiutil`-produced AES-128 and AES-256 images.
//! The 3DES key-wrap path (older images) shares the same flow with a different
//! unwrap cipher; it is wired but has not been verified against a real vintage
//! image (modern `hdiutil` wraps with AES-192 even for AES-128 volumes).

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use std::io::{self, Read, Seek, SeekFrom};

use aes::{Aes128, Aes192, Aes256};
use anyhow::{anyhow, bail, Result};
use cbc::cipher::{BlockModeDecrypt, KeyIvInit};
use des::TdesEde3;
use sha1::{Digest, Sha1};

/// v2 signature at offset 0.
const ENCRCDSA_MAGIC: &[u8; 8] = b"encrcdsa";
/// v1 signature (older "cdsaencr" trailer format) — detected, not yet decoded.
const CDSAENCR_MAGIC: &[u8; 8] = b"cdsaencr";

/// CSSM algorithm id for AES.
const ALG_AES: u32 = 0x8000_0001;
/// CSSM algorithm id for 3DES-EDE.
const ALG_3DES: u32 = 0x0000_0011;
/// Trailer magic on the unwrapped key blob.
const CKIE_SUFFIX: &[u8] = b"CKIE\0";

/// Kind of encrypted-DMG header seen at offset 0, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptedDmgKind {
    /// `encrcdsa` version 2 — decodable with a passphrase.
    V2,
    /// `cdsaencr` version 1 — recognized but not yet decodable.
    V1,
}

/// Peek `head` (>= 8 bytes) for an encrypted-DMG signature.
pub fn detect_encrypted_dmg(head: &[u8]) -> Option<EncryptedDmgKind> {
    if head.len() < 8 {
        return None;
    }
    if &head[0..8] == ENCRCDSA_MAGIC {
        Some(EncryptedDmgKind::V2)
    } else if &head[0..8] == CDSAENCR_MAGIC {
        Some(EncryptedDmgKind::V1)
    } else {
        None
    }
}

fn be32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes(b[o..o + 4].try_into().unwrap())
}
fn be64(b: &[u8], o: usize) -> u64 {
    u64::from_be_bytes(b[o..o + 8].try_into().unwrap())
}

fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(data);
    h.finalize().into()
}

/// HMAC-SHA1 (block size 64). Hand-rolled to avoid a new `hmac` dependency,
/// matching how [`crate::fs::apfs_crypto`] hand-rolls HMAC-SHA256.
fn hmac_sha1(key: &[u8], msg: &[u8]) -> [u8; 20] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..20].copy_from_slice(&sha1(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha1::new();
    inner.update(ipad);
    inner.update(msg);
    let inner = inner.finalize();
    let mut outer = Sha1::new();
    outer.update(opad);
    outer.update(inner);
    outer.finalize().into()
}

/// PBKDF2-HMAC-SHA1.
fn pbkdf2_hmac_sha1(password: &[u8], salt: &[u8], iterations: u32, dk_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(dk_len);
    let mut block_index: u32 = 1;
    while out.len() < dk_len {
        let mut msg = salt.to_vec();
        msg.extend_from_slice(&block_index.to_be_bytes());
        let mut u = hmac_sha1(password, &msg);
        let mut t = u;
        for _ in 1..iterations {
            u = hmac_sha1(password, &u);
            for i in 0..20 {
                t[i] ^= u[i];
            }
        }
        out.extend_from_slice(&t);
        block_index += 1;
    }
    out.truncate(dk_len);
    out
}

/// In-place CBC decrypt of a whole-block-aligned buffer, dispatching on the
/// key length (AES-128/192/256) or a 24-byte 3DES key. `alg` selects the
/// cipher family. The IV is zero-padded/truncated to the cipher block size.
fn cbc_decrypt(alg: u32, key: &[u8], iv: &[u8], buf: &mut [u8]) -> Result<()> {
    match alg {
        ALG_AES => {
            let mut iv16 = [0u8; 16];
            let n = iv.len().min(16);
            iv16[..n].copy_from_slice(&iv[..n]);
            if !buf.len().is_multiple_of(16) {
                bail!("AES-CBC input not a multiple of 16 bytes");
            }
            let blocks: &mut [aes::Block] = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut aes::Block, buf.len() / 16)
            };
            match key.len() {
                16 => cbc::Decryptor::<Aes128>::new_from_slices(key, &iv16)
                    .map_err(|_| anyhow!("bad AES-128 key/iv"))?
                    .decrypt_blocks(blocks),
                24 => cbc::Decryptor::<Aes192>::new_from_slices(key, &iv16)
                    .map_err(|_| anyhow!("bad AES-192 key/iv"))?
                    .decrypt_blocks(blocks),
                32 => cbc::Decryptor::<Aes256>::new_from_slices(key, &iv16)
                    .map_err(|_| anyhow!("bad AES-256 key/iv"))?
                    .decrypt_blocks(blocks),
                other => bail!("unsupported AES key length {other}"),
            }
            Ok(())
        }
        ALG_3DES => {
            if key.len() != 24 {
                bail!("3DES requires a 24-byte key, got {}", key.len());
            }
            let mut iv8 = [0u8; 8];
            let n = iv.len().min(8);
            iv8[..n].copy_from_slice(&iv[..n]);
            if !buf.len().is_multiple_of(8) {
                bail!("3DES-CBC input not a multiple of 8 bytes");
            }
            let blocks: &mut [des::cipher::Block<TdesEde3>] = unsafe {
                std::slice::from_raw_parts_mut(
                    buf.as_mut_ptr() as *mut des::cipher::Block<TdesEde3>,
                    buf.len() / 8,
                )
            };
            cbc::Decryptor::<TdesEde3>::new_from_slices(key, &iv8)
                .map_err(|_| anyhow!("bad 3DES key/iv"))?
                .decrypt_blocks(blocks);
            Ok(())
        }
        other => bail!("unsupported key-wrap algorithm 0x{other:08X}"),
    }
}

/// Parsed + unwrapped encryption context: everything needed to decrypt blocks.
struct CryptoContext {
    aeskey: Vec<u8>,
    hmackey: Vec<u8>,
    block_iv_len: usize,
    block_size: u64,
    data_offset: u64,
    data_len: u64,
}

impl CryptoContext {
    /// Parse the `encrcdsa` v2 header + passphrase blob from `header` bytes and
    /// unwrap the volume key with `password`.
    fn open(header: &[u8], password: &[u8]) -> Result<Self> {
        if header.len() < 0x50 || &header[0..8] != ENCRCDSA_MAGIC {
            bail!("not an encrcdsa image");
        }
        let version = be32(header, 8);
        if version != 2 {
            bail!("unsupported encrcdsa version {version}");
        }
        let block_iv_len = be32(header, 0x0C) as usize;
        let key_bits = be32(header, 0x18) as usize;
        let block_size = be32(header, 0x34) as u64;
        let data_len = be64(header, 0x38);
        let data_offset = be64(header, 0x40);
        let key_count = be32(header, 0x48);
        if block_size == 0 || !block_size.is_multiple_of(16) {
            bail!("invalid encrcdsa block size {block_size}");
        }

        // Walk the key-item descriptors for a passphrase-direct entry (type 1).
        let mut blob_off = None;
        for i in 0..key_count as usize {
            let d = 0x4C + i * 20;
            if d + 20 > header.len() {
                break;
            }
            let item_type = be32(header, d);
            let off = be64(header, d + 4) as usize;
            if item_type == 1 {
                blob_off = Some(off);
                break;
            }
        }
        let b = blob_off.ok_or_else(|| {
            anyhow!("encrypted image has no passphrase key (certificate/keybag-only unlock)")
        })?;
        if b + 0x68 > header.len() {
            bail!("encrcdsa passphrase blob is truncated");
        }

        let iterations = be32(header, b + 0x08);
        let salt_len = be32(header, b + 0x0C) as usize;
        let salt = &header[b + 0x10..b + 0x10 + salt_len.min(32)];
        let blob_iv_len = be32(header, b + 0x30) as usize;
        let blob_iv = &header[b + 0x34..b + 0x34 + blob_iv_len.min(32)];
        let wrap_key_bits = be32(header, b + 0x54) as usize;
        let wrap_alg = be32(header, b + 0x58);
        let enc_blob_len = be32(header, b + 0x64) as usize;
        if b + 0x68 + enc_blob_len > header.len() {
            bail!("encrcdsa key blob length {enc_blob_len} exceeds header");
        }
        let mut blob = header[b + 0x68..b + 0x68 + enc_blob_len].to_vec();

        // Derive the wrap key and unwrap the volume key blob.
        let derived = pbkdf2_hmac_sha1(password, salt, iterations, 32);
        let wrap_key = &derived[..wrap_key_bits / 8];
        cbc_decrypt(wrap_alg, wrap_key, blob_iv, &mut blob)?;

        // Strip PKCS#7 padding; a wrong passphrase shows up here.
        let plain = strip_pkcs7(&blob).ok_or_else(bad_passphrase)?;
        if !plain.ends_with(CKIE_SUFFIX) {
            return Err(bad_passphrase());
        }
        let keydata = &plain[..plain.len() - CKIE_SUFFIX.len()];
        let key_bytes = key_bits / 8;
        if keydata.len() < key_bytes + 20 {
            bail!("unwrapped key material too short");
        }
        let aeskey = keydata[..key_bytes].to_vec();
        let hmackey = keydata[key_bytes..].to_vec();

        Ok(CryptoContext {
            aeskey,
            hmackey,
            block_iv_len: block_iv_len.min(20),
            block_size,
            data_offset,
            data_len,
        })
    }

    /// Decrypt a single ciphertext block `n` (in place).
    fn decrypt_block(&self, n: u64, buf: &mut [u8]) -> Result<()> {
        let iv = hmac_sha1(&self.hmackey, &(n as u32).to_be_bytes());
        cbc_decrypt(ALG_AES, &self.aeskey, &iv[..self.block_iv_len], buf)
    }
}

fn strip_pkcs7(data: &[u8]) -> Option<&[u8]> {
    let pad = *data.last()? as usize;
    if pad == 0 || pad > 16 || pad > data.len() {
        return None;
    }
    if data[data.len() - pad..].iter().all(|&b| b as usize == pad) {
        Some(&data[..data.len() - pad])
    } else {
        None
    }
}

fn bad_passphrase() -> anyhow::Error {
    anyhow!("incorrect passphrase for encrypted disk image (or unsupported key wrap)")
}

/// A `Read + Seek` view over a decrypted encrypted-DMG's plaintext disk.
pub struct EncryptedDmgReader<R: Read + Seek> {
    source: R,
    ctx: CryptoContext,
    position: u64,
    /// Cached decrypted block + its index.
    cache_block: Option<u64>,
    cache_data: Vec<u8>,
}

impl<R: Read + Seek> EncryptedDmgReader<R> {
    /// Total decrypted (plaintext) size in bytes.
    pub fn total_size(&self) -> u64 {
        self.ctx.data_len
    }

    fn ensure_block(&mut self, block: u64) -> io::Result<()> {
        if self.cache_block == Some(block) {
            return Ok(());
        }
        let off = self.ctx.data_offset + block * self.ctx.block_size;
        self.source.seek(SeekFrom::Start(off))?;
        let mut buf = vec![0u8; self.ctx.block_size as usize];
        self.source.read_exact(&mut buf)?;
        self.ctx
            .decrypt_block(block, &mut buf)
            .map_err(crate::compat::io_other)?;
        self.cache_block = Some(block);
        self.cache_data = buf;
        Ok(())
    }
}

/// Open an `encrcdsa` v2 encrypted image over `source` with `password`.
pub fn open_encrypted_dmg<R: Read + Seek>(
    mut source: R,
    password: &[u8],
) -> Result<EncryptedDmgReader<R>> {
    // The header + key blob live in the first data_offset bytes; read a bounded
    // prefix (blobs are small — a few hundred bytes past a 512 B header).
    let file_len = source.seek(SeekFrom::End(0))?;
    let head_len = file_len.min(4096) as usize;
    source.seek(SeekFrom::Start(0))?;
    let mut header = vec![0u8; head_len];
    source.read_exact(&mut header)?;
    match detect_encrypted_dmg(&header) {
        Some(EncryptedDmgKind::V2) => {}
        Some(EncryptedDmgKind::V1) => bail!(
            "cdsaencr (v1) encrypted disk images are not yet supported; \
             re-create as encrcdsa (modern hdiutil) or decrypt with vfdecrypt"
        ),
        None => bail!("not an encrypted disk image"),
    }
    let ctx = CryptoContext::open(&header, password)?;
    Ok(EncryptedDmgReader {
        source,
        ctx,
        position: 0,
        cache_block: None,
        cache_data: Vec::new(),
    })
}

impl<R: Read + Seek> Read for EncryptedDmgReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.ctx.data_len || buf.is_empty() {
            return Ok(0);
        }
        let block = self.position / self.ctx.block_size;
        let offset_in_block = (self.position % self.ctx.block_size) as usize;
        self.ensure_block(block)?;
        let avail_in_block = self.cache_data.len() - offset_in_block;
        let remaining_in_disk = (self.ctx.data_len - self.position) as usize;
        let n = buf.len().min(avail_in_block).min(remaining_in_disk);
        buf[..n].copy_from_slice(&self.cache_data[offset_in_block..offset_in_block + n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for EncryptedDmgReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::Current(d) => self.position as i64 + d,
            SeekFrom::End(d) => self.ctx.data_len as i64 + d,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        self.position = new_pos as u64;
        Ok(self.position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_magics() {
        assert_eq!(
            detect_encrypted_dmg(b"encrcdsa"),
            Some(EncryptedDmgKind::V2)
        );
        assert_eq!(
            detect_encrypted_dmg(b"cdsaencr"),
            Some(EncryptedDmgKind::V1)
        );
        assert_eq!(detect_encrypted_dmg(b"koly1234"), None);
        assert_eq!(detect_encrypted_dmg(b"abc"), None);
    }

    #[test]
    fn pbkdf2_sha1_known_vector() {
        // RFC 6070 PBKDF2-HMAC-SHA1: P="password", S="salt", c=1, dkLen=20.
        let dk = pbkdf2_hmac_sha1(b"password", b"salt", 1, 20);
        assert_eq!(
            dk,
            hex_literal(&[
                0x0c, 0x60, 0xc8, 0x0f, 0x96, 0x1f, 0x0e, 0x71, 0xf3, 0xa9, 0xb5, 0x24, 0xaf, 0x60,
                0x12, 0x06, 0x2f, 0xe0, 0x37, 0xa6
            ])
        );
    }

    #[test]
    fn pbkdf2_sha1_two_iterations() {
        // RFC 6070: c=2.
        let dk = pbkdf2_hmac_sha1(b"password", b"salt", 2, 20);
        assert_eq!(
            dk,
            hex_literal(&[
                0xea, 0x6c, 0x01, 0x4d, 0xc7, 0x2d, 0x6f, 0x8c, 0xcd, 0x1e, 0xd9, 0x2a, 0xce, 0x1d,
                0x41, 0xf0, 0xd8, 0xde, 0x89, 0x57
            ])
        );
    }

    #[test]
    fn hmac_sha1_rfc2202() {
        // RFC 2202 test case 2: key="Jefe", data="what do ya want for nothing?".
        let mac = hmac_sha1(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            mac,
            [
                0xef, 0xfc, 0xdf, 0x6a, 0xe5, 0xeb, 0x2f, 0xa2, 0xd2, 0x74, 0x16, 0xd5, 0xf1, 0x84,
                0xdf, 0x9c, 0x25, 0x9a, 0x7c, 0x79
            ]
        );
    }

    #[test]
    fn pkcs7_strip() {
        assert_eq!(
            strip_pkcs7(&[1, 2, 3, 0x05, 0x05, 0x05, 0x05, 0x05]),
            Some(&[1, 2, 3][..])
        );
        assert_eq!(strip_pkcs7(&[1, 2, 3, 0x05, 0x05, 0x05, 0x05, 0x04]), None); // bad pad
        assert_eq!(strip_pkcs7(&[0x00]), None);
    }

    fn hex_literal(b: &[u8]) -> Vec<u8> {
        b.to_vec()
    }
}
