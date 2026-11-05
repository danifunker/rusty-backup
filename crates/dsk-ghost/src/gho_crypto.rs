//! Norton Ghost (`.GHO`/`.GHS`) password encryption.
//!
//! Reverse-engineered from `ghostexp.exe` (Ghost Explorer 11.5) and verified
//! bit-exact against real password-protected fixtures (password `password`).
//! See `docs/gho_password.md` for the full derivation.
//!
//! ## Scheme
//!
//! Only the **bodies** of the inner record stream are encrypted; the 10-byte
//! record headers and the container header stay in clear text. Each body is
//! enciphered independently with the cipher **state reset to the password
//! seed** at the start of that body.
//!
//! 1. **Password -> 16-bit seed** ([`gho_seed_from_password`]): the password is
//!    ASCII-uppercased and truncated to 11 bytes, run through a *forward*
//!    CRC-32 (poly `0x04C11DB7`, init 0, no reflection/xorout), then folded
//!    `seed = (crc ^ (crc >> 16)) & 0xFFFF`.
//! 2. **Verifier** ([`gho_verify_seed`]): the 16-byte verifier at container
//!    offset `0x0C` decrypts (with the seed) to `b"BinaryResearch\0"` in its
//!    first 15 bytes when the password is correct — a fast wrong-password check.
//! 3. **Body cipher** ([`gho_decrypt_body`]): a self-synchronising CRC-16-CCITT
//!    (poly `0x1021`) keystream cipher whose 256-entry table is pre-seeded with
//!    `0x07A2`. For each byte: `plain = cipher ^ (state & 0xFF)` then
//!    `state = ((state & 0xFF) << 8) ^ TABLE[(state >> 8) ^ cipher]`.

/// Plaintext the verifier decrypts to (first 15 bytes) for a correct password.
pub const GHO_VERIFIER_PLAINTEXT: &[u8; 15] = b"BinaryResearch\0";

/// Build one entry of the 16-bit body-cipher table.
///
/// Faithful port of `ghostexp.exe sub_4dc710(i, poly=0x1021, init=0x07A2)`:
/// a CRC-16-CCITT step run over the byte `i` with the register pre-loaded to
/// `0x07A2` and the index interleaved as it shifts. `u16` shifts drop the
/// high bits, matching the binary's 16-bit masking.
const fn gen_table_entry(i: u16) -> u16 {
    const POLY: u16 = 0x1021;
    let mut eax: u16 = 0x07A2;
    let mut ecx: u16 = i << 8;
    let mut r = 0;
    while r < 8 {
        if r != 0 {
            ecx <<= 1;
        }
        let t = ecx ^ eax;
        eax <<= 1;
        if t & 0x8000 != 0 {
            eax ^= POLY;
        }
        r += 1;
    }
    eax
}

const fn build_body_table() -> [u16; 256] {
    let mut t = [0u16; 256];
    let mut i = 0usize;
    while i < 256 {
        t[i] = gen_table_entry(i as u16);
        i += 1;
    }
    t
}

/// 256-entry body-cipher table (CRC-16-CCITT, pre-seeded `0x07A2`).
pub const GHO_BODY_TABLE: [u16; 256] = build_body_table();

const fn build_crc32_fwd_table() -> [u32; 256] {
    const POLY: u32 = 0x04C1_1DB7;
    let mut t = [0u32; 256];
    let mut i = 0u32;
    while i < 256 {
        let mut c = i << 24;
        let mut k = 0;
        while k < 8 {
            if c & 0x8000_0000 != 0 {
                c = (c << 1) ^ POLY;
            } else {
                c <<= 1;
            }
            k += 1;
        }
        t[i as usize] = c;
        i += 1;
    }
    t
}

/// Forward (non-reflected) CRC-32 table, poly `0x04C11DB7`, used by the KDF.
const GHO_CRC32_FWD: [u32; 256] = build_crc32_fwd_table();

/// Maximum number of password bytes Ghost feeds into the seed KDF.
pub const GHO_PASSWORD_MAX_LEN: usize = 11;

#[inline]
const fn ascii_upper(b: u8) -> u8 {
    if b >= b'a' && b <= b'z' {
        b - 32
    } else {
        b
    }
}

/// Derive the 16-bit cipher seed from a password.
///
/// Mirrors `ghostexp.exe sub_4dc860`: ASCII-uppercase the password, keep at
/// most [`GHO_PASSWORD_MAX_LEN`] bytes, forward-CRC-32 them, and fold the
/// 32-bit result into 16 bits. Ghost passwords are therefore **case-insensitive**
/// and only the first 11 characters are significant.
pub fn gho_seed_from_password(password: &[u8]) -> u16 {
    let mut crc: u32 = 0;
    for &b in password.iter().take(GHO_PASSWORD_MAX_LEN) {
        let b = ascii_upper(b);
        crc = (crc << 8) ^ GHO_CRC32_FWD[(((crc >> 24) as u8) ^ b) as usize];
    }
    ((crc ^ (crc >> 16)) & 0xFFFF) as u16
}

/// Decrypt one record body in place, resetting the cipher state to `seed`.
///
/// Each call is independent — the GHO format resets the cipher at the start of
/// every body, so callers must pass exactly one body's bytes per call.
pub fn gho_decrypt_body(buf: &mut [u8], seed: u16) {
    let mut state = seed;
    for byte in buf.iter_mut() {
        let c = *byte;
        *byte = c ^ (state & 0xFF) as u8;
        state = ((state & 0xFF) << 8) ^ GHO_BODY_TABLE[(((state >> 8) as u8) ^ c) as usize];
    }
}

/// Encrypt one record body in place (inverse of [`gho_decrypt_body`]).
///
/// Provided for round-trip testing and any future write path; the feedback
/// uses the ciphertext byte, identical to Ghost's `sub_4dc8d0`.
pub fn gho_encrypt_body(buf: &mut [u8], seed: u16) {
    let mut state = seed;
    for byte in buf.iter_mut() {
        let c = *byte ^ (state & 0xFF) as u8;
        *byte = c;
        state = ((state & 0xFF) << 8) ^ GHO_BODY_TABLE[(((state >> 8) as u8) ^ c) as usize];
    }
}

/// Check a candidate seed against the container's 16-byte password verifier.
///
/// Returns `true` when the verifier decrypts to [`GHO_VERIFIER_PLAINTEXT`] in
/// its first 15 bytes (byte 16 is framing and is not checked).
pub fn gho_verify_seed(verifier: &[u8; 16], seed: u16) -> bool {
    let mut buf = *verifier;
    gho_decrypt_body(&mut buf, seed);
    &buf[..15] == GHO_VERIFIER_PLAINTEXT.as_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Constants captured from ghostexp.exe / the real `GH11PW.GHO` fixture
    // (password "password").
    const PW_SEED: u16 = 0x9BE1;
    const PW_VERIFIER: [u8; 16] = [
        0xa3, 0xd5, 0x5d, 0x38, 0x00, 0x4e, 0x2e, 0xa0, 0xb4, 0x8f, 0x9c, 0x7a, 0xea, 0x66, 0xb8,
        0xb1,
    ];

    #[test]
    fn body_table_matches_ghostexp() {
        // First 8 entries dumped from the reverse-engineered generator.
        let expect = [
            0xd2e7, 0xc2c6, 0xf2a5, 0xe284, 0x9263, 0x8242, 0xb221, 0xa200,
        ];
        assert_eq!(&GHO_BODY_TABLE[..8], &expect);
    }

    #[test]
    fn kdf_seed_matches_fixture() {
        assert_eq!(gho_seed_from_password(b"password"), PW_SEED);
    }

    #[test]
    fn kdf_is_case_insensitive() {
        assert_eq!(gho_seed_from_password(b"PASSWORD"), PW_SEED);
        assert_eq!(gho_seed_from_password(b"PassWord"), PW_SEED);
    }

    #[test]
    fn kdf_truncates_at_11_bytes() {
        // Bytes past the 11th must not affect the seed.
        let a = gho_seed_from_password(b"ABCDEFGHIJK");
        let b = gho_seed_from_password(b"ABCDEFGHIJKLMNOP");
        assert_eq!(a, b);
    }

    #[test]
    fn verifier_decrypts_with_correct_seed() {
        assert!(gho_verify_seed(&PW_VERIFIER, PW_SEED));
        let mut v = PW_VERIFIER;
        gho_decrypt_body(&mut v, PW_SEED);
        assert_eq!(&v[..15], GHO_VERIFIER_PLAINTEXT.as_slice());
    }

    #[test]
    fn verifier_rejects_wrong_seed() {
        assert!(!gho_verify_seed(&PW_VERIFIER, PW_SEED ^ 0x0001));
        assert!(!gho_verify_seed(
            &PW_VERIFIER,
            gho_seed_from_password(b"wrong")
        ));
    }

    #[test]
    fn first_keystream_byte_is_low_seed() {
        // Decrypting a zero buffer yields the keystream; byte 0 == seed & 0xFF.
        let mut buf = [0u8; 4];
        gho_decrypt_body(&mut buf, PW_SEED);
        assert_eq!(buf[0], (PW_SEED & 0xFF) as u8);
    }

    #[test]
    fn encrypt_decrypt_round_trips() {
        let original: Vec<u8> = (0..1000u32)
            .map(|i| (i.wrapping_mul(37) ^ 0x5A) as u8)
            .collect();
        let mut buf = original.clone();
        gho_encrypt_body(&mut buf, PW_SEED);
        assert_ne!(buf, original, "encryption must change the data");
        gho_decrypt_body(&mut buf, PW_SEED);
        assert_eq!(buf, original, "decrypt(encrypt(x)) must equal x");
    }
}
