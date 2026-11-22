//! Fixed-width integers at a byte offset, bounds-checked.
//!
//! Every filesystem crate reads on-disk fields as little- or big-endian
//! integers, and each used to carry its own `le32` / `be16` helper. These are
//! the shared ones. They return `None` (reads) or `false` (writes) when the
//! field runs past the buffer, so an offset taken from a corrupt image becomes
//! an error the caller reports instead of a panic.

macro_rules! endian_rw {
    ($($get:ident, $put:ident, $t:ty, $from:ident, $to:ident;)*) => {
        $(
            #[doc = concat!("The `", stringify!($t), "` at `off`, or `None` past the end of `b`.")]
            #[inline]
            pub fn $get(b: &[u8], off: usize) -> Option<$t> {
                const N: usize = std::mem::size_of::<$t>();
                let end = off.checked_add(N)?;
                let mut raw = [0u8; N];
                raw.copy_from_slice(b.get(off..end)?);
                Some(<$t>::$from(raw))
            }

            #[doc = concat!("Store `v` as a `", stringify!($t), "` at `off`; `false` (and `b` untouched) past the end.")]
            #[inline]
            pub fn $put(b: &mut [u8], off: usize, v: $t) -> bool {
                const N: usize = std::mem::size_of::<$t>();
                match off.checked_add(N).and_then(|end| b.get_mut(off..end)) {
                    Some(dst) => {
                        dst.copy_from_slice(&v.$to());
                        true
                    }
                    None => false,
                }
            }
        )*
    };
}

endian_rw! {
    le16, put_le16, u16, from_le_bytes, to_le_bytes;
    le32, put_le32, u32, from_le_bytes, to_le_bytes;
    le64, put_le64, u64, from_le_bytes, to_le_bytes;
    be16, put_be16, u16, from_be_bytes, to_be_bytes;
    be32, put_be32, u32, from_be_bytes, to_be_bytes;
    be64, put_be64, u64, from_be_bytes, to_be_bytes;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_both_byte_orders() {
        let b = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09];
        assert_eq!(le16(&b, 0), Some(0x0201));
        assert_eq!(be16(&b, 0), Some(0x0102));
        assert_eq!(le32(&b, 1), Some(0x0504_0302));
        assert_eq!(be32(&b, 1), Some(0x0203_0405));
        assert_eq!(le64(&b, 1), Some(0x0908_0706_0504_0302));
        assert_eq!(be64(&b, 0), Some(0x0102_0304_0506_0708));
    }

    #[test]
    fn out_of_range_is_none_not_a_panic() {
        let b = [0u8; 4];
        assert_eq!(le32(&b, 0), Some(0));
        assert_eq!(le32(&b, 1), None);
        assert_eq!(be16(&b, 3), None);
        assert_eq!(le64(&b, usize::MAX), None, "offset overflow");
    }

    #[test]
    fn writes_round_trip_and_refuse_short_buffers() {
        let mut b = [0u8; 8];
        assert!(put_le32(&mut b, 2, 0xDEAD_BEEF));
        assert_eq!(le32(&b, 2), Some(0xDEAD_BEEF));
        assert!(put_be16(&mut b, 6, 0x1234));
        assert_eq!(b[6..], [0x12, 0x34]);
        assert!(!put_be64(&mut b, 1, 1));
        assert_eq!(
            le32(&b, 2),
            Some(0xDEAD_BEEF),
            "a refused write changes nothing"
        );
    }
}
