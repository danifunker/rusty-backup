//! Pieces the AFFS, PFS3 and SFS checkers share: an issue constructor, capped issue
//! buckets, the observed-allocation bit set and its comparison with a stored free map.

use disk_core::fsck::FsckIssue;

/// An error-level issue under `code`.
pub(crate) fn issue(code: &str, message: impl Into<String>, repairable: bool) -> FsckIssue {
    FsckIssue {
        code: code.to_string(),
        message: message.into(),
        repairable,
        debug: false,
    }
}

/// Push up to 32 issues under `code`, then one line counting the rest.
#[cfg(any(feature = "pfs3", feature = "sfs"))]
pub(crate) fn push_bucket(errors: &mut Vec<FsckIssue>, code: &str, msgs: &[String]) {
    const CAP: usize = 32;
    for msg in msgs.iter().take(CAP) {
        errors.push(issue(code, msg.clone(), false));
    }
    if msgs.len() > CAP {
        errors.push(issue(
            code,
            format!("... and {} more", msgs.len() - CAP),
            false,
        ));
    }
}

/// A compact bit set (one bit per index, LSB-first within each byte); `set bit = allocated`.
#[cfg(any(feature = "pfs3", feature = "sfs"))]
pub(crate) struct BitSet {
    bits: Vec<u8>,
    len: u32,
}

#[cfg(any(feature = "pfs3", feature = "sfs"))]
impl BitSet {
    pub(crate) fn new(len: u32) -> Self {
        Self {
            bits: vec![0u8; (len as usize).div_ceil(8)],
            len,
        }
    }
    pub(crate) fn set(&mut self, idx: u32) {
        if idx < self.len {
            self.bits[(idx / 8) as usize] |= 1u8 << (idx % 8);
        }
    }
    pub(crate) fn get(&self, idx: u32) -> bool {
        idx < self.len && (self.bits[(idx / 8) as usize] >> (idx % 8)) & 1 == 1
    }
    pub(crate) fn count_set(&self) -> u32 {
        self.bits.iter().map(|b| b.count_ones()).sum()
    }
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bits
    }
}

/// (allocated but stored free, free but stored allocated) over the first `len` indexes of a
/// stored map whose set bits mean free.
#[cfg(any(feature = "pfs3", feature = "sfs"))]
pub(crate) fn diff_against_free(observed: &BitSet, stored_free: &[u8], len: u32) -> (u32, u32) {
    let mut missing = 0u32;
    let mut extra = 0u32;
    for idx in 0..len {
        let sto_free = stored_free
            .get((idx / 8) as usize)
            .map(|b| (b >> (idx % 8)) & 1 == 1)
            .unwrap_or(false);
        match (observed.get(idx), !sto_free) {
            (true, false) => missing += 1,
            (false, true) => extra += 1,
            _ => {}
        }
    }
    (missing, extra)
}
