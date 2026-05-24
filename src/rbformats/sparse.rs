//! Shared sparse-allocation bookkeeping for the sparse container formats.
//!
//! Dynamic VHD (blocks), QCOW2 (clusters), and VMDK sparse (grains) all allocate
//! fixed-size storage units on demand and skip runs of all-zero source data. The
//! per-unit accounting is identical; only the unit size and the table that stores
//! the returned offsets differ. [`SparseAllocator`] owns the "where does the next
//! unit go" cursor and nothing else — the caller passes the unit size and owns the
//! BAT / L1+L2 / GD+GT it writes the returned offsets into.
//!
//! Two allocation modes share the same struct:
//! - **streaming append** (export): seed `next_offset` at the first data offset,
//!   call [`SparseAllocator::alloc`] once per non-zero unit as you stream forward.
//! - **incremental edit** (in-place allocate-on-write): seed `next_offset` at the
//!   current end-of-data, call `alloc` to carve a fresh unit at EOF, write the
//!   returned offset into the table.
//!
//! We deliberately keep this a plain helper struct rather than a trait — the
//! genuinely shared state is tiny (see CONTRIBUTING "patterns to avoid", which
//! rejected a unified sparse-format trait).

/// Returns true if every byte in `unit` is zero — the zero-skip predicate that
/// decides whether a source unit needs a backing allocation at all.
///
/// Thin re-export wrapper over the crate-internal `is_all_zeros` so the sparse
/// writers have a single named predicate to call.
pub(crate) fn is_zero_unit(unit: &[u8]) -> bool {
    super::is_all_zeros(unit)
}

/// Hands out monotonically increasing host offsets for fixed-size storage units.
///
/// Unit-size-agnostic (the caller passes the byte size of one unit) and
/// table-agnostic (the caller writes the returned offset into whatever
/// allocation table the format uses).
#[derive(Debug, Clone)]
pub(crate) struct SparseAllocator {
    /// Host byte offset where the next allocated unit will land.
    next_offset: u64,
    /// Bytes consumed by one allocation unit (e.g. VHD `bitmap_size + block_size`,
    /// a QCOW2 cluster, a VMDK grain).
    unit_size: u64,
}

impl SparseAllocator {
    /// Create an allocator whose first allocation lands at `first_offset` and
    /// that advances by `unit_size` bytes per allocation.
    ///
    /// Panics if `unit_size` is zero.
    pub(crate) fn new(first_offset: u64, unit_size: u64) -> Self {
        assert!(
            unit_size > 0,
            "sparse allocation unit size must be non-zero"
        );
        Self {
            next_offset: first_offset,
            unit_size,
        }
    }

    /// Reserve the next unit, returning its host byte offset and advancing the
    /// cursor by one unit.
    pub(crate) fn alloc(&mut self) -> u64 {
        let offset = self.next_offset;
        self.next_offset += self.unit_size;
        offset
    }

    /// Host byte offset that the next [`alloc`](Self::alloc) would return — i.e.
    /// the current end of allocated data. After a streaming export this is where
    /// a trailing footer / final structure goes.
    pub(crate) fn next_offset(&self) -> u64 {
        self.next_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_advances_by_unit_size() {
        let mut a = SparseAllocator::new(1536, 2 * 1024 * 1024 + 512);
        assert_eq!(a.next_offset(), 1536);
        let first = a.alloc();
        assert_eq!(first, 1536);
        let second = a.alloc();
        assert_eq!(second, 1536 + 2 * 1024 * 1024 + 512);
        assert_eq!(a.next_offset(), second + 2 * 1024 * 1024 + 512);
    }

    #[test]
    fn no_alloc_leaves_cursor_at_start() {
        let a = SparseAllocator::new(4096, 65536);
        assert_eq!(a.next_offset(), 4096);
    }

    #[test]
    fn zero_unit_predicate() {
        assert!(is_zero_unit(&[0u8; 64]));
        assert!(!is_zero_unit(&[0, 0, 1, 0]));
        assert!(is_zero_unit(&[]));
    }
}
