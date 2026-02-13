//! Shared validation helpers for Unix-like filesystems.
//!
//! Provides small utility functions for checking magic bytes, value ranges,
//! and collecting validation results with warnings and errors.

use anyhow::{bail, Result};

/// Check that magic bytes at a given offset match the expected value.
pub fn validate_magic(data: &[u8], offset: usize, expected: &[u8], fs_name: &str) -> Result<()> {
    if data.len() < offset + expected.len() {
        bail!("{fs_name}: data too short for magic check at offset {offset}");
    }
    if &data[offset..offset + expected.len()] != expected {
        bail!("{fs_name}: invalid magic at offset {offset}");
    }
    Ok(())
}

/// Check that a value is a power of two.
pub fn validate_power_of_two(value: u64, name: &str) -> Result<()> {
    if value == 0 || (value & (value - 1)) != 0 {
        bail!("{name}: {value} is not a power of two");
    }
    Ok(())
}

/// Check that a value falls within an expected range (inclusive).
pub fn validate_range(value: u64, min: u64, max: u64, name: &str) -> Result<()> {
    if value < min || value > max {
        bail!("{name}: {value} not in range [{min}, {max}]");
    }
    Ok(())
}

/// Accumulator for validation results (non-fatal warnings + fatal errors).
#[derive(Default)]
pub struct ValidationResult {
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

impl ValidationResult {
    pub fn warn(&mut self, msg: impl Into<String>) {
        self.warnings.push(msg.into());
    }

    pub fn error(&mut self, msg: impl Into<String>) {
        self.errors.push(msg.into());
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_magic_ok() {
        let data = [0x53, 0xEF, 0x00, 0x00]; // ext2 magic at offset 0
        assert!(validate_magic(&data, 0, &[0x53, 0xEF], "ext").is_ok());
    }

    #[test]
    fn test_validate_magic_at_offset() {
        let data = [0x00, 0x00, 0x53, 0xEF];
        assert!(validate_magic(&data, 2, &[0x53, 0xEF], "ext").is_ok());
    }

    #[test]
    fn test_validate_magic_mismatch() {
        let data = [0x00, 0x00, 0x00, 0x00];
        let err = validate_magic(&data, 0, &[0x53, 0xEF], "ext").unwrap_err();
        assert!(err.to_string().contains("invalid magic"));
    }

    #[test]
    fn test_validate_magic_too_short() {
        let data = [0x53];
        let err = validate_magic(&data, 0, &[0x53, 0xEF], "ext").unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn test_validate_power_of_two_ok() {
        assert!(validate_power_of_two(1, "block_size").is_ok());
        assert!(validate_power_of_two(512, "block_size").is_ok());
        assert!(validate_power_of_two(1024, "block_size").is_ok());
        assert!(validate_power_of_two(4096, "block_size").is_ok());
    }

    #[test]
    fn test_validate_power_of_two_zero() {
        assert!(validate_power_of_two(0, "block_size").is_err());
    }

    #[test]
    fn test_validate_power_of_two_non_power() {
        let err = validate_power_of_two(3, "block_size").unwrap_err();
        assert!(err.to_string().contains("not a power of two"));
    }

    #[test]
    fn test_validate_range_ok() {
        assert!(validate_range(512, 512, 4096, "block_size").is_ok());
        assert!(validate_range(4096, 512, 4096, "block_size").is_ok());
        assert!(validate_range(2000, 512, 4096, "block_size").is_ok());
    }

    #[test]
    fn test_validate_range_below() {
        let err = validate_range(256, 512, 4096, "block_size").unwrap_err();
        assert!(err.to_string().contains("not in range"));
    }

    #[test]
    fn test_validate_range_above() {
        let err = validate_range(8192, 512, 4096, "block_size").unwrap_err();
        assert!(err.to_string().contains("not in range"));
    }

    #[test]
    fn test_validation_result() {
        let mut vr = ValidationResult::default();
        assert!(!vr.has_errors());

        vr.warn("minor issue");
        assert!(!vr.has_errors());
        assert_eq!(vr.warnings.len(), 1);

        vr.error("fatal problem");
        assert!(vr.has_errors());
        assert_eq!(vr.errors.len(), 1);
    }
}
