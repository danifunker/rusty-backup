use thiserror::Error;

#[derive(Error, Debug)]
pub enum RustyBackupError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid MBR: {0}")]
    InvalidMbr(String),

    #[error("Invalid GPT: {0}")]
    InvalidGpt(String),

    #[error("Invalid APM: {0}")]
    InvalidApm(String),

    #[error("Invalid SGI Volume Header: {0}")]
    InvalidSgi(String),

    #[error("Invalid RDB: {0}")]
    InvalidRdb(String),

    #[error("Invalid AHDI: {0}")]
    InvalidAhdi(String),

    #[error("Device is currently mounted: {0}")]
    DeviceMounted(String),

    #[error("Insufficient space: need {needed} bytes, have {available} bytes")]
    InsufficientSpace { needed: u64, available: u64 },

    #[error("Filesystem validation failed: {0}")]
    InvalidFilesystem(String),

    #[error("Bad sector detected at LBA {lba}")]
    BadSector { lba: u64 },

    #[error("CHD file not found: {0}")]
    ChdFileMissing(String),

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}
