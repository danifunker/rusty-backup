//! Why a model operation was refused, as a kind a shell can act on: the CLI maps it to an exit
//! code, a GUI to its wording. The message is the user-facing text.

/// What kind of refusal a [`Fault`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// A named thing does not exist: an image file, a partition, a path inside a volume.
    NotFound,
    /// Not allowed, including a write to a read-only target.
    PermissionDenied,
}

#[derive(Debug)]
pub struct Fault {
    pub kind: FaultKind,
    pub message: String,
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Fault {}

pub fn not_found(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(Fault {
        kind: FaultKind::NotFound,
        message: message.into(),
    })
}

pub fn permission_denied(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(Fault {
        kind: FaultKind::PermissionDenied,
        message: message.into(),
    })
}

/// The first [`FaultKind`] in `err`'s chain, so `.context(..)` never hides it.
pub fn kind_of(err: &anyhow::Error) -> Option<FaultKind> {
    err.chain()
        .find_map(|e| e.downcast_ref::<Fault>())
        .map(|f| f.kind)
}
