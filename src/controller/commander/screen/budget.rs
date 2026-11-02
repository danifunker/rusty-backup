//! The SquashFS size budget asked for before Edit Mode (docs/squashfs_edit.md section 2.6).
//!
//! SquashFS has no free space and rebuilds the whole image on every write, so the user declares
//! a ceiling first: fit (whatever it becomes), an absolute size, or the current size plus some
//! headroom. [`BudgetForm`] holds that choice and refuses a ceiling above the image's container.

use fs_squashfs::squashfs_edit::{SizeBudget, SizePlan};

const MIB: u64 = 1024 * 1024;

/// Which budget shape is selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetMode {
    /// Accept whatever the rebuild produces (the container still binds).
    Fit,
    /// An absolute ceiling.
    Absolute,
    /// Current size plus headroom.
    Headroom,
}

/// An edit in the size-budget dialog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetAction {
    SetMode(BudgetMode),
    SetAbsoluteMib(u64),
    SetHeadroomMib(u64),
    /// "Start editing": install the budget and enter Edit Mode.
    Start,
    /// Leave the volume read-only.
    Cancel,
}

/// The dialog's state: the measured image and the budget being chosen.
#[derive(Debug, Clone, Copy)]
pub struct BudgetForm {
    plan: SizePlan,
    mode: BudgetMode,
    absolute_mib: u64,
    headroom_mib: u64,
}

impl BudgetForm {
    /// Seeded from where the image stands, so the numbers start meaningful rather than at zero.
    pub fn new(plan: SizePlan) -> Self {
        let cur_mib = plan.image_len.div_ceil(MIB).max(1);
        BudgetForm {
            plan,
            mode: BudgetMode::Fit,
            absolute_mib: plan.capacity.map(|c| c / MIB).unwrap_or(cur_mib * 2).max(1),
            headroom_mib: 64,
        }
    }

    pub fn plan(&self) -> &SizePlan {
        &self.plan
    }

    pub fn mode(&self) -> BudgetMode {
        self.mode
    }

    pub fn absolute_mib(&self) -> u64 {
        self.absolute_mib
    }

    pub fn headroom_mib(&self) -> u64 {
        self.headroom_mib
    }

    /// The budget the selection represents.
    pub fn selected(&self) -> SizeBudget {
        match self.mode {
            BudgetMode::Fit => SizeBudget::Fit,
            BudgetMode::Absolute => SizeBudget::Limit(self.absolute_mib * MIB),
            BudgetMode::Headroom => SizeBudget::Grow(self.headroom_mib * MIB),
        }
    }

    /// The hard ceiling the selection resolves to, for the live readout.
    pub fn ceiling(&self) -> Option<u64> {
        self.selected().ceiling_for(self.plan.image_len)
    }

    /// The ceiling is above what the container holds: Start is refused.
    pub fn over_capacity(&self) -> bool {
        matches!((self.ceiling(), self.plan.capacity), (Some(want), Some(cap)) if want > cap)
    }

    /// Apply a field edit; `Start` / `Cancel` are the caller's.
    pub(super) fn edit(&mut self, action: BudgetAction) {
        match action {
            BudgetAction::SetMode(m) => self.mode = m,
            BudgetAction::SetAbsoluteMib(n) => self.absolute_mib = n,
            BudgetAction::SetHeadroomMib(n) => self.headroom_mib = n,
            BudgetAction::Start | BudgetAction::Cancel => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(capacity: Option<u64>) -> SizePlan {
        SizePlan {
            image_len: 10 * MIB,
            capacity,
            content_len: 20 * MIB,
            ratio: 0.5,
            headroom: capacity.map(|c| c - 10 * MIB),
        }
    }

    #[test]
    fn the_form_seeds_from_the_image_and_refuses_a_ceiling_past_the_container() {
        let mut f = BudgetForm::new(plan(Some(32 * MIB)));
        assert_eq!(
            (f.mode(), f.absolute_mib(), f.headroom_mib()),
            (BudgetMode::Fit, 32, 64)
        );
        assert!(!f.over_capacity());
        f.edit(BudgetAction::SetMode(BudgetMode::Headroom));
        assert_eq!(f.ceiling(), Some(74 * MIB));
        assert!(f.over_capacity());
        f.edit(BudgetAction::SetHeadroomMib(8));
        assert!(!f.over_capacity());
        assert_eq!(f.selected(), SizeBudget::Grow(8 * MIB));
        let bare = BudgetForm::new(plan(None));
        assert_eq!(bare.absolute_mib(), 20, "a bare file seeds twice its size");
    }
}
