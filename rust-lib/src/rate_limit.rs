use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub(crate) struct EpochAllocation {
    pub(crate) rln_identifier: String,
    pub(crate) epoch: u64,
    pub(crate) used: u64,
}

#[derive(Serialize, Deserialize, Clone, Default, PartialEq)]
pub(crate) struct AllocationState {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) allocations: Vec<EpochAllocation>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) epoch_size_sec: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) prune_floor: u64,
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if requires a fn(&T) -> bool"
)]
fn is_zero(n: &u64) -> bool {
    *n == 0
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AllocError {
    BudgetExhausted,
    EpochBelowFloor,
}

pub(crate) fn current_epoch(now_unix: u64, epoch_size_sec: u64) -> u64 {
    now_unix / epoch_size_sec.max(1)
}

pub(crate) fn reserve_slot(
    alloc: &mut AllocationState,
    rln_identifier_hex: &str,
    epoch: u64,
    retain_floor_candidate: u64,
    rate_limit: u64,
) -> Result<u64, AllocError> {
    let highest = alloc
        .allocations
        .iter()
        .filter(|a| a.epoch <= retain_floor_candidate)
        .map(|a| a.epoch.saturating_add(1))
        .max();
    let threshold = match highest {
        Some(h) => alloc.prune_floor.max(retain_floor_candidate.min(h)),
        None => alloc.prune_floor,
    };
    if epoch < threshold {
        return Err(AllocError::EpochBelowFloor);
    }

    let slot = if let Some(row) = alloc
        .allocations
        .iter_mut()
        .find(|a| a.rln_identifier == rln_identifier_hex && a.epoch == epoch)
    {
        if row.used >= rate_limit {
            return Err(AllocError::BudgetExhausted);
        }
        let slot = row.used;
        row.used += 1;
        slot
    } else {
        if rate_limit == 0 {
            return Err(AllocError::BudgetExhausted);
        }
        alloc.allocations.push(EpochAllocation {
            rln_identifier: rln_identifier_hex.to_string(),
            epoch,
            used: 1,
        });
        0
    };
    alloc.prune_floor = threshold;
    alloc.allocations.retain(|a| a.epoch >= threshold);
    Ok(slot)
}

pub(crate) fn remaining(
    allocations: &[EpochAllocation],
    rln_identifier_hex: &str,
    epoch: u64,
    rate_limit: u64,
) -> u64 {
    let used = allocations
        .iter()
        .find(|a| a.rln_identifier == rln_identifier_hex && a.epoch == epoch)
        .map(|a| a.used)
        .unwrap_or(0);
    rate_limit.saturating_sub(used)
}

#[cfg(test)]
mod tests {
    use super::*;

    const APP_A: &str = "aa";
    const APP_B: &str = "bb";

    #[test]
    fn epoch_is_floor_division() {
        assert_eq!(current_epoch(0, 600), 0);
        assert_eq!(current_epoch(599, 600), 0);
        assert_eq!(current_epoch(600, 600), 1);
        assert_eq!(current_epoch(1_250, 600), 2);
        assert_eq!(current_epoch(5, 0), 5);
    }

    #[test]
    fn slots_increment_from_zero_then_exhaust() {
        let mut alloc = AllocationState::default();
        assert_eq!(reserve_slot(&mut alloc, APP_A, 1, 1, 3), Ok(0));
        assert_eq!(reserve_slot(&mut alloc, APP_A, 1, 1, 3), Ok(1));
        assert_eq!(reserve_slot(&mut alloc, APP_A, 1, 1, 3), Ok(2));
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 1, 1, 3),
            Err(AllocError::BudgetExhausted)
        );
        assert_eq!(remaining(&alloc.allocations, APP_A, 1, 3), 0);
    }

    #[test]
    fn below_floor_epochs_are_pruned_but_in_window_ones_survive() {
        let mut alloc = AllocationState::default();
        assert_eq!(reserve_slot(&mut alloc, APP_A, 5, 4, 2), Ok(0));
        assert_eq!(reserve_slot(&mut alloc, APP_A, 4, 4, 2), Ok(0));
        assert_eq!(alloc.allocations.len(), 2);
        assert_eq!(reserve_slot(&mut alloc, APP_A, 5, 4, 2), Ok(1));
        assert_eq!(remaining(&alloc.allocations, APP_A, 5, 2), 0);
        assert_eq!(remaining(&alloc.allocations, APP_A, 4, 2), 1);
        assert_eq!(reserve_slot(&mut alloc, APP_A, 6, 5, 2), Ok(0));
        assert!(alloc.allocations.iter().all(|a| a.epoch >= 5));
        assert_eq!(alloc.prune_floor, 5);
    }

    #[test]
    fn interleaved_in_window_epochs_never_reissue_a_slot() {
        let mut alloc = AllocationState::default();
        assert_eq!(reserve_slot(&mut alloc, APP_A, 10, 9, 5), Ok(0));
        assert_eq!(reserve_slot(&mut alloc, APP_A, 9, 9, 5), Ok(0));
        assert_eq!(reserve_slot(&mut alloc, APP_A, 10, 9, 5), Ok(1));
        assert_eq!(remaining(&alloc.allocations, APP_A, 10, 5), 3);
        assert_eq!(remaining(&alloc.allocations, APP_A, 9, 5), 4);
    }

    #[test]
    fn applications_are_independent_within_an_epoch() {
        let mut alloc = AllocationState::default();
        assert_eq!(reserve_slot(&mut alloc, APP_A, 1, 1, 2), Ok(0));
        assert_eq!(reserve_slot(&mut alloc, APP_B, 1, 1, 2), Ok(0));
        assert_eq!(reserve_slot(&mut alloc, APP_A, 1, 1, 2), Ok(1));
        assert_eq!(reserve_slot(&mut alloc, APP_B, 1, 1, 2), Ok(1));
        assert_eq!(alloc.allocations.len(), 2);
        assert_eq!(remaining(&alloc.allocations, APP_A, 1, 2), 0);
        assert_eq!(remaining(&alloc.allocations, APP_B, 1, 2), 0);
    }

    #[test]
    fn zero_rate_limit_never_allocates() {
        let mut alloc = AllocationState::default();
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 1, 1, 0),
            Err(AllocError::BudgetExhausted)
        );
    }

    #[test]
    fn rewound_clock_never_reissues_a_pruned_epoch() {
        let mut alloc = AllocationState::default();
        assert_eq!(reserve_slot(&mut alloc, APP_A, 10, 9, 5), Ok(0));
        assert_eq!(reserve_slot(&mut alloc, APP_A, 12, 11, 5), Ok(0));
        assert_eq!(
            alloc.prune_floor, 11,
            "pruning epoch 10 must be recorded in the floor"
        );
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 10, 9, 5),
            Err(AllocError::EpochBelowFloor)
        );
        assert_eq!(
            alloc.prune_floor, 11,
            "a rewound candidate must never lower the floor"
        );
        assert_eq!(reserve_slot(&mut alloc, APP_A, 12, 9, 5), Ok(1));
    }

    #[test]
    fn forward_clock_spike_cannot_brick_future_epochs() {
        let mut alloc = AllocationState::default();
        assert_eq!(reserve_slot(&mut alloc, APP_A, 10, 9, 5), Ok(0));
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 1_000_000, 999_999, 5),
            Ok(0)
        );
        assert_eq!(
            alloc.prune_floor, 11,
            "the spike must not persist a far-future floor"
        );
        assert_eq!(reserve_slot(&mut alloc, APP_A, 12, 11, 5), Ok(0));
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 10, 11, 5),
            Err(AllocError::EpochBelowFloor)
        );
    }

    #[test]
    fn two_forward_spikes_do_not_brick_future_epochs() {
        let mut alloc = AllocationState::default();
        assert_eq!(reserve_slot(&mut alloc, APP_A, 10, 9, 5), Ok(0));
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 1_000_000, 999_999, 5),
            Ok(0)
        );
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 1_000_000, 999_999, 5),
            Ok(1)
        );
        assert_eq!(
            alloc.prune_floor, 11,
            "a second spike must not drag the floor to the spiked clock"
        );
        assert_eq!(reserve_slot(&mut alloc, APP_A, 12, 11, 5), Ok(0));
    }

    #[test]
    fn failed_reservations_mutate_nothing() {
        let mut alloc = AllocationState::default();
        assert_eq!(reserve_slot(&mut alloc, APP_A, 10, 9, 1), Ok(0));
        let before = (alloc.allocations.clone(), alloc.prune_floor);
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 10, 10, 1),
            Err(AllocError::BudgetExhausted)
        );
        assert_eq!((alloc.allocations.clone(), alloc.prune_floor), before);
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 11, 10, 0),
            Err(AllocError::BudgetExhausted)
        );
        assert_eq!((alloc.allocations.clone(), alloc.prune_floor), before);
        assert_eq!(reserve_slot(&mut alloc, APP_A, 12, 11, 1), Ok(0));
        let after_prune = (alloc.allocations.clone(), alloc.prune_floor);
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 10, 11, 1),
            Err(AllocError::EpochBelowFloor)
        );
        assert_eq!((alloc.allocations.clone(), alloc.prune_floor), after_prune);
    }

    #[test]
    fn widened_epoch_gap_never_resurrects_pruned_epochs() {
        let mut alloc = AllocationState::default();
        assert_eq!(reserve_slot(&mut alloc, APP_A, 9, 8, 5), Ok(0));
        assert_eq!(reserve_slot(&mut alloc, APP_A, 12, 11, 5), Ok(0));
        assert!(
            alloc.allocations.iter().all(|a| a.epoch >= 10),
            "epoch 9's row is pruned"
        );
        assert_eq!(alloc.prune_floor, 10);
        assert_eq!(
            reserve_slot(&mut alloc, APP_A, 9, 2, 5),
            Err(AllocError::EpochBelowFloor)
        );
        assert_eq!(reserve_slot(&mut alloc, APP_A, 12, 2, 5), Ok(1));
    }

    #[test]
    fn remaining_before_any_allocation_is_full() {
        let allocs = Vec::new();
        assert_eq!(remaining(&allocs, APP_A, 1, 100), 100);
    }
}
