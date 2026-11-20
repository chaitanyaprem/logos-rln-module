use std::collections::HashMap;
use std::sync::Mutex;

pub(crate) enum RecordOutcome {
    Fresh,
    Duplicate,
    Collision {
        prior_x: [u8; 32],
        prior_y: [u8; 32],
    },
}

type Shares = ([u8; 32], [u8; 32]);
type EpochBucket = HashMap<[u8; 32], Shares>;
type RegistryLog = HashMap<u64, EpochBucket>;

static LOG: Mutex<Option<HashMap<String, RegistryLog>>> = Mutex::new(None);

pub(crate) fn record_verified(
    registry: &str,
    epoch: u64,
    nullifier: [u8; 32],
    share_x: [u8; 32],
    share_y: [u8; 32],
    retain_floor: u64,
) -> RecordOutcome {
    let mut guard = crate::util::lock(&LOG);
    let log = guard.get_or_insert_with(HashMap::new);
    let epochs = match log.get_mut(registry) {
        Some(epochs) => epochs,
        None => log.entry(registry.to_string()).or_default(),
    };
    epochs.retain(|&e, _| e >= retain_floor);
    let bucket = epochs.entry(epoch).or_default();
    match bucket.get(&nullifier).copied() {
        None => {
            bucket.insert(nullifier, (share_x, share_y));
            RecordOutcome::Fresh
        }
        Some((prior_x, _)) if prior_x == share_x => RecordOutcome::Duplicate,
        Some((prior_x, prior_y)) => RecordOutcome::Collision { prior_x, prior_y },
    }
}

#[cfg(test)]
pub(crate) fn reset_for_test() {
    *crate::util::lock(&LOG) = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    const REG: &str = "logos:local:d";

    #[test]
    fn fresh_then_duplicate_then_collision() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_for_test();
        let n = [0xa1u8; 32];
        let (x1, y1) = ([0x11u8; 32], [0x21u8; 32]);
        let (x2, y2) = ([0x12u8; 32], [0x22u8; 32]);

        assert!(matches!(
            record_verified(REG, 100, n, x1, y1, 0),
            RecordOutcome::Fresh
        ));
        assert!(matches!(
            record_verified(REG, 100, n, x1, y1, 0),
            RecordOutcome::Duplicate
        ));
        match record_verified(REG, 100, n, x2, y2, 0) {
            RecordOutcome::Collision { prior_x, prior_y } => {
                assert_eq!(prior_x, x1);
                assert_eq!(prior_y, y1);
            }
            _ => panic!("expected collision carrying the prior share"),
        }
    }

    #[test]
    fn prunes_below_retain_floor() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_for_test();
        let n = [0xb2u8; 32];
        let (x1, y1) = ([0x31u8; 32], [0x41u8; 32]);

        assert!(matches!(
            record_verified(REG, 100, n, x1, y1, 0),
            RecordOutcome::Fresh
        ));
        assert!(matches!(
            record_verified(REG, 102, n, x1, y1, 101),
            RecordOutcome::Fresh
        ));
        assert!(matches!(
            record_verified(REG, 100, n, x1, y1, 0),
            RecordOutcome::Fresh
        ));
    }

    #[test]
    fn prune_is_per_registry() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_for_test();
        let slow = "logos:local:d";
        let fast = "logos:local:m";
        let n = [0xc3u8; 32];
        let (x1, y1) = ([0x51u8; 32], [0x61u8; 32]);

        assert!(matches!(
            record_verified(slow, 2_980_000, n, x1, y1, 2_979_999),
            RecordOutcome::Fresh
        ));
        assert!(matches!(
            record_verified(fast, 178_800_000, n, x1, y1, 178_799_997),
            RecordOutcome::Fresh
        ));
        assert!(
            matches!(
                record_verified(slow, 2_980_000, n, x1, y1, 2_979_999),
                RecordOutcome::Duplicate
            ),
            "the fast registry's prune floor emptied the slow registry's log"
        );
    }
}
