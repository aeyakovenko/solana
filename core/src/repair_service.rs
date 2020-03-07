//! The `repair_service` module implements the tools necessary to generate a thread which
//! regularly finds missing shreds in the ledger and sends repair requests for those shreds
use crate::{
    cluster_info::ClusterInfo,
    result::Result,
    serve_repair::{RepairType, ServeRepair},
};
use solana_ledger::{
    bank_forks::BankForks,
    blockstore::{Blockstore, CompletedSlotsReceiver, SlotMeta},
};
use solana_sdk::{clock::Slot, epoch_schedule::EpochSchedule, pubkey::Pubkey};
use std::{
    collections::{BTreeSet, HashSet},
    iter::Iterator,
    net::UdpSocket,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, RwLock},
    thread::sleep,
    thread::{self, Builder, JoinHandle},
    time::Duration,
};

pub const MAX_REPAIR_LENGTH: usize = 512;
pub const REPAIR_MS: u64 = 100;
pub const MAX_ORPHANS: usize = 5;

pub enum RepairStrategy {
    RepairRange(RepairSlotRange),
    RepairAll {
        bank_forks: Arc<RwLock<BankForks>>,
        completed_slots_receiver: CompletedSlotsReceiver,
        epoch_schedule: EpochSchedule,
    },
}

pub struct RepairSlotRange {
    pub start: Slot,
    pub end: Slot,
}

impl Default for RepairSlotRange {
    fn default() -> Self {
        RepairSlotRange {
            start: 0,
            end: std::u64::MAX,
        }
    }
}

pub struct RepairService {
    t_repair: JoinHandle<()>,
}

impl RepairService {
    pub fn new(
        blockstore: Arc<Blockstore>,
        exit: Arc<AtomicBool>,
        repair_socket: Arc<UdpSocket>,
        cluster_info: Arc<RwLock<ClusterInfo>>,
        repair_strategy: RepairStrategy,
    ) -> Self {
        let t_repair = Builder::new()
            .name("solana-repair-service".to_string())
            .spawn(move || {
                Self::run(
                    &blockstore,
                    &exit,
                    &repair_socket,
                    &cluster_info,
                    repair_strategy,
                )
            })
            .unwrap();

        RepairService { t_repair }
    }

    fn run(
        blockstore: &Arc<Blockstore>,
        exit: &Arc<AtomicBool>,
        repair_socket: &Arc<UdpSocket>,
        cluster_info: &Arc<RwLock<ClusterInfo>>,
        repair_strategy: RepairStrategy,
    ) {
        let serve_repair = ServeRepair::new(cluster_info.clone());
        let mut epoch_slots: BTreeSet<Slot> = BTreeSet::new();
        let id = cluster_info.read().unwrap().id();
        if let RepairStrategy::RepairAll {
            ref epoch_schedule, ..
        } = repair_strategy
        {
            let current_root = blockstore.last_root();
            Self::initialize_epoch_slots(
                id,
                blockstore,
                &mut epoch_slots,
                current_root,
                epoch_schedule,
                cluster_info,
            );
        }
        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            let repairs = {
                match repair_strategy {
                    RepairStrategy::RepairRange(ref repair_slot_range) => {
                        // Strategy used by archivers
                        Self::generate_repairs_in_range(
                            blockstore,
                            MAX_REPAIR_LENGTH,
                            repair_slot_range,
                        )
                    }

                    RepairStrategy::RepairAll {
                        ref completed_slots_receiver,
                        ..
                    } => {
                        let new_root = blockstore.last_root();
                        let lowest_slot = blockstore.lowest_slot();
                        Self::update_epoch_slots(
                            id,
                            lowest_slot,
                            &cluster_info,
                            completed_slots_receiver,
                        );
                        Self::generate_repairs(blockstore, new_root, MAX_REPAIR_LENGTH)
                    }
                }
            };

            if let Ok(repairs) = repairs {
                let reqs: Vec<_> = repairs
                    .into_iter()
                    .filter_map(|repair_request| {
                        serve_repair
                            .repair_request(&repair_request)
                            .map(|result| (result, repair_request))
                            .ok()
                    })
                    .collect();

                for ((to, req), _) in reqs {
                    repair_socket.send_to(&req, to).unwrap_or_else(|e| {
                        info!("{} repair req send_to({}) error {:?}", id, to, e);
                        0
                    });
                }
            }
            sleep(Duration::from_millis(REPAIR_MS));
        }
    }

    // Generate repairs for all slots `x` in the repair_range.start <= x <= repair_range.end
    pub fn generate_repairs_in_range(
        blockstore: &Blockstore,
        max_repairs: usize,
        repair_range: &RepairSlotRange,
    ) -> Result<Vec<RepairType>> {
        // Slot height and shred indexes for shreds we want to repair
        let mut repairs: Vec<RepairType> = vec![];
        for slot in repair_range.start..=repair_range.end {
            if repairs.len() >= max_repairs {
                break;
            }

            let meta = blockstore
                .meta(slot)
                .expect("Unable to lookup slot meta")
                .unwrap_or(SlotMeta {
                    slot,
                    ..SlotMeta::default()
                });

            let new_repairs = Self::generate_repairs_for_slot(
                blockstore,
                slot,
                &meta,
                max_repairs - repairs.len(),
            );
            repairs.extend(new_repairs);
        }

        Ok(repairs)
    }

    fn generate_repairs(
        blockstore: &Blockstore,
        root: Slot,
        max_repairs: usize,
    ) -> Result<Vec<RepairType>> {
        // Slot height and shred indexes for shreds we want to repair
        let mut repairs: Vec<RepairType> = vec![];
        Self::generate_repairs_for_fork(blockstore, &mut repairs, max_repairs, root);

        // TODO: Incorporate gossip to determine priorities for repair?

        // Try to resolve orphans in blockstore
        let orphans = blockstore.orphans_iterator(root + 1).unwrap();
        Self::generate_repairs_for_orphans(orphans, &mut repairs);
        Ok(repairs)
    }

    fn generate_repairs_for_slot(
        blockstore: &Blockstore,
        slot: Slot,
        slot_meta: &SlotMeta,
        max_repairs: usize,
    ) -> Vec<RepairType> {
        if slot_meta.is_full() {
            vec![]
        } else if slot_meta.consumed == slot_meta.received {
            vec![RepairType::HighestShred(slot, slot_meta.received)]
        } else {
            let reqs = blockstore.find_missing_data_indexes(
                slot,
                slot_meta.first_shred_timestamp,
                slot_meta.consumed,
                slot_meta.received,
                max_repairs,
            );
            reqs.into_iter()
                .map(|i| RepairType::Shred(slot, i))
                .collect()
        }
    }

    fn generate_repairs_for_orphans(
        orphans: impl Iterator<Item = u64>,
        repairs: &mut Vec<RepairType>,
    ) {
        repairs.extend(orphans.take(MAX_ORPHANS).map(RepairType::Orphan));
    }

    /// Repairs any fork starting at the input slot
    fn generate_repairs_for_fork(
        blockstore: &Blockstore,
        repairs: &mut Vec<RepairType>,
        max_repairs: usize,
        slot: Slot,
    ) {
        let mut pending_slots = vec![slot];
        while repairs.len() < max_repairs && !pending_slots.is_empty() {
            let slot = pending_slots.pop().unwrap();
            if let Some(slot_meta) = blockstore.meta(slot).unwrap() {
                let new_repairs = Self::generate_repairs_for_slot(
                    blockstore,
                    slot,
                    &slot_meta,
                    max_repairs - repairs.len(),
                );
                repairs.extend(new_repairs);
                let next_slots = slot_meta.next_slots;
                pending_slots.extend(next_slots);
            } else {
                break;
            }
        }
    }

    fn get_completed_slots_past_root(
        blockstore: &Blockstore,
        slots_in_gossip: &mut BTreeSet<Slot>,
        root: Slot,
        epoch_schedule: &EpochSchedule,
    ) {
        let last_confirmed_epoch = epoch_schedule.get_leader_schedule_epoch(root);
        let last_epoch_slot = epoch_schedule.get_last_slot_in_epoch(last_confirmed_epoch);

        let meta_iter = blockstore
            .slot_meta_iterator(root + 1)
            .expect("Couldn't get db iterator");

        for (current_slot, meta) in meta_iter {
            if current_slot > last_epoch_slot {
                break;
            }
            if meta.is_full() {
                slots_in_gossip.insert(current_slot);
            }
        }
    }

    fn initialize_epoch_slots(
        id: Pubkey,
        blockstore: &Blockstore,
        slots_in_gossip: &mut BTreeSet<Slot>,
        root: Slot,
        epoch_schedule: &EpochSchedule,
        cluster_info: &RwLock<ClusterInfo>,
    ) {
        Self::get_completed_slots_past_root(blockstore, slots_in_gossip, root, epoch_schedule);

        // Safe to set into gossip because by this time, the leader schedule cache should
        // also be updated with the latest root (done in blockstore_processor) and thus
        // will provide a schedule to window_service for any incoming shreds up to the
        // last_confirmed_epoch.
        cluster_info.write().unwrap().push_epoch_slots(
            id,
            blockstore.lowest_slot(),
        );
    }

    // Update the gossiped structure used for the "Repairmen" repair protocol. See docs
    // for details.
    fn update_epoch_slots(
        id: Pubkey,
        lowest_slot: Slot,
        cluster_info: &RwLock<ClusterInfo>,
        completed_slots_receiver: &CompletedSlotsReceiver,
    ) {
        //TBD: remove this once new EpochSlots are merged
        while let Ok(_) = completed_slots_receiver.try_recv() {
        }
        cluster_info.write().unwrap().push_epoch_slots(
            id,
            lowest_slot,
        );
    }

    #[allow(dead_code)]
    fn find_incomplete_slots(blockstore: &Blockstore, root: Slot) -> HashSet<Slot> {
        blockstore
            .live_slots_iterator(root)
            .filter_map(|(slot, slot_meta)| {
                if !slot_meta.is_full() {
                    Some(slot)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_repair.join()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::cluster_info::Node;
    use solana_ledger::blockstore::{
        make_chaining_slot_entries, make_many_slot_entries, make_slot_entries,
    };
    use solana_ledger::shred::max_ticks_per_n_shreds;
    use solana_ledger::{blockstore::Blockstore, get_tmp_ledger_path};

    #[test]
    pub fn test_repair_orphan() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Create some orphan slots
            let (mut shreds, _) = make_slot_entries(1, 0, 1);
            let (shreds2, _) = make_slot_entries(5, 2, 1);
            shreds.extend(shreds2);
            blockstore.insert_shreds(shreds, None, false).unwrap();
            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, 2).unwrap(),
                vec![RepairType::HighestShred(0, 0), RepairType::Orphan(2)]
            );
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_empty_slot() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let (shreds, _) = make_slot_entries(2, 0, 1);

            // Write this shred to slot 2, should chain to slot 0, which we haven't received
            // any shreds for
            blockstore.insert_shreds(shreds, None, false).unwrap();

            // Check that repair tries to patch the empty slot
            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, 2).unwrap(),
                vec![RepairType::HighestShred(0, 0)]
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_generate_repairs() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let nth = 3;
            let num_slots = 2;

            // Create some shreds
            let (mut shreds, _) = make_many_slot_entries(0, num_slots as u64, 150 as u64);
            let num_shreds = shreds.len() as u64;
            let num_shreds_per_slot = num_shreds / num_slots;

            // write every nth shred
            let mut shreds_to_write = vec![];
            let mut missing_indexes_per_slot = vec![];
            for i in (0..num_shreds).rev() {
                let index = i % num_shreds_per_slot;
                if index % nth == 0 {
                    shreds_to_write.insert(0, shreds.remove(i as usize));
                } else if i < num_shreds_per_slot {
                    missing_indexes_per_slot.insert(0, index);
                }
            }
            blockstore
                .insert_shreds(shreds_to_write, None, false)
                .unwrap();
            // sleep so that the holes are ready for repair
            sleep(Duration::from_secs(1));
            let expected: Vec<RepairType> = (0..num_slots)
                .flat_map(|slot| {
                    missing_indexes_per_slot
                        .iter()
                        .map(move |shred_index| RepairType::Shred(slot as u64, *shred_index))
                })
                .collect();

            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, std::usize::MAX).unwrap(),
                expected
            );

            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, expected.len() - 2).unwrap()[..],
                expected[0..expected.len() - 2]
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_generate_highest_repair() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let num_entries_per_slot = 100;

            // Create some shreds
            let (mut shreds, _) = make_slot_entries(0, 0, num_entries_per_slot as u64);
            let num_shreds_per_slot = shreds.len() as u64;

            // Remove last shred (which is also last in slot) so that slot is not complete
            shreds.pop();

            blockstore.insert_shreds(shreds, None, false).unwrap();

            // We didn't get the last shred for this slot, so ask for the highest shred for that slot
            let expected: Vec<RepairType> =
                vec![RepairType::HighestShred(0, num_shreds_per_slot - 1)];

            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, std::usize::MAX).unwrap(),
                expected
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_range() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let slots: Vec<u64> = vec![1, 3, 5, 7, 8];
            let num_entries_per_slot = max_ticks_per_n_shreds(1) + 1;

            let shreds = make_chaining_slot_entries(&slots, num_entries_per_slot);
            for (mut slot_shreds, _) in shreds.into_iter() {
                slot_shreds.remove(0);
                blockstore.insert_shreds(slot_shreds, None, false).unwrap();
            }
            // sleep to make slot eligible for repair
            sleep(Duration::from_secs(1));
            // Iterate through all possible combinations of start..end (inclusive on both
            // sides of the range)
            for start in 0..slots.len() {
                for end in start..slots.len() {
                    let mut repair_slot_range = RepairSlotRange::default();
                    repair_slot_range.start = slots[start];
                    repair_slot_range.end = slots[end];
                    let expected: Vec<RepairType> = (repair_slot_range.start
                        ..=repair_slot_range.end)
                        .map(|slot_index| {
                            if slots.contains(&(slot_index as u64)) {
                                RepairType::Shred(slot_index as u64, 0)
                            } else {
                                RepairType::HighestShred(slot_index as u64, 0)
                            }
                        })
                        .collect();

                    assert_eq!(
                        RepairService::generate_repairs_in_range(
                            &blockstore,
                            std::usize::MAX,
                            &repair_slot_range
                        )
                        .unwrap(),
                        expected
                    );
                }
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_range_highest() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let num_entries_per_slot = 10;

            let num_slots = 1;
            let start = 5;

            // Create some shreds in slots 0..num_slots
            for i in start..start + num_slots {
                let parent = if i > 0 { i - 1 } else { 0 };
                let (shreds, _) = make_slot_entries(i, parent, num_entries_per_slot as u64);

                blockstore.insert_shreds(shreds, None, false).unwrap();
            }

            let end = 4;
            let expected: Vec<RepairType> = vec![
                RepairType::HighestShred(end - 2, 0),
                RepairType::HighestShred(end - 1, 0),
                RepairType::HighestShred(end, 0),
            ];

            let mut repair_slot_range = RepairSlotRange::default();
            repair_slot_range.start = 2;
            repair_slot_range.end = end;

            assert_eq!(
                RepairService::generate_repairs_in_range(
                    &blockstore,
                    std::usize::MAX,
                    &repair_slot_range
                )
                .unwrap(),
                expected
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_get_completed_slots_past_root() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_entries_per_slot = 10;
            let root = 10;

            let fork1 = vec![5, 7, root, 15, 20, 21];
            let fork1_shreds: Vec<_> = make_chaining_slot_entries(&fork1, num_entries_per_slot)
                .into_iter()
                .flat_map(|(shreds, _)| shreds)
                .collect();
            let fork2 = vec![8, 12];
            let fork2_shreds = make_chaining_slot_entries(&fork2, num_entries_per_slot);

            // Remove the last shred from each slot to make an incomplete slot
            let fork2_incomplete_shreds: Vec<_> = fork2_shreds
                .into_iter()
                .flat_map(|(mut shreds, _)| {
                    shreds.pop();
                    shreds
                })
                .collect();
            let mut full_slots = BTreeSet::new();

            blockstore.insert_shreds(fork1_shreds, None, false).unwrap();
            blockstore
                .insert_shreds(fork2_incomplete_shreds, None, false)
                .unwrap();

            // Test that only slots > root from fork1 were included
            let epoch_schedule = EpochSchedule::custom(32, 32, false);

            RepairService::get_completed_slots_past_root(
                &blockstore,
                &mut full_slots,
                root,
                &epoch_schedule,
            );

            let mut expected: BTreeSet<_> = fork1.into_iter().filter(|x| *x > root).collect();
            assert_eq!(full_slots, expected);

            // Test that slots past the last confirmed epoch boundary don't get included
            let last_epoch = epoch_schedule.get_leader_schedule_epoch(root);
            let last_slot = epoch_schedule.get_last_slot_in_epoch(last_epoch);
            let fork3 = vec![last_slot, last_slot + 1];
            let fork3_shreds: Vec<_> = make_chaining_slot_entries(&fork3, num_entries_per_slot)
                .into_iter()
                .flat_map(|(shreds, _)| shreds)
                .collect();
            blockstore.insert_shreds(fork3_shreds, None, false).unwrap();
            RepairService::get_completed_slots_past_root(
                &blockstore,
                &mut full_slots,
                root,
                &epoch_schedule,
            );
            expected.insert(last_slot);
            assert_eq!(full_slots, expected);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_update_epoch_slots() {
            let node_info = Node::new_localhost_with_pubkey(&Pubkey::default());
            let cluster_info = RwLock::new(ClusterInfo::new_with_invalid_keypair(
                node_info.info.clone(),
            ));
            let (_, completed_slots_receiver) = std::sync::mpsc::sync_channel(1);
            RepairService::update_epoch_slots(
                Pubkey::default(),
                5,
                &cluster_info,
                &completed_slots_receiver,
            );
            let epoch = cluster_info.read().unwrap().get_epoch_state_for_node(&Pubkey::default(), None).unwrap().0.clone();
            assert_eq!(epoch.lowest, 5);
    }

    #[test]
    fn test_find_incomplete_slots() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_entries_per_slot = 100;
            let (mut shreds, _) = make_slot_entries(0, 0, num_entries_per_slot);
            assert!(shreds.len() > 1);
            let (shreds4, _) = make_slot_entries(4, 0, num_entries_per_slot);
            shreds.extend(shreds4);
            blockstore.insert_shreds(shreds, None, false).unwrap();

            // Nothing is incomplete
            assert!(RepairService::find_incomplete_slots(&blockstore, 0).is_empty());

            // Insert a slot 5 that chains to an incomplete orphan slot 3
            let (shreds5, _) = make_slot_entries(5, 3, num_entries_per_slot);
            blockstore.insert_shreds(shreds5, None, false).unwrap();
            assert_eq!(
                RepairService::find_incomplete_slots(&blockstore, 0),
                vec![3].into_iter().collect()
            );

            // Insert another incomplete orphan slot 2 that is the parent of slot 3.
            // Both should be incomplete
            let (shreds3, _) = make_slot_entries(3, 2, num_entries_per_slot);
            blockstore
                .insert_shreds(shreds3[1..].to_vec(), None, false)
                .unwrap();
            assert_eq!(
                RepairService::find_incomplete_slots(&blockstore, 0),
                vec![2, 3].into_iter().collect()
            );

            // Insert a incomplete slot 6 that chains to the root 0,
            // should also be incomplete
            let (shreds6, _) = make_slot_entries(6, 0, num_entries_per_slot);
            blockstore
                .insert_shreds(shreds6[1..].to_vec(), None, false)
                .unwrap();
            assert_eq!(
                RepairService::find_incomplete_slots(&blockstore, 0),
                vec![2, 3, 6].into_iter().collect()
            );

            // Complete slot 3, should no longer be marked incomplete
            blockstore
                .insert_shreds(shreds3[..].to_vec(), None, false)
                .unwrap();

            assert_eq!(
                RepairService::find_incomplete_slots(&blockstore, 0),
                vec![2, 6].into_iter().collect()
            );
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }
}
