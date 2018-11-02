//! This module implements Cluster Replicated Data Store for
//! asynchronous updates in a distributed network.  ContactInfo data
//! type represents the location of distributed nodes in the network
//! which are hosting copies Crds itself, and ContactInfo is stored in
//! the Crds itself The Crds maintains a list of the nodes that are
//! distributing copies of itself.  For all the types replicated in the
//! Crds, the latest version is picked.  This creates a mutual dependency
//! between Gossip <-> Crds libraries.
//!
//! Semantically data is organized in Labels and Values and is indexed by Pubkey.
//!     struct PubkeyRecord {
//!         contact_info: ContactInfo,
//!         vote: Transaction,
//!         leader_id: Pubkey,
//!     }
//!
//! The actual data is stored in a single map of
//! `CrdsValueLabel(Pubkey) -> CrdsValue` This allows for partial record
//! updates to be propagated through the network.  With a label specific merge strategy.
//! This means that full `Record` updates are not atomic.
//!
//! Additional labels can be added by appending them to the CrdsValueLabel,
//! CrdsValue enums.
//!
//! Merge strategy
//! `(a: Crds, b: Crds) -> Crds`
//! is implemented in the following steps:
//!
//! 1. a has a local Crds::version A', and
//!    it knows of b's remote Crds::version B
//! 2. b has a local Crds::version B' and it
//!    knows of a's remote Crds::version A
//! 3. a asynchronously calls b.get_updates_since(B, max_bytes)
//! 4. b responds with changes to its `table` between B and up to
//!    B' and the max version in the response B''
//! 5. a inserts all the updates that b responded with, records
//!    a.version value at which they where committed into a.table.
//!    It updates a.remote[&b] = B''
//! 6. b does the same
//! 7. Eventually the values returned in the updated will be
//!    synchronized and no new inserts will occur in either table.
//! 8. get_updates_since will then return 0 updates
//!
//!
//! Each item in the Crds has its own version.  It's only successfully
//! updated if the update has a newer version then what is currently
//! in the Crds::table.  So if b had no new updates for a, while it
//! would still transmit data, a's would not update any values in its
//! table and there for wouldn't update its own Crds::version.

use bincode::{serialize, serialized_size};
use crds_value::{CrdsValue, CrdsValueLabel};
use hash::{hash, Hash};
use indexmap::map::IndexMap;
use solana_sdk::pubkey::Pubkey;
use std::cmp;

pub struct Crds {
    /// Stores the map of labels and values
    pub table: IndexMap<CrdsValueLabel, VersionedCrdsValue>,

    /// the version of the `table`
    /// every change to `table` should increase this version number
    pub version: u64,
}

#[derive(PartialEq, Debug)]
pub enum CrdsError {
    InsertFailed,
}

/// This structure stores some local metadata assosciated with the CrdsValue
/// The implementation of PartialOrd ensures that the "highest" version is always picked to be
/// stored in the Crds
#[derive(PartialEq, Debug)]
pub struct VersionedCrdsValue {
    pub value: CrdsValue,
    /// local time when inserted
    pub insert_timestamp: u64,
    /// local time when updated
    pub local_timestamp: u64,
    /// local crds version when added
    local_version: u64,
    /// value hash
    pub value_hash: Hash,
}

impl PartialOrd for VersionedCrdsValue {
    fn partial_cmp(&self, other: &VersionedCrdsValue) -> Option<cmp::Ordering> {
        if self.value.label() != other.value.label() {
            None
        } else {
            if self.value.wallclock() == other.value.wallclock() {
                Some(self.value_hash.cmp(&other.value_hash))
            } else {
                Some(self.value.wallclock().cmp(&other.value.wallclock()))
            }
        }
    }
}
impl VersionedCrdsValue {
    pub fn new(local_timestamp: u64, version: u64, value: CrdsValue) -> Self {
        let value_hash = hash(&serialize(&value).unwrap());
        VersionedCrdsValue {
            value,
            insert_timestamp: local_timestamp,
            local_timestamp,
            local_version: version,
            value_hash,
        }
    }
}

impl Default for Crds {
    fn default() -> Self {
        Crds {
            table: IndexMap::new(),
            version: 0,
        }
    }
}

impl Crds {
    /// must be called atomically with `insert_versioned`
    pub fn new_versioned(&self, local_timestamp: u64, value: CrdsValue) -> VersionedCrdsValue {
        VersionedCrdsValue::new(local_timestamp, self.version, value)
    }
    /// insert the new value, returns the old value if insert succeeds
    pub fn insert_versioned(
        &mut self,
        new_value: VersionedCrdsValue,
    ) -> Result<Option<VersionedCrdsValue>, CrdsError> {
        let label = new_value.value.label();
        let wallclock = new_value.value.wallclock();
        let do_insert = self
            .table
            .get(&label)
            .map(|current| new_value > *current)
            .unwrap_or(true);
        if do_insert {
            self.version += 1;
            let old = self.table.insert(label, new_value);
            Ok(old)
        } else {
            trace!("INSERT FAILED data: {} new.wallclock: {}", label, wallclock,);
            Err(CrdsError::InsertFailed)
        }
    }
    pub fn insert(
        &mut self,
        value: CrdsValue,
        local_timestamp: u64,
    ) -> Result<Option<VersionedCrdsValue>, CrdsError> {
        let new_value = self.new_versioned(local_timestamp, value);
        self.insert_versioned(new_value)
    }
    pub fn lookup(&self, label: &CrdsValueLabel) -> Option<&CrdsValue> {
        self.table.get(label).map(|x| &x.value)
    }

    pub fn lookup_versioned(&self, label: &CrdsValueLabel) -> Option<&VersionedCrdsValue> {
        self.table.get(label)
    }

    fn update_label_timestamp(&mut self, id: &CrdsValueLabel, now: u64) {
        self.table
            .get_mut(id)
            .map(|e| e.local_timestamp = cmp::max(e.local_timestamp, now));
    }

    /// Update the timestamp's of all the labels that are assosciated with Pubkey
    pub fn update_record_timestamp(&mut self, pubkey: Pubkey, now: u64) {
        for label in &CrdsValue::record_labels(pubkey) {
            self.update_label_timestamp(label, now);
        }
    }

    /// find all the keys that are older or equal to min_ts
    pub fn find_old_labels(&self, min_ts: u64) -> Vec<CrdsValueLabel> {
        self.table
            .iter()
            .filter_map(|(k, v)| {
                if v.local_timestamp <= min_ts {
                    Some(k)
                } else {
                    None
                }
            }).cloned()
            .collect()
    }

    pub fn remove(&mut self, key: &CrdsValueLabel) {
        self.table.remove(key);
    }

    /// Get updated node since min_version up to a maximum of `max_bytes` of updates
    /// * min_version - return updates greater then min_version
    /// * max_bytes - max number of bytes to encode.  This would allow gossip to fit the response
    /// into a 64kb packet.
    /// * remote_versions - The remote `Crds::version` values for each update.  This is a structure
    /// about the external state of the network that is maintained by the gossip library.
    /// Returns (max version, updates)  
    /// * max version - the maximum version that is in the updates
    /// * updates - a vector of (CrdsValues, CrdsValue's remote update index) that have been changed.  CrdsValues
    /// remote update index is the last update index seen from the ContactInfo that is referenced by
    /// CrdsValue::label().pubkey().
    pub fn get_updates_since(
        &self,
        min_version: u64,
        mut max_bytes: usize,
        remote_versions: &IndexMap<Pubkey, u64>,
    ) -> (u64, Vec<(CrdsValue, u64)>) {
        let mut items: Vec<_> = self
            .table
            .iter()
            .filter_map(|(k, x)| {
                if k.pubkey() != Pubkey::default() && x.local_version >= min_version {
                    let remote = *remote_versions.get(&k.pubkey()).unwrap_or(&0);
                    Some((x, remote))
                } else {
                    None
                }
            }).collect();
        trace!("items length: {}", items.len());
        items.sort_by_key(|k| k.0.local_version);
        let last = {
            let mut last = 0;
            let sz = serialized_size(&min_version).unwrap() as usize;
            if max_bytes > sz {
                max_bytes -= sz;
            }
            for i in &items {
                let sz = serialized_size(&(&i.0.value, i.1)).unwrap() as usize;
                if max_bytes < sz {
                    break;
                }
                max_bytes -= sz;
                last += 1;
            }
            last
        };
        let last_version = cmp::max(last, 1) - 1;
        let max_update_version = items
            .get(last_version)
            .map(|i| i.0.local_version)
            .unwrap_or(0);
        let updates: Vec<(CrdsValue, u64)> = items
            .into_iter()
            .take(last)
            .map(|x| (x.0.value.clone(), x.1))
            .collect();
        (max_update_version, updates)
    }
    pub fn contact_info_trace(&self, leader_id: Pubkey) -> String {
        let nodes: Vec<_> = self
            .table
            .values()
            .filter_map(|v| (*v).value.clone().contact_info())
            .map(|node| {
                format!(
                    " ncp: {:20} | {}{}\n \
                     rpu: {:20} |\n \
                     tpu: {:20} |\n",
                    node.ncp.to_string(),
                    node.id,
                    if node.id == leader_id {
                        " <==== leader"
                    } else {
                        ""
                    },
                    node.rpu.to_string(),
                    node.tpu.to_string()
                )
            }).collect();

        format!(
            " Contact_info              | Node identifier\n\
             ---------------------------+------------------\n\
             {}\n \
             Nodes: {}",
            nodes.join(""),
            nodes.len()
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use contact_info::ContactInfo;
    use crds_value::LeaderId;
    use signature::{Keypair, KeypairUtil};

    #[test]
    fn test_insert() {
        let mut crds = Crds::default();
        let val = CrdsValue::LeaderId(LeaderId::default());
        assert_eq!(crds.insert(val.clone(), 0).ok(), Some(None));
        assert_eq!(crds.version, 1);
        assert_eq!(crds.table.len(), 1);
        assert!(crds.table.contains_key(&val.label()));
        assert_eq!(crds.table[&val.label()].local_timestamp, 0);
        assert_eq!(crds.table[&val.label()].local_version, crds.version - 1);
    }
    #[test]
    fn test_update_old() {
        let mut crds = Crds::default();
        let val = CrdsValue::LeaderId(LeaderId::default());
        assert_eq!(crds.insert(val.clone(), 0), Ok(None));
        assert_eq!(crds.insert(val.clone(), 1), Err(CrdsError::InsertFailed));
        assert_eq!(crds.table[&val.label()].local_timestamp, 0);
        assert_eq!(crds.table[&val.label()].local_version, crds.version - 1);
        assert_eq!(crds.version, 1);
    }
    #[test]
    fn test_update_new() {
        let mut crds = Crds::default();
        let original = CrdsValue::LeaderId(LeaderId::default());
        assert_matches!(crds.insert(original.clone(), 0), Ok(_));
        let val = CrdsValue::LeaderId(LeaderId {
            id: Pubkey::default(),
            leader_id: Pubkey::default(),
            wallclock: 1,
        });
        assert_eq!(
            crds.insert(val.clone(), 1).unwrap().unwrap().value,
            original
        );
        assert_eq!(crds.table[&val.label()].local_timestamp, 1);
        assert_eq!(crds.table[&val.label()].local_version, crds.version - 1);
        assert_eq!(crds.version, 2);
    }
    #[test]
    fn test_update_timestsamp() {
        let mut crds = Crds::default();
        let val = CrdsValue::LeaderId(LeaderId::default());
        assert_eq!(crds.insert(val.clone(), 0), Ok(None));

        crds.update_label_timestamp(&val.label(), 1);
        assert_eq!(crds.table[&val.label()].local_timestamp, 1);
        assert_eq!(crds.table[&val.label()].insert_timestamp, 0);

        let val2 = CrdsValue::ContactInfo(ContactInfo::default());
        assert_eq!(val2.label().pubkey(), val.label().pubkey());
        assert_matches!(crds.insert(val2.clone(), 0), Ok(None));

        crds.update_record_timestamp(val.label().pubkey(), 2);
        assert_eq!(crds.table[&val.label()].local_timestamp, 2);
        assert_eq!(crds.table[&val.label()].insert_timestamp, 0);
        assert_eq!(crds.table[&val2.label()].local_timestamp, 2);
        assert_eq!(crds.table[&val2.label()].insert_timestamp, 0);

        crds.update_record_timestamp(val.label().pubkey(), 1);
        assert_eq!(crds.table[&val.label()].local_timestamp, 2);
        assert_eq!(crds.table[&val.label()].insert_timestamp, 0);

        let mut ci = ContactInfo::default();
        ci.wallclock += 1;
        let val3 = CrdsValue::ContactInfo(ci);
        assert_matches!(crds.insert(val3.clone(), 3), Ok(Some(_)));
        assert_eq!(crds.table[&val2.label()].local_timestamp, 3);
        assert_eq!(crds.table[&val2.label()].insert_timestamp, 3);
    }
    #[test]
    fn test_find_old_records() {
        let mut crds = Crds::default();
        let val = CrdsValue::LeaderId(LeaderId::default());
        assert_eq!(crds.insert(val.clone(), 1), Ok(None));

        assert!(crds.find_old_labels(0).is_empty());
        assert_eq!(crds.find_old_labels(1), vec![val.label()]);
        assert_eq!(crds.find_old_labels(2), vec![val.label()]);
    }
    #[test]
    fn test_remove() {
        let mut crds = Crds::default();
        let val = CrdsValue::LeaderId(LeaderId::default());
        assert_matches!(crds.insert(val.clone(), 1), Ok(_));

        assert_eq!(crds.find_old_labels(1), vec![val.label()]);
        crds.remove(&val.label());
        assert!(crds.find_old_labels(1).is_empty());
    }
    #[test]
    fn test_updates_empty() {
        let crds = Crds::default();
        let remotes = IndexMap::new();
        assert_eq!(crds.get_updates_since(0, 0, &remotes), (0, vec![]));
        assert_eq!(crds.get_updates_since(0, 1024, &remotes), (0, vec![]));
    }
    #[test]
    fn test_updates_default_key() {
        let mut crds = Crds::default();
        let val = CrdsValue::LeaderId(LeaderId::default());
        assert_matches!(crds.insert(val.clone(), 1), Ok(_));
        let mut remotes = IndexMap::new();
        remotes.insert(val.label().pubkey(), 1);
        assert_eq!(crds.get_updates_since(0, 1024, &remotes), (0, vec![]));
        assert_eq!(crds.get_updates_since(0, 0, &remotes), (0, vec![]));
        assert_eq!(crds.get_updates_since(1, 1024, &remotes), (0, vec![]));
    }
    #[test]
    fn test_updates_real_key() {
        let key = Keypair::new();
        let mut crds = Crds::default();
        let val = CrdsValue::LeaderId(LeaderId {
            id: key.pubkey(),
            leader_id: Pubkey::default(),
            wallclock: 0,
        });
        assert_eq!(crds.insert(val.clone(), 1), Ok(None));
        let mut remotes = IndexMap::new();
        assert_eq!(
            crds.get_updates_since(0, 1024, &remotes),
            (0, vec![(val.clone(), 0)])
        );
        remotes.insert(val.label().pubkey(), 1);
        let sz = serialized_size(&(0, vec![(val.clone(), 1)])).unwrap() as usize;
        assert_eq!(crds.get_updates_since(0, sz, &remotes), (0, vec![(val, 1)]));
        assert_eq!(crds.get_updates_since(0, sz - 1, &remotes), (0, vec![]));
        assert_eq!(crds.get_updates_since(0, 0, &remotes), (0, vec![]));
        assert_eq!(crds.get_updates_since(1, sz, &remotes), (0, vec![]));
    }
    #[test]
    fn test_equal() {
        let key = Keypair::new();
        let v1 = VersionedCrdsValue::new(
            1,
            1,
            CrdsValue::LeaderId(LeaderId {
                id: key.pubkey(),
                leader_id: Pubkey::default(),
                wallclock: 0,
            }),
        );
        let v2 = VersionedCrdsValue::new(
            1,
            1,
            CrdsValue::LeaderId(LeaderId {
                id: key.pubkey(),
                leader_id: Pubkey::default(),
                wallclock: 0,
            }),
        );
        assert!(!(v1 != v2));
        assert!(v1 == v2);
    }
    #[test]
    fn test_hash_order() {
        let key = Keypair::new();
        let v1 = VersionedCrdsValue::new(
            1,
            1,
            CrdsValue::LeaderId(LeaderId {
                id: key.pubkey(),
                leader_id: Pubkey::default(),
                wallclock: 0,
            }),
        );
        let v2 = VersionedCrdsValue::new(
            1,
            1,
            CrdsValue::LeaderId(LeaderId {
                id: key.pubkey(),
                leader_id: key.pubkey(),
                wallclock: 0,
            }),
        );
        assert!(v1 != v2);
        assert!(!(v1 == v2));
        if v1 > v2 {
            assert!(v2 < v1)
        } else {
            assert!(v2 > v1)
        }
    }
    #[test]
    fn test_wallclock_order() {
        let key = Keypair::new();
        let v1 = VersionedCrdsValue::new(
            1,
            1,
            CrdsValue::LeaderId(LeaderId {
                id: key.pubkey(),
                leader_id: Pubkey::default(),
                wallclock: 1,
            }),
        );
        let v2 = VersionedCrdsValue::new(
            1,
            1,
            CrdsValue::LeaderId(LeaderId {
                id: key.pubkey(),
                leader_id: Pubkey::default(),
                wallclock: 0,
            }),
        );
        assert!(v1 > v2);
        assert!(!(v1 < v2));
        assert!(v1 != v2);
        assert!(!(v1 == v2));
    }
    #[test]
    fn test_label_order() {
        let v1 = VersionedCrdsValue::new(
            1,
            1,
            CrdsValue::LeaderId(LeaderId {
                id: Keypair::new().pubkey(),
                leader_id: Pubkey::default(),
                wallclock: 0,
            }),
        );
        let v2 = VersionedCrdsValue::new(
            1,
            1,
            CrdsValue::LeaderId(LeaderId {
                id: Keypair::new().pubkey(),
                leader_id: Pubkey::default(),
                wallclock: 0,
            }),
        );
        assert!(v1 != v2);
        assert!(!(v1 == v2));
        assert!(!(v1 < v2));
        assert!(!(v1 > v2));
        assert!(!(v2 < v1));
        assert!(!(v2 > v1));
    }
}
