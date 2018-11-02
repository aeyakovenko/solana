//! Crds Gossip Pull overlay
//! This module is used to synchronize the Crds with nodes in the network.
//! The basic strategy is as follows
//! 1. Construct a bloom filter of the local data set
//! 2. Randomly ask a node on the network for data that is is not contained in the bloom filter.
//!
//! Bloom filters have a false positive rate.  Because each filter is constructed with random hash
//! functions each subsequent request will have a different distribution of false positivies.

use bincode::serialized_size;
use bloom::Bloom;
use crds::Crds;
use crds_gossip_error::CrdsGossipError;
use crds_value::{CrdsValue, CrdsValueLabel};
use hash::Hash;
use packet::BLOB_DATA_SIZE;
use rand;
use rand::distributions::{Distribution, Weighted, WeightedChoice};
use solana_sdk::pubkey::Pubkey;
use std::cmp;
use std::collections::HashMap;
use std::collections::VecDeque;

pub struct CrdsGossipPull {
    /// timestamp of last request
    pub pull_request_time: HashMap<Pubkey, u64>,
    /// hash and insert time
    purged_values: VecDeque<(Hash, u64)>,
    /// max bytes per message
    pub max_bytes: usize,
}

impl Default for CrdsGossipPull {
    fn default() -> Self {
        Self {
            purged_values: VecDeque::new(),
            pull_request_time: HashMap::new(),
            max_bytes: BLOB_DATA_SIZE,
        }
    }
}
impl CrdsGossipPull {
    /// generate a random request
    pub fn new_pull_request(
        &self,
        crds: &Crds,
        self_id: Pubkey,
        now: u64,
    ) -> Result<(Pubkey, Bloom, CrdsValue), CrdsGossipError> {
        let mut options: Vec<_> = crds
            .table
            .values()
            .filter_map(|v| (*v).value.clone().contact_info())
            .filter(|v| {
                v.id != self_id && !v.ncp.ip().is_unspecified() && !v.ncp.ip().is_multicast()
            }).map(|item| {
                let req_time: u64 = *self.pull_request_time.get(&item.id).unwrap_or(&0);
                let weight = cmp::max(
                    1,
                    cmp::min(u32::max_value() as u64 - 1, now - req_time) as u32,
                );
                Weighted { weight, item }
            }).collect();
        if options.is_empty() {
            return Err(CrdsGossipError::NoPeers);
        }
        let filter = self.build_crds_filter(crds);
        let random = WeightedChoice::new(&mut options).sample(&mut rand::thread_rng());
        let self_info = crds.lookup(&CrdsValueLabel::ContactInfo(self_id)).unwrap();
        Ok((random.id, filter, self_info.clone()))
    }

    /// time when a request to `from` was initiated
    /// This is used for weighted random selection durring `new_pull_request`
    /// It's important to use the local nodes request creation time as the weight
    /// instaad of the response received time otherwise failed nodes will increase their weight.
    pub fn mark_pull_request_creation_time(&mut self, from: Pubkey, now: u64) {
        self.pull_request_time.insert(from, now);
    }

    /// Store an old hash in the purged values set
    pub fn record_old_hash(&mut self, hash: Hash, timestamp: u64) {
        self.purged_values.push_back((hash, timestamp))
    }

    /// process a pull request and create a response
    pub fn process_pull_request(
        &mut self,
        crds: &mut Crds,
        caller: CrdsValue,
        mut filter: Bloom,
        now: u64,
    ) -> Vec<CrdsValue> {
        let rv = self.filter_crds_values(crds, &mut filter);
        let key = caller.label().pubkey();
        let old = crds.insert(caller, now);
        old.ok().and_then(|opt| opt).map(|val| {
            self.purged_values
                .push_back((val.value_hash, val.local_timestamp))
        });
        crds.update_record_timestamp(key, now);
        rv
    }
    /// process a pull response
    pub fn process_pull_response(
        &mut self,
        crds: &mut Crds,
        from: Pubkey,
        response: Vec<CrdsValue>,
        now: u64,
    ) -> usize {
        let mut failed = 0;
        for r in response {
            let owner = r.label().pubkey();
            let old = crds.insert(r, now);
            failed += old.is_err() as usize;
            old.ok().map(|opt| {
                crds.update_record_timestamp(owner, now);
                opt.map(|val| {
                    self.purged_values
                        .push_back((val.value_hash, val.local_timestamp))
                })
            });
        }
        crds.update_record_timestamp(from, now);
        failed
    }
    /// build a filter of the current crds table
    fn build_crds_filter(&self, crds: &Crds) -> Bloom {
        let num = crds.table.values().count() + self.purged_values.len();
        let mut bloom = Bloom::random(
            num,
            Hash::default().as_ref().len() as u8,
            0.1,
            4 * 1024 * 8 - 1,
        );
        for v in crds.table.values() {
            bloom.add(v.value_hash.as_ref());
        }
        for (value_hash, _insert_timestamp) in &self.purged_values {
            bloom.add(value_hash.as_ref());
        }
        bloom
    }
    /// filter values that fail the bloom filter up to max_bytes
    fn filter_crds_values(&self, crds: &Crds, filter: &mut Bloom) -> Vec<CrdsValue> {
        let mut max_bytes = self.max_bytes as isize;
        let mut ret = vec![];
        for v in crds.table.values() {
            if filter.contains(v.value_hash.as_ref()) {
                continue;
            }
            max_bytes -= serialized_size(&v.value).unwrap() as isize;
            if max_bytes < 0 {
                break;
            }
            ret.push(v.value.clone());
        }
        ret
    }
    /// Purge values from the crds that are older then `active_timeout`
    /// The value_hash of an active item is put into self.purged_values queue
    pub fn purge_active(&mut self, crds: &mut Crds, self_id: Pubkey, min_ts: u64) {
        let old = crds.find_old_labels(min_ts);
        let mut purged: VecDeque<_> = old
            .iter()
            .filter(|label| label.pubkey() != self_id)
            .filter_map(|label| {
                crds.lookup_versioned(label)
                    .map(|val| (val.value_hash, val.local_timestamp))
            }).collect();
        old.iter().for_each(|label| crds.remove(label));
        self.purged_values.append(&mut purged);
    }
    /// Purge values from the `self.purged_values` queue that are older then purge_timeout
    pub fn purge_purged(&mut self, min_ts: u64) {
        let cnt = self
            .purged_values
            .iter()
            .take_while(|v| v.1 < min_ts)
            .count();
        self.purged_values.drain(..cnt);
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use contact_info::ContactInfo;
    use crds_value::LeaderId;
    use signature::{Keypair, KeypairUtil};

    #[test]
    fn test_new_pull_request() {
        let mut crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        let id = entry.label().pubkey();
        let node = CrdsGossipPull::default();
        assert_eq!(
            node.new_pull_request(&crds, id, 0),
            Err(CrdsGossipError::NoPeers)
        );

        crds.insert(entry.clone(), 0).unwrap();
        assert_eq!(
            node.new_pull_request(&crds, id, 0),
            Err(CrdsGossipError::NoPeers)
        );

        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        crds.insert(new.clone(), 0).unwrap();
        let req = node.new_pull_request(&crds, id, 0);
        let (to, _, self_info) = req.unwrap();
        assert_eq!(to, new.label().pubkey());
        assert_eq!(self_info, entry);
    }

    #[test]
    fn test_new_mark_creation_time() {
        let mut crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        let node_id = entry.label().pubkey();
        let mut node = CrdsGossipPull::default();
        crds.insert(entry.clone(), 0).unwrap();
        let old = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        crds.insert(old.clone(), 0).unwrap();
        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        crds.insert(new.clone(), 0).unwrap();

        // set request creation time to max_value
        node.mark_pull_request_creation_time(new.label().pubkey(), u64::max_value());

        // odds of getting the other request should be 1 in u64::max_value()
        for _ in 0..10 {
            let req = node.new_pull_request(&crds, node_id, u64::max_value());
            let (to, _, self_info) = req.unwrap();
            assert_eq!(to, old.label().pubkey());
            assert_eq!(self_info, entry);
        }
    }

    #[test]
    fn test_process_pull_request() {
        let mut node_crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        let node_id = entry.label().pubkey();
        let node = CrdsGossipPull::default();
        node_crds.insert(entry.clone(), 0).unwrap();
        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        node_crds.insert(new.clone(), 0).unwrap();
        let req = node.new_pull_request(&node_crds, node_id, 0);

        let mut dest_crds = Crds::default();
        let mut dest = CrdsGossipPull::default();
        let (_, filter, caller) = req.unwrap();
        let rsp = dest.process_pull_request(&mut dest_crds, caller.clone(), filter, 1);
        assert!(rsp.is_empty());
        assert!(dest_crds.lookup(&caller.label()).is_some());
        assert_eq!(
            dest_crds
                .lookup_versioned(&caller.label())
                .unwrap()
                .insert_timestamp,
            1
        );
        assert_eq!(
            dest_crds
                .lookup_versioned(&caller.label())
                .unwrap()
                .local_timestamp,
            1
        );
    }
    #[test]
    fn test_process_pull_request_response() {
        let mut node_crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        let node_id = entry.label().pubkey();
        let mut node = CrdsGossipPull::default();
        node_crds.insert(entry.clone(), 0).unwrap();
        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        node_crds.insert(new.clone(), 0).unwrap();

        let mut dest = CrdsGossipPull::default();
        let mut dest_crds = Crds::default();
        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        dest_crds.insert(new.clone(), 0).unwrap();

        // node contains a key from the dest node, but at an older local timestamp
        let dest_id = new.label().pubkey();
        let same_key = CrdsValue::LeaderId(LeaderId {
            id: dest_id,
            leader_id: dest_id,
            wallclock: 1,
        });
        node_crds.insert(same_key.clone(), 0).unwrap();
        assert_eq!(
            node_crds
                .lookup_versioned(&same_key.label())
                .unwrap()
                .local_timestamp,
            0
        );
        let mut done = false;
        for _ in 0..30 {
            // there is a chance of a false posititve with bloom filters
            let req = node.new_pull_request(&node_crds, node_id, 0);
            let (_, filter, caller) = req.unwrap();
            let rsp = dest.process_pull_request(&mut dest_crds, caller, filter, 0);
            // if there is a false positive this is empty
            // prob should be around 0.1 per iteration
            if rsp.is_empty() {
                continue;
            }

            assert_eq!(rsp.len(), 1);
            let failed = node.process_pull_response(&mut node_crds, node_id, rsp, 1);
            assert_eq!(failed, 0);
            assert_eq!(
                node_crds
                    .lookup_versioned(&new.label())
                    .unwrap()
                    .local_timestamp,
                1
            );
            // verify that the whole record was updated for dest since this is a response from dest
            assert_eq!(
                node_crds
                    .lookup_versioned(&same_key.label())
                    .unwrap()
                    .local_timestamp,
                1
            );
            done = true;
            break;
        }
        assert!(done);
    }
    #[test]
    fn test_gossip_purge() {
        let mut node_crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        let node_id = entry.label().pubkey();
        let mut node = CrdsGossipPull::default();
        node_crds.insert(entry.clone(), 0).unwrap();
        let old = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey()));
        node_crds.insert(old.clone(), 0).unwrap();
        let value_hash = node_crds.lookup_versioned(&old.label()).unwrap().value_hash;
        node.purge_active(&mut node_crds, node_id, 1);
        assert_eq!(node_crds.lookup_versioned(&old.label()), None);
        assert_eq!(node.purged_values.len(), 1);
        for _ in 0..30 {
            // there is a chance of a false posititve with bloom filters
            // assert that purged value is still in the set
            // chance of 30 consequtive false positives is 0.1^30
            let mut filter = node.build_crds_filter(&node_crds);
            assert!(filter.contains(value_hash.as_ref()));
        }

        // purge the value
        node.purge_purged(1);
        assert_eq!(node.purged_values.len(), 0);
    }
}
