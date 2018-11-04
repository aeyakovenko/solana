//! The `cluster_info` module defines a data structure that is shared by all the nodes in the network over
//! a gossip control plane.  The goal is to share small bits of off-chain information and detect and
//! repair partitions.
//!
//! This CRDT only supports a very limited set of types.  A map of Pubkey -> Versioned Struct.
//! The last version is always picked during an update.
//!
//! The network is arranged in layers:
//!
//! * layer 0 - Leader.
//! * layer 1 - As many nodes as we can fit
//! * layer 2 - Everyone else, if layer 1 is `2^10`, layer 2 should be able to fit `2^20` number of nodes.
//!
//! Bank needs to provide an interface for us to query the stake weight
use bincode::{deserialize, serialize};
use bloom::Bloom;
use contact_info::ContactInfo;
use counter::Counter;
use crds_gossip::CrdsGossip;
use crds_value::{CrdsValue, CrdsValueLabel, LeaderId};
use hash::Hash;
use leader_scheduler::LeaderScheduler;
use ledger::LedgerWindow;
use log::Level;
use netutil::{bind_in_range, bind_to, multi_bind_in_range};
use packet::{to_blob, Blob, SharedBlob, BLOB_SIZE};
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use result::Result;
use signature::{Keypair, KeypairUtil};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{sleep, Builder, JoinHandle};
use std::time::{Duration, Instant};
use streamer::{BlobReceiver, BlobSender};
use timing::{duration_as_ms, timestamp};
use window::{SharedWindow, WindowIndex};

pub type NodeInfo = ContactInfo;

pub const FULLNODE_PORT_RANGE: (u16, u16) = (8000, 10_000);

/// milliseconds we sleep for between gossip requests
const GOSSIP_SLEEP_MILLIS: u64 = 100;

#[derive(Debug, PartialEq, Eq)]
pub enum ClusterInfoError {
    NoPeers,
    NoLeader,
    BadContactInfo,
    BadNodeInfo,
    BadGossipAddress,
}

pub struct ClusterInfo {
    /// The network
    pub gossip: CrdsGossip,
}

// TODO These messages should be signed, and go through the gpu pipeline for spam filtering
#[derive(Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "cargo-clippy", allow(large_enum_variant))]
enum Protocol {
    /// Gosisp protocol messages
    PullRequest(Bloom<Hash>, CrdsValue),
    PullResponse(Pubkey, Vec<CrdsValue>),
    PushMessage(Pubkey, Vec<CrdsValue>),
    PruneMessage(Pubkey, Vec<Pubkey>),

    /// Window protocol messages
    /// TODO: move this message to a different module
    RequestWindowIndex(NodeInfo, u64),
}

impl ClusterInfo {
    pub fn new(node_info: NodeInfo) -> Result<ClusterInfo> {
        let mut me = ClusterInfo {
            gossip: CrdsGossip::default(),
        };
        let id = node_info.id;
        me.gossip.set_self(id);
        let entry = CrdsValue::ContactInfo(node_info);
        me.gossip.process_push_message(&[entry], timestamp());
        Ok(me)
    }
    pub fn insert_info(&mut self, node_info: NodeInfo) {
        let value = CrdsValue::ContactInfo(node_info);
        let _ = self.gossip.crds.insert(value, timestamp());
    }
    pub fn id(&self) -> Pubkey {
        self.gossip.id
    }
    pub fn lookup(&self, id: Pubkey) -> Option<&NodeInfo> {
        let entry = CrdsValueLabel::ContactInfo(id);
        self.gossip
            .crds
            .lookup(&entry)
            .and_then(|x| x.contact_info())
    }
    pub fn my_data(&self) -> NodeInfo {
        self.lookup(self.id()).map(|x| x.clone()).unwrap()
    }
    pub fn leader_id(&self) -> Pubkey {
        let entry = CrdsValueLabel::LeaderId(self.id());
        self.gossip
            .crds
            .lookup(&entry)
            .and_then(|v| v.leader_id())
            .map(|x| x.leader_id)
            .unwrap_or(Pubkey::default())
    }
    pub fn leader_data(&self) -> Option<&NodeInfo> {
        let leader_id = self.leader_id();
        if leader_id == Pubkey::default() {
            return None;
        }
        self.lookup(leader_id)
    }
    pub fn node_info_trace(&self) -> String {
        let leader_id = self.leader_id();
        let nodes: Vec<_> = self
            .gossip
            .crds
            .table
            .values()
            .filter_map(|n| n.value.contact_info())
            .filter(|n| Self::is_valid_address(&n.rpu))
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
            " NodeInfo.contact_info     | Node identifier\n\
             ---------------------------+------------------\n\
             {}\n \
             Nodes: {}",
            nodes.join(""),
            nodes.len()
        )
    }

    pub fn set_leader(&mut self, key: Pubkey) -> () {
        let prev = self.leader_id();
        let self_id = self.gossip.id;
        let now = timestamp();
        let leader = LeaderId {
            id: self_id,
            leader_id: key,
            wallclock: now,
        };
        let entry = CrdsValue::LeaderId(leader);
        warn!("{}: LEADER_UPDATE TO {} from {}", self_id, key, prev);
        self.gossip.process_push_message(&[entry], now);
    }

    pub fn purge(&mut self, now: u64) {
        self.gossip.purge(now);
    }
    pub fn rpu_peers(&self) -> Vec<NodeInfo> {
        let me = self.my_data().id;
        self.gossip
            .crds
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| x.id != me)
            .filter(|x| ClusterInfo::is_valid_address(&x.rpu))
            .cloned()
            .collect()
    }

    pub fn ncp_peers(&self) -> Vec<NodeInfo> {
        let me = self.my_data().id;
        self.gossip
            .crds
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| x.id != me)
            .filter(|x| ClusterInfo::is_valid_address(&x.ncp))
            .cloned()
            .collect()
    }

    /// compute broadcast table
    pub fn tvu_peers(&self) -> Vec<NodeInfo> {
        let me = self.my_data().id;
        self.gossip
            .crds
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| x.id != me)
            .filter(|x| ClusterInfo::is_valid_address(&x.tvu))
            .cloned()
            .collect()
    }

    /// compute broadcast table
    pub fn tpu_peers(&self) -> Vec<NodeInfo> {
        let me = self.my_data().id;
        self.gossip
            .crds
            .table
            .values()
            .filter_map(|x| x.value.contact_info())
            .filter(|x| x.id != me)
            .filter(|x| ClusterInfo::is_valid_address(&x.tpu))
            .cloned()
            .collect()
    }

    /// broadcast messages from the leader to layer 1 nodes
    /// # Remarks
    /// We need to avoid having obj locked while doing any io, such as the `send_to`
    #[cfg_attr(feature = "cargo-clippy", allow(too_many_arguments))]
    pub fn broadcast(
        leader_scheduler: &Arc<RwLock<LeaderScheduler>>,
        tick_height: u64,
        leader_id: Pubkey,
        me: &NodeInfo,
        broadcast_table: &[NodeInfo],
        window: &SharedWindow,
        s: &UdpSocket,
        transmit_index: &mut WindowIndex,
        received_index: u64,
    ) -> Result<()> {
        if broadcast_table.is_empty() {
            debug!("{}:not enough peers in cluster_info table", me.id);
            inc_new_counter_info!("cluster_info-broadcast-not_enough_peers_error", 1);
            Err(ClusterInfoError::NoPeers)?;
        }
        trace!(
            "{} transmit_index: {:?} received_index: {} broadcast_len: {}",
            me.id,
            *transmit_index,
            received_index,
            broadcast_table.len()
        );

        let old_transmit_index = transmit_index.data;

        // enumerate all the blobs in the window, those are the indices
        // transmit them to nodes, starting from a different node. Add one
        // to the capacity in case we want to send an extra blob notifying the
        // next leader about the blob right before leader rotation
        let mut orders = Vec::with_capacity((received_index - transmit_index.data + 1) as usize);
        let window_l = window.read().unwrap();

        let mut br_idx = transmit_index.data as usize % broadcast_table.len();

        for idx in transmit_index.data..received_index {
            let w_idx = idx as usize % window_l.len();

            trace!(
                "{} broadcast order data w_idx {} br_idx {}",
                me.id,
                w_idx,
                br_idx
            );

            // Make sure the next leader in line knows about the entries before his slot in the leader
            // rotation so they can initiate repairs if necessary
            {
                let ls_lock = leader_scheduler.read().unwrap();
                let next_leader_height = ls_lock.max_height_for_leader(tick_height);
                let next_leader_id =
                    next_leader_height.map(|nlh| ls_lock.get_scheduled_leader(nlh));
                // In the case the next scheduled leader is None, then the write_stage moved
                // the schedule too far ahead and we no longer are in the known window
                // (will happen during calculation of the next set of slots every epoch or
                // seed_rotation_interval heights when we move the window forward in the
                // LeaderScheduler). For correctness, this is fine write_stage will never send
                // blobs past the point of when this node should stop being leader, so we just
                // continue broadcasting until we catch up to write_stage. The downside is we
                // can't guarantee the current leader will broadcast the last entry to the next
                // scheduled leader, so the next leader will have to rely on avalanche/repairs
                // to get this last blob, which could cause slowdowns during leader handoffs.
                // See corresponding issue for repairs in repair() function in window.rs.
                if let Some(Some(next_leader_id)) = next_leader_id {
                    if next_leader_id == me.id {
                        break;
                    }
                    let info_result = broadcast_table.iter().position(|n| n.id == next_leader_id);
                    if let Some(index) = info_result {
                        orders.push((window_l[w_idx].data.clone(), &broadcast_table[index]));
                    }
                }
            }

            orders.push((window_l[w_idx].data.clone(), &broadcast_table[br_idx]));
            br_idx += 1;
            br_idx %= broadcast_table.len();
        }

        for idx in transmit_index.coding..received_index {
            let w_idx = idx as usize % window_l.len();

            // skip over empty slots
            if window_l[w_idx].coding.is_none() {
                continue;
            }

            trace!(
                "{} broadcast order coding w_idx: {} br_idx  :{}",
                me.id,
                w_idx,
                br_idx,
            );

            orders.push((window_l[w_idx].coding.clone(), &broadcast_table[br_idx]));
            br_idx += 1;
            br_idx %= broadcast_table.len();
        }

        trace!("broadcast orders table {}", orders.len());
        let errs: Vec<_> = orders
            .into_iter()
            .map(|(b, v)| {
                // only leader should be broadcasting
                assert!(leader_id != v.id);
                let bl = b.unwrap();
                let blob = bl.read().unwrap();
                //TODO profile this, may need multiple sockets for par_iter
                trace!(
                    "{}: BROADCAST idx: {} sz: {} to {},{} coding: {}",
                    me.id,
                    blob.get_index().unwrap(),
                    blob.meta.size,
                    v.id,
                    v.tvu,
                    blob.is_coding()
                );
                assert!(blob.meta.size <= BLOB_SIZE);
                let e = s.send_to(&blob.data[..blob.meta.size], &v.tvu);
                trace!(
                    "{}: done broadcast {} to {} {}",
                    me.id,
                    blob.meta.size,
                    v.id,
                    v.tvu
                );
                e
            }).collect();

        trace!("broadcast results {}", errs.len());
        for e in errs {
            if let Err(e) = &e {
                trace!("broadcast result {:?}", e);
            }
            e?;
            if transmit_index.data < received_index {
                transmit_index.data += 1;
            }
        }
        inc_new_counter_info!(
            "cluster_info-broadcast-max_idx",
            (transmit_index.data - old_transmit_index) as usize
        );
        transmit_index.coding = transmit_index.data;

        Ok(())
    }

    /// retransmit messages from the leader to layer 1 nodes
    /// # Remarks
    /// We need to avoid having obj locked while doing any io, such as the `send_to`
    pub fn retransmit(obj: &Arc<RwLock<Self>>, blob: &SharedBlob, s: &UdpSocket) -> Result<()> {
        let (me, orders): (NodeInfo, Vec<NodeInfo>) = {
            // copy to avoid locking during IO
            let s = obj.read().expect("'obj' read lock in pub fn retransmit");
            (s.my_data().clone(), s.tvu_peers())
        };
        blob.write()
            .unwrap()
            .set_id(me.id)
            .expect("set_id in pub fn retransmit");
        let rblob = blob.read().unwrap();
        trace!("retransmit orders {}", orders.len());
        let errs: Vec<_> = orders
            .par_iter()
            .map(|v| {
                debug!(
                    "{}: retransmit blob {} to {} {}",
                    me.id,
                    rblob.get_index().unwrap(),
                    v.id,
                    v.tvu,
                );
                //TODO profile this, may need multiple sockets for par_iter
                assert!(rblob.meta.size <= BLOB_SIZE);
                s.send_to(&rblob.data[..rblob.meta.size], &v.tvu)
            }).collect();
        for e in errs {
            if let Err(e) = &e {
                inc_new_counter_info!("cluster_info-retransmit-send_to_error", 1, 1);
                error!("retransmit result {:?}", e);
            }
            e?;
        }
        Ok(())
    }

    pub fn convergence(&self) -> u64 {
        self.gossip.crds.table.len() as u64
    }

    pub fn window_index_request(&self, ix: u64) -> Result<(SocketAddr, Vec<u8>)> {
        // find a peer that appears to be accepting replication, as indicated
        //  by a valid tvu port location
        let valid: Vec<_> = self.tvu_peers();
        if valid.is_empty() {
            Err(ClusterInfoError::NoPeers)?;
        }
        let n = thread_rng().gen::<usize>() % valid.len();
        let addr = valid[n].ncp; // send the request to the peer's gossip port
        let req = Protocol::RequestWindowIndex(self.my_data().clone(), ix);
        let out = serialize(&req)?;
        Ok((addr, out))
    }
    fn gossip_pull_requests(&mut self) -> Vec<(SocketAddr, Protocol)> {
        let now = timestamp();
        let pulls: Vec<_> = self.gossip.new_pull_request(now).ok().into_iter().collect();

        let pr: Vec<_> = pulls
            .into_iter()
            .filter_map(|(peer, filter, self_info)| {
                let peer_label = CrdsValueLabel::ContactInfo(peer);
                self.gossip
                    .crds
                    .lookup(&peer_label)
                    .and_then(|v| v.contact_info())
                    .map(|peer_info| (peer, filter, peer_info.ncp, self_info))
            }).collect();
        pr.into_iter()
            .map(|(peer, filter, ncp, self_info)| {
                self.gossip.mark_pull_request_creation_time(peer, now);
                (ncp, Protocol::PullRequest(filter, self_info))
            }).collect()
    }
    fn gossip_request(&mut self) -> Vec<(SocketAddr, Protocol)> {
        let self_id = self.gossip.id;
        let pulls: Vec<_> = self.gossip_pull_requests();
        let pushes: Vec<_> = {
            let (_, peers, msgs) = self.gossip.new_push_messages(timestamp());
            peers
                .into_iter()
                .filter_map(|p| {
                    let peer_label = CrdsValueLabel::ContactInfo(p);
                    self.gossip
                        .crds
                        .lookup(&peer_label)
                        .and_then(|v| v.contact_info())
                        .map(|p| p.ncp)
                }).map(|peer| (peer, Protocol::PushMessage(self_id, msgs.clone())))
                .collect()
        };
        vec![pulls, pushes].into_iter().flat_map(|x| x).collect()
    }

    /// At random pick a node and try to get updated changes from them
    fn run_gossip(obj: &Arc<RwLock<Self>>, blob_sender: &BlobSender) -> Result<()> {
        let reqs = obj.write().unwrap().gossip_request();
        let blobs = reqs
            .into_iter()
            .filter_map(|(remote_gossip_addr, req)| to_blob(req, remote_gossip_addr).ok())
            .collect();
        blob_sender.send(blobs)?;
        Ok(())
    }

    pub fn get_gossip_top_leader(&self) -> Option<&NodeInfo> {
        let mut table = HashMap::new();
        let def = Pubkey::default();
        let cur = self
            .gossip
            .crds
            .table
            .values()
            .filter_map(|x| x.value.leader_id())
            .filter(|x| x.leader_id != def);
        for v in cur {
            let cnt = table.entry(&v.leader_id).or_insert(0);
            *cnt += 1;
            trace!("leader {} {}", v.leader_id, *cnt);
        }
        let mut sorted: Vec<(&Pubkey, usize)> = table.into_iter().collect();
        for x in &sorted {
            trace!("{}: sorted leaders {} votes: {}", self.gossip.id, x.0, x.1);
        }
        sorted.sort_by_key(|a| a.1);
        let top_leader = sorted.last().map(|a| *a.0);

        top_leader
            .and_then(|x| {
                let leader_label = CrdsValueLabel::ContactInfo(x);
                self.gossip.crds.lookup(&leader_label)
            }).and_then(|x| x.contact_info())
    }

    /// randomly pick a node and ask them for updates asynchronously
    pub fn gossip(
        obj: Arc<RwLock<Self>>,
        blob_sender: BlobSender,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("solana-gossip".to_string())
            .spawn(move || loop {
                let start = timestamp();
                let _ = Self::run_gossip(&obj, &blob_sender);
                if exit.load(Ordering::Relaxed) {
                    return;
                }
                obj.write().unwrap().purge(timestamp());
                //TODO: possibly tune this parameter
                //we saw a deadlock passing an obj.read().unwrap().timeout into sleep
                let elapsed = timestamp() - start;
                if GOSSIP_SLEEP_MILLIS > elapsed {
                    let time_left = GOSSIP_SLEEP_MILLIS - elapsed;
                    sleep(Duration::from_millis(time_left));
                }
            }).unwrap()
    }
    fn run_window_request(
        from: &NodeInfo,
        from_addr: &SocketAddr,
        window: &SharedWindow,
        ledger_window: &mut Option<&mut LedgerWindow>,
        me: &NodeInfo,
        leader_id: Pubkey,
        ix: u64,
    ) -> Option<SharedBlob> {
        let pos = (ix as usize) % window.read().unwrap().len();
        if let Some(ref mut blob) = &mut window.write().unwrap()[pos].data {
            let mut wblob = blob.write().unwrap();
            let blob_ix = wblob.get_index().expect("run_window_request get_index");
            if blob_ix == ix {
                let num_retransmits = wblob.meta.num_retransmits;
                wblob.meta.num_retransmits += 1;
                // Setting the sender id to the requester id
                // prevents the requester from retransmitting this response
                // to other peers
                let mut sender_id = from.id;

                // Allow retransmission of this response if the node
                // is the leader and the number of repair requests equals
                // a power of two
                if leader_id == me.id && (num_retransmits == 0 || num_retransmits.is_power_of_two())
                {
                    sender_id = me.id
                }

                let out = SharedBlob::default();

                // copy to avoid doing IO inside the lock
                {
                    let mut outblob = out.write().unwrap();
                    let sz = wblob.meta.size;
                    outblob.meta.size = sz;
                    outblob.data[..sz].copy_from_slice(&wblob.data[..sz]);
                    outblob.meta.set_addr(from_addr);
                    outblob.set_id(sender_id).expect("blob set_id");
                }
                inc_new_counter_info!("cluster_info-window-request-pass", 1);

                return Some(out);
            } else {
                inc_new_counter_info!("cluster_info-window-request-outside", 1);
                trace!(
                    "requested ix {} != blob_ix {}, outside window!",
                    ix,
                    blob_ix
                );
                // falls through to checking window_ledger
            }
        }

        if let Some(ledger_window) = ledger_window {
            if let Ok(entry) = ledger_window.get_entry(ix) {
                inc_new_counter_info!("cluster_info-window-request-ledger", 1);

                let out = entry.to_blob(
                    Some(ix),
                    Some(me.id), // causes retransmission if I'm the leader
                    Some(from_addr),
                );

                return Some(out);
            }
        }

        inc_new_counter_info!("cluster_info-window-request-fail", 1);
        trace!(
            "{}: failed RequestWindowIndex {} {} {}",
            me.id,
            from.id,
            ix,
            pos,
        );

        None
    }

    //TODO we should first coalesce all the requests
    fn handle_blob(
        obj: &Arc<RwLock<Self>>,
        window: &SharedWindow,
        ledger_window: &mut Option<&mut LedgerWindow>,
        blob: &Blob,
    ) -> Option<SharedBlob> {
        match deserialize(&blob.data[..blob.meta.size]) {
            Ok(request) => {
                ClusterInfo::handle_protocol(obj, &blob.meta.addr(), request, window, ledger_window)
            }
            Err(_) => {
                warn!("deserialize cluster_info packet failed");
                None
            }
        }
    }

    fn handle_protocol(
        me: &Arc<RwLock<Self>>,
        from_addr: &SocketAddr,
        request: Protocol,
        window: &SharedWindow,
        ledger_window: &mut Option<&mut LedgerWindow>,
    ) -> Option<SharedBlob> {
        let self_id = me.read().unwrap().gossip.id;
        match request {
            // TODO sigverify these
            Protocol::PullRequest(filter, caller) => {
                inc_new_counter_info!("cluster_info-pull_request", 1);
                if caller.contact_info().is_none() {
                    return None;
                }
                let mut from = caller.contact_info().cloned().unwrap();
                if from.id == self_id {
                    warn!(
                        "PullRequest ignored, I'm talking to myself: me={} remoteme={}",
                        self_id, from.id
                    );
                    inc_new_counter_info!("cluster_info-window-request-loopback", 1);
                    return None;
                }
                let now = timestamp();
                let data = me
                    .write()
                    .unwrap()
                    .gossip
                    .process_pull_request(caller, filter, now);
                let len = data.len();
                trace!("get updates since response {}", len);
                if data.is_empty() {
                    trace!("no updates me {}", self_id);
                    None
                } else {
                    let rsp = Protocol::PullResponse(self_id, data);
                    // the remote side may not know his public IP:PORT, record what he looks like to us
                    //  this may or may not be correct for everybody but it's better than leaving him with
                    //  an unspecified address in our table
                    if from.ncp.ip().is_unspecified() {
                        inc_new_counter_info!("cluster_info-window-request-updates-unspec-ncp", 1);
                        from.ncp = *from_addr;
                    }
                    inc_new_counter_info!("cluster_info-pull_request-rsp", len);
                    to_blob(rsp, from.ncp).ok()
                }
            }
            Protocol::PullResponse(from, data) => {
                let len = data.len();
                let now = Instant::now();
                trace!("PullResponse me: {} len={}", self_id, len);
                me.write()
                    .unwrap()
                    .gossip
                    .process_pull_response(from, data, timestamp());
                inc_new_counter_info!("cluster_info-pull_request_response", 1);
                inc_new_counter_info!("cluster_info-pull_request_response-size", len);

                report_time_spent("ReceiveUpdates", &now.elapsed(), &format!(" len: {}", len));
                None
            }
            Protocol::PushMessage(from, data) => {
                inc_new_counter_info!("cluster_info-push_message", 1);
                let prunes: Vec<_> = me
                    .write()
                    .unwrap()
                    .gossip
                    .process_push_message(&data, timestamp());
                if prunes.len() > 0 {
                    inc_new_counter_info!("cluster_info-push_message-rsp", prunes.len());
                    let rsp = Protocol::PruneMessage(self_id, prunes);
                    let from_label = CrdsValueLabel::ContactInfo(from);
                    let ci = me
                        .read()
                        .unwrap()
                        .gossip
                        .crds
                        .lookup(&from_label)
                        .and_then(|val| val.contact_info().cloned());
                    ci.and_then(|ci| to_blob(rsp, ci.ncp).ok())
                } else {
                    None
                }
            }
            Protocol::PruneMessage(from, data) => {
                inc_new_counter_info!("cluster_info-prune_message", 1);
                inc_new_counter_info!("cluster_info-prune_message-size", data.len());
                me.write().unwrap().gossip.process_prune_msg(from, &data);
                None
            }
            Protocol::RequestWindowIndex(from, ix) => {
                let now = Instant::now();

                //TODO this doesn't depend on cluster_info module, could be moved
                //but we are using the listen thread to service these request
                //TODO verify from is signed

                let self_id = me.read().unwrap().gossip.id;
                if from.id == me.read().unwrap().gossip.id {
                    warn!(
                        "{}: Ignored received RequestWindowIndex from ME {} {} ",
                        self_id, from.id, ix,
                    );
                    inc_new_counter_info!("cluster_info-window-request-address-eq", 1);
                    return None;
                }

                me.write().unwrap().insert_info(from.clone());
                let leader_id = me.read().unwrap().leader_id();
                let my_info = me.read().unwrap().my_data().clone();
                inc_new_counter_info!("cluster_info-window-request-recv", 1);
                trace!(
                    "{}: received RequestWindowIndex {} {} ",
                    self_id,
                    from.id,
                    ix,
                );
                let res = Self::run_window_request(
                    &from,
                    &from_addr,
                    &window,
                    ledger_window,
                    &my_info,
                    leader_id,
                    ix,
                );
                report_time_spent(
                    "RequestWindowIndex",
                    &now.elapsed(),
                    &format!(" ix: {}", ix),
                );
                res
            }
        }
    }

    /// Process messages from the network
    fn run_listen(
        obj: &Arc<RwLock<Self>>,
        window: &SharedWindow,
        ledger_window: &mut Option<&mut LedgerWindow>,
        requests_receiver: &BlobReceiver,
        response_sender: &BlobSender,
    ) -> Result<()> {
        //TODO cache connections
        let timeout = Duration::new(1, 0);
        let mut reqs = requests_receiver.recv_timeout(timeout)?;
        while let Ok(mut more) = requests_receiver.try_recv() {
            reqs.append(&mut more);
        }
        let mut resps = Vec::new();
        for req in reqs {
            if let Some(resp) = Self::handle_blob(obj, window, ledger_window, &req.read().unwrap())
            {
                resps.push(resp);
            }
        }
        response_sender.send(resps)?;
        Ok(())
    }
    pub fn listen(
        me: Arc<RwLock<Self>>,
        window: SharedWindow,
        ledger_path: Option<&str>,
        requests_receiver: BlobReceiver,
        response_sender: BlobSender,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let mut ledger_window = ledger_path.map(|p| LedgerWindow::open(p).unwrap());

        Builder::new()
            .name("solana-listen".to_string())
            .spawn(move || loop {
                let e = Self::run_listen(
                    &me,
                    &window,
                    &mut ledger_window.as_mut(),
                    &requests_receiver,
                    &response_sender,
                );
                if exit.load(Ordering::Relaxed) {
                    return;
                }
                if e.is_err() {
                    let me = me.read().unwrap();
                    debug!(
                        "{}: run_listen timeout, table size: {}",
                        me.gossip.id,
                        me.gossip.crds.table.len()
                    );
                }
            }).unwrap()
    }

    fn is_valid_ip(addr: IpAddr) -> bool {
        !(addr.is_unspecified() || addr.is_multicast())
        // || (addr.is_loopback() && !cfg_test))
        // TODO: boot loopback in production networks
    }
    /// port must not be 0
    /// ip must be specified and not mulitcast
    /// loopback ip is only allowed in tests
    pub fn is_valid_address(addr: &SocketAddr) -> bool {
        (addr.port() != 0) && Self::is_valid_ip(addr.ip())
    }

    pub fn spy_node() -> (NodeInfo, UdpSocket) {
        let (_, gossip_socket) = bind_in_range(FULLNODE_PORT_RANGE).unwrap();
        let pubkey = Keypair::new().pubkey();
        let daddr = socketaddr_any!();

        let node = NodeInfo::new(pubkey, daddr, daddr, daddr, daddr, daddr, timestamp());
        (node, gossip_socket)
    }
}

#[derive(Debug)]
pub struct Sockets {
    pub gossip: UdpSocket,
    pub requests: UdpSocket,
    pub replicate: Vec<UdpSocket>,
    pub transaction: Vec<UdpSocket>,
    pub respond: UdpSocket,
    pub broadcast: UdpSocket,
    pub repair: UdpSocket,
    pub retransmit: UdpSocket,
}

#[derive(Debug)]
pub struct Node {
    pub info: NodeInfo,
    pub sockets: Sockets,
}

impl Node {
    pub fn new_localhost() -> Self {
        let pubkey = Keypair::new().pubkey();
        Self::new_localhost_with_pubkey(pubkey)
    }
    pub fn new_localhost_with_pubkey(pubkey: Pubkey) -> Self {
        let transaction = UdpSocket::bind("127.0.0.1:0").unwrap();
        let gossip = UdpSocket::bind("127.0.0.1:0").unwrap();
        let replicate = UdpSocket::bind("127.0.0.1:0").unwrap();
        let requests = UdpSocket::bind("127.0.0.1:0").unwrap();
        let repair = UdpSocket::bind("127.0.0.1:0").unwrap();

        let respond = UdpSocket::bind("0.0.0.0:0").unwrap();
        let broadcast = UdpSocket::bind("0.0.0.0:0").unwrap();
        let retransmit = UdpSocket::bind("0.0.0.0:0").unwrap();
        let storage = UdpSocket::bind("0.0.0.0:0").unwrap();
        let info = NodeInfo::new(
            pubkey,
            gossip.local_addr().unwrap(),
            replicate.local_addr().unwrap(),
            requests.local_addr().unwrap(),
            transaction.local_addr().unwrap(),
            storage.local_addr().unwrap(),
            timestamp(),
        );
        Node {
            info,
            sockets: Sockets {
                gossip,
                requests,
                replicate: vec![replicate],
                transaction: vec![transaction],
                respond,
                broadcast,
                repair,
                retransmit,
            },
        }
    }
    pub fn new_with_external_ip(pubkey: Pubkey, ncp: &SocketAddr) -> Node {
        fn bind() -> (u16, UdpSocket) {
            bind_in_range(FULLNODE_PORT_RANGE).expect("Failed to bind")
        };

        let (gossip_port, gossip) = if ncp.port() != 0 {
            (ncp.port(), bind_to(ncp.port(), false).expect("ncp bind"))
        } else {
            bind()
        };

        let (replicate_port, replicate_sockets) =
            multi_bind_in_range(FULLNODE_PORT_RANGE, 8).expect("tvu multi_bind");

        let (requests_port, requests) = bind();

        let (transaction_port, transaction_sockets) =
            multi_bind_in_range(FULLNODE_PORT_RANGE, 32).expect("tpu multi_bind");

        let (_, repair) = bind();
        let (_, broadcast) = bind();
        let (_, retransmit) = bind();
        let (storage_port, _) = bind();

        // Responses are sent from the same Udp port as requests are received
        // from, in hopes that a NAT sitting in the middle will route the
        // response Udp packet correctly back to the requester.
        let respond = requests.try_clone().unwrap();

        let info = NodeInfo::new(
            pubkey,
            SocketAddr::new(ncp.ip(), gossip_port),
            SocketAddr::new(ncp.ip(), replicate_port),
            SocketAddr::new(ncp.ip(), requests_port),
            SocketAddr::new(ncp.ip(), transaction_port),
            SocketAddr::new(ncp.ip(), storage_port),
            0,
        );
        trace!("new NodeInfo: {:?}", info);

        Node {
            info,
            sockets: Sockets {
                gossip,
                requests,
                replicate: replicate_sockets,
                transaction: transaction_sockets,
                respond,
                broadcast,
                repair,
                retransmit,
            },
        }
    }
}

fn report_time_spent(label: &str, time: &Duration, extra: &str) {
    let count = duration_as_ms(time);
    if count > 5 {
        info!("{} took: {} ms {}", label, count, extra);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crds_value::CrdsValueLabel;
    use entry::Entry;
    use hash::{hash, Hash};
    use ledger::{get_tmp_ledger_path, LedgerWindow, LedgerWriter};
    use logger;
    use packet::SharedBlob;
    use result::Error;
    use signature::{Keypair, KeypairUtil};
    use std::fs::remove_dir_all;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, RwLock};
    use window::default_window;

    #[test]
    fn test_cluster_info_new() {
        let d = NodeInfo::new_localhost(Keypair::new().pubkey(), timestamp());
        let cluster_info = ClusterInfo::new(d.clone()).expect("ClusterInfo::new");
        assert_eq!(d, cluster_info.my_data());
    }

    #[test]
    fn insert_info_test() {
        let d = NodeInfo::new_localhost(Keypair::new().pubkey(), timestamp());
        let mut cluster_info = ClusterInfo::new(d).expect("ClusterInfo::new");
        let d = NodeInfo::new_localhost(Keypair::new().pubkey(), timestamp());
        let label = CrdsValueLabel::ContactInfo(d.id);
        cluster_info.insert_info(d);
        assert!(cluster_info.gossip.crds.lookup(&label).is_some());
    }
    #[test]
    fn window_index_request() {
        let me = NodeInfo::new_localhost(Keypair::new().pubkey(), timestamp());
        let mut cluster_info = ClusterInfo::new(me).expect("ClusterInfo::new");
        let rv = cluster_info.window_index_request(0);
        assert_matches!(rv, Err(Error::ClusterInfoError(ClusterInfoError::NoPeers)));

        let ncp = socketaddr!([127, 0, 0, 1], 1234);
        let nxt = NodeInfo::new(
            Keypair::new().pubkey(),
            ncp,
            socketaddr!([127, 0, 0, 1], 1235),
            socketaddr!([127, 0, 0, 1], 1236),
            socketaddr!([127, 0, 0, 1], 1237),
            socketaddr!([127, 0, 0, 1], 1238),
            0,
        );
        cluster_info.insert_info(nxt.clone());
        let rv = cluster_info.window_index_request(0).unwrap();
        assert_eq!(nxt.ncp, ncp);
        assert_eq!(rv.0, nxt.ncp);

        let ncp2 = socketaddr!([127, 0, 0, 2], 1234);
        let nxt = NodeInfo::new(
            Keypair::new().pubkey(),
            ncp2,
            socketaddr!([127, 0, 0, 1], 1235),
            socketaddr!([127, 0, 0, 1], 1236),
            socketaddr!([127, 0, 0, 1], 1237),
            socketaddr!([127, 0, 0, 1], 1238),
            0,
        );
        cluster_info.insert_info(nxt);
        let mut one = false;
        let mut two = false;
        while !one || !two {
            //this randomly picks an option, so eventually it should pick both
            let rv = cluster_info.window_index_request(0).unwrap();
            if rv.0 == ncp {
                one = true;
            }
            if rv.0 == ncp2 {
                two = true;
            }
        }
        assert!(one && two);
    }

    /// test window requests respond with the right blob, and do not overrun
    #[test]
    fn run_window_request() {
        logger::setup();
        let window = Arc::new(RwLock::new(default_window()));
        let me = NodeInfo::new(
            Keypair::new().pubkey(),
            socketaddr!("127.0.0.1:1234"),
            socketaddr!("127.0.0.1:1235"),
            socketaddr!("127.0.0.1:1236"),
            socketaddr!("127.0.0.1:1237"),
            socketaddr!("127.0.0.1:1238"),
            0,
        );
        let leader_id = me.id;
        let rv = ClusterInfo::run_window_request(
            &me,
            &socketaddr_any!(),
            &window,
            &mut None,
            &me,
            leader_id,
            0,
        );
        assert!(rv.is_none());
        let out = SharedBlob::default();
        out.write().unwrap().meta.size = 200;
        window.write().unwrap()[0].data = Some(out);
        let rv = ClusterInfo::run_window_request(
            &me,
            &socketaddr_any!(),
            &window,
            &mut None,
            &me,
            leader_id,
            0,
        );
        assert!(rv.is_some());
        let v = rv.unwrap();
        //test we copied the blob
        assert_eq!(v.read().unwrap().meta.size, 200);
        let len = window.read().unwrap().len() as u64;
        let rv = ClusterInfo::run_window_request(
            &me,
            &socketaddr_any!(),
            &window,
            &mut None,
            &me,
            leader_id,
            len,
        );
        assert!(rv.is_none());

        fn tmp_ledger(name: &str) -> String {
            let path = get_tmp_ledger_path(name);

            let mut writer = LedgerWriter::open(&path, true).unwrap();
            let zero = Hash::default();
            let one = hash(&zero.as_ref());
            writer
                .write_entries(&vec![Entry::new_tick(0, &zero), Entry::new_tick(0, &one)].to_vec())
                .unwrap();
            path
        }

        let ledger_path = tmp_ledger("run_window_request");
        let mut ledger_window = LedgerWindow::open(&ledger_path).unwrap();

        let rv = ClusterInfo::run_window_request(
            &me,
            &socketaddr_any!(),
            &window,
            &mut Some(&mut ledger_window),
            &me,
            leader_id,
            1,
        );
        assert!(rv.is_some());

        remove_dir_all(ledger_path).unwrap();
    }

    /// test window requests respond with the right blob, and do not overrun
    #[test]
    fn run_window_request_with_backoff() {
        let window = Arc::new(RwLock::new(default_window()));

        let me = NodeInfo::new_with_socketaddr(&socketaddr!("127.0.0.1:1234"));
        let leader_id = me.id;

        let mock_peer = NodeInfo::new_with_socketaddr(&socketaddr!("127.0.0.1:1234"));

        // Simulate handling a repair request from mock_peer
        let rv = ClusterInfo::run_window_request(
            &mock_peer,
            &socketaddr_any!(),
            &window,
            &mut None,
            &me,
            leader_id,
            0,
        );
        assert!(rv.is_none());
        let blob = SharedBlob::default();
        let blob_size = 200;
        blob.write().unwrap().meta.size = blob_size;
        window.write().unwrap()[0].data = Some(blob);

        let num_requests: u32 = 64;
        for i in 0..num_requests {
            let shared_blob = ClusterInfo::run_window_request(
                &mock_peer,
                &socketaddr_any!(),
                &window,
                &mut None,
                &me,
                leader_id,
                0,
            ).unwrap();
            let blob = shared_blob.read().unwrap();
            // Test we copied the blob
            assert_eq!(blob.meta.size, blob_size);

            let id = if i == 0 || i.is_power_of_two() {
                me.id
            } else {
                mock_peer.id
            };
            assert_eq!(blob.get_id().unwrap(), id);
        }
    }

    #[test]
    fn test_default_leader() {
        logger::setup();
        let node_info = NodeInfo::new_localhost(Keypair::new().pubkey(), 0);
        let mut cluster_info = ClusterInfo::new(node_info).unwrap();
        let network_entry_point = NodeInfo::new_entry_point(&socketaddr!("127.0.0.1:1239"));
        cluster_info.insert_info(network_entry_point);
        assert!(cluster_info.leader_data().is_none());
    }

    #[test]
    fn new_with_external_ip_test_random() {
        let ip = Ipv4Addr::from(0);
        let node = Node::new_with_external_ip(Keypair::new().pubkey(), &socketaddr!(ip, 0));
        assert_eq!(node.sockets.gossip.local_addr().unwrap().ip(), ip);
        assert!(node.sockets.replicate.len() > 1);
        for tx_socket in node.sockets.replicate.iter() {
            assert_eq!(tx_socket.local_addr().unwrap().ip(), ip);
        }
        assert_eq!(node.sockets.requests.local_addr().unwrap().ip(), ip);
        assert!(node.sockets.transaction.len() > 1);
        for tx_socket in node.sockets.transaction.iter() {
            assert_eq!(tx_socket.local_addr().unwrap().ip(), ip);
        }
        assert_eq!(node.sockets.repair.local_addr().unwrap().ip(), ip);

        assert!(node.sockets.gossip.local_addr().unwrap().port() >= FULLNODE_PORT_RANGE.0);
        assert!(node.sockets.gossip.local_addr().unwrap().port() < FULLNODE_PORT_RANGE.1);
        let tx_port = node.sockets.replicate[0].local_addr().unwrap().port();
        assert!(tx_port >= FULLNODE_PORT_RANGE.0);
        assert!(tx_port < FULLNODE_PORT_RANGE.1);
        for tx_socket in node.sockets.replicate.iter() {
            assert_eq!(tx_socket.local_addr().unwrap().port(), tx_port);
        }
        assert!(node.sockets.requests.local_addr().unwrap().port() >= FULLNODE_PORT_RANGE.0);
        assert!(node.sockets.requests.local_addr().unwrap().port() < FULLNODE_PORT_RANGE.1);
        let tx_port = node.sockets.transaction[0].local_addr().unwrap().port();
        assert!(tx_port >= FULLNODE_PORT_RANGE.0);
        assert!(tx_port < FULLNODE_PORT_RANGE.1);
        for tx_socket in node.sockets.transaction.iter() {
            assert_eq!(tx_socket.local_addr().unwrap().port(), tx_port);
        }
        assert!(node.sockets.repair.local_addr().unwrap().port() >= FULLNODE_PORT_RANGE.0);
        assert!(node.sockets.repair.local_addr().unwrap().port() < FULLNODE_PORT_RANGE.1);
    }

    #[test]
    fn new_with_external_ip_test_gossip() {
        let ip = IpAddr::V4(Ipv4Addr::from(0));
        let node = Node::new_with_external_ip(Keypair::new().pubkey(), &socketaddr!(0, 8050));
        assert_eq!(node.sockets.gossip.local_addr().unwrap().ip(), ip);
        assert!(node.sockets.replicate.len() > 1);
        for tx_socket in node.sockets.replicate.iter() {
            assert_eq!(tx_socket.local_addr().unwrap().ip(), ip);
        }
        assert_eq!(node.sockets.requests.local_addr().unwrap().ip(), ip);
        assert!(node.sockets.transaction.len() > 1);
        for tx_socket in node.sockets.transaction.iter() {
            assert_eq!(tx_socket.local_addr().unwrap().ip(), ip);
        }
        assert_eq!(node.sockets.repair.local_addr().unwrap().ip(), ip);

        assert_eq!(node.sockets.gossip.local_addr().unwrap().port(), 8050);
        let tx_port = node.sockets.replicate[0].local_addr().unwrap().port();
        assert!(tx_port >= FULLNODE_PORT_RANGE.0);
        assert!(tx_port < FULLNODE_PORT_RANGE.1);
        for tx_socket in node.sockets.replicate.iter() {
            assert_eq!(tx_socket.local_addr().unwrap().port(), tx_port);
        }
        assert!(node.sockets.requests.local_addr().unwrap().port() >= FULLNODE_PORT_RANGE.0);
        assert!(node.sockets.requests.local_addr().unwrap().port() < FULLNODE_PORT_RANGE.1);
        let tx_port = node.sockets.transaction[0].local_addr().unwrap().port();
        assert!(tx_port >= FULLNODE_PORT_RANGE.0);
        assert!(tx_port < FULLNODE_PORT_RANGE.1);
        for tx_socket in node.sockets.transaction.iter() {
            assert_eq!(tx_socket.local_addr().unwrap().port(), tx_port);
        }
        assert!(node.sockets.repair.local_addr().unwrap().port() >= FULLNODE_PORT_RANGE.0);
        assert!(node.sockets.repair.local_addr().unwrap().port() < FULLNODE_PORT_RANGE.1);
    }
}
