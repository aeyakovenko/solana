//! The `vote_stage` votes on the `last_id` of the bank at a regular cadence

use bank::Bank;
use bincode::serialize;
use cluster_info::ClusterInfo;
use counter::Counter;
use hash::Hash;
use log::Level;
use packet::SharedBlob;
use result::{Error, Result};
use signature::Keypair;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use streamer::BlobSender;
use transaction::Transaction;
use vote_program::Vote;
use vote_transaction::VoteTransaction;

pub const VOTE_TIMEOUT_MS: u64 = 1000;

#[derive(Debug, PartialEq, Eq)]
pub enum VoteError {
    NoValidLastIdsToVoteOn,
    NoLeader,
    LeaderInfoNotFound,
}

// TODO: Change voting to be on fixed tick intervals based on bank state
pub fn create_new_signed_vote_blob(
    last_id: &Hash,
    vote_account: &Keypair,
    bank: &Arc<Bank>,
    cluster_info: &Arc<RwLock<ClusterInfo>>,
) -> Result<SharedBlob> {
    let shared_blob = SharedBlob::default();
    let tick_height = bank.get_tick_height();

    let leader_tpu = get_leader_tpu(cluster_info)?;
    //TODO: doesn't seem like there is a synchronous call to get height and id
    debug!("voting on {:?}", &last_id.as_ref()[..8]);
    let vote = Vote { tick_height };
    let tx = Transaction::vote_new(&vote_account, vote, *last_id, 0);
    {
        let mut blob = shared_blob.write().unwrap();
        let bytes = serialize(&tx)?;
        let len = bytes.len();
        blob.data[..len].copy_from_slice(&bytes);
        blob.meta.set_addr(&leader_tpu);
        blob.meta.size = len;
    };

    Ok(shared_blob)
}

fn get_leader_tpu(cluster_info: &Arc<RwLock<ClusterInfo>>) -> Result<SocketAddr> {
    let leader_tpu = cluster_info.read().unwrap().leader_data().map(|l| l.tpu);

    if let Some(leader_tpu) = leader_tpu {
        Ok(leader_tpu)
    } else {
        Err(Error::VoteError(VoteError::LeaderInfoNotFound))
    }
}

pub fn send_validator_vote(
    bank: &Arc<Bank>,
    vote_account: &Keypair,
    cluster_info: &Arc<RwLock<ClusterInfo>>,
    vote_blob_sender: &BlobSender,
) -> Result<()> {
    let last_id = bank.last_id();

    let shared_blob = create_new_signed_vote_blob(&last_id, vote_account, bank, cluster_info)?;
    inc_new_counter_info!("replicate-vote_sent", 1);
    vote_blob_sender.send(vec![shared_blob])?;

    Ok(())
}
