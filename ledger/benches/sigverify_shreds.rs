#![feature(test)]

extern crate test;
use solana_ledger::shred::Shred;
use solana_ledger::shred::SIZE_OF_DATA_SHRED_PAYLOAD;
use solana_ledger::sigverify_shreds::{
    sign_shreds_cpu, sign_shreds_gpu, sign_shreds_gpu_pinned_keypair,
};
use solana_perf::recycler_cache::RecyclerCache;
use solana_sdk::signature::{Keypair, KeypairUtil};
use std::sync::Arc;
use test::Bencher;

const NUM_PACKETS: usize = 256;
const NUM_BATCHES: usize = 1;
#[bench]
fn bench_sigverify_shreds_sign_gpu(bencher: &mut Bencher) {
    let recycler_cache = RecyclerCache::default();
    let slot = 0xdeadc0de;
    // need to pin explicitly since the resize will not cause re-allocation
    let packets: Vec<_> = (0..NUM_PACKETS)
        .into_iter()
        .map(|_| {
            Shred::new_from_data(
                slot,
                0xc0de,
                0xdead,
                Some(&[5; SIZE_OF_DATA_SHRED_PAYLOAD]),
                true,
                true,
                1,
                2,
                0,
            )
        })
        .collect();
    let mut batch = vec![packets; NUM_BATCHES];
    let keypair = Keypair::new();
    let pinned_keypair = sign_shreds_gpu_pinned_keypair(&keypair, &recycler_cache);
    let pinned_keypair = Some(Arc::new(pinned_keypair));
    //warmup
    for _ in 0..100 {
        sign_shreds_gpu(&keypair, &pinned_keypair, &mut batch, &recycler_cache);
    }
    bencher.iter(|| {
        sign_shreds_gpu(&keypair, &pinned_keypair, &mut batch, &recycler_cache);
    })
}

#[bench]
fn bench_sigverify_shreds_sign_cpu(bencher: &mut Bencher) {
    let slot = 0xdeadc0de;
    let packets: Vec<_> = (0..NUM_PACKETS)
        .into_iter()
        .map(|_| {
            Shred::new_from_data(
                slot,
                0xc0de,
                0xdead,
                Some(&[5; SIZE_OF_DATA_SHRED_PAYLOAD]),
                true,
                true,
                1,
                2,
                0,
            )
        })
        .collect();
    let mut batch = vec![packets; NUM_BATCHES];

    let keypair = Keypair::new();
    bencher.iter(|| {
        sign_shreds_cpu(&keypair, &mut batch);
    })
}
