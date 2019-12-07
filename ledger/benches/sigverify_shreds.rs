#![feature(test)]

extern crate test;
use solana_ledger::shred::Shred;
use solana_ledger::shred::SIZE_OF_DATA_SHRED_PAYLOAD;
use solana_ledger::sigverify_shreds::{sign_shreds_cpu, sign_shreds_gpu};
use solana_perf::packet::{Packet, Packets};
use solana_perf::recycler_cache::RecyclerCache;
use solana_sdk::signature::{Keypair, KeypairUtil};
use test::Bencher;

#[bench]
fn bench_sigverify_shreds_sign_gpu(bencher: &mut Bencher) {
    let recycler_cache = RecyclerCache::default();

    let mut packets = Packets::default();
    packets.packets.set_pinnable();
    let num_packets = 32;
    let num_batches = 100;
    let slot = 0xdeadc0de;
    // need to pin explicitly since the resize will not cause re-allocation
    packets.packets.reserve_and_pin(num_packets);
    packets.packets.resize(num_packets, Packet::default());
    for p in packets.packets.iter_mut() {
        let shred = Shred::new_from_data(
            slot,
            0xc0de,
            0xdead,
            Some(&[5; SIZE_OF_DATA_SHRED_PAYLOAD]),
            true,
            true,
            1,
            2,
        );
        shred.copy_to_packet(p);
    }
    let mut batch = vec![packets; num_batches];
    let keypair = Keypair::new();
    let pubkeys = [
        (slot, keypair.pubkey().to_bytes()),
        (std::u64::MAX, [0u8; 32]),
    ]
    .iter()
    .cloned()
    .collect();
    let privkeys = [
        (slot, keypair.secret.to_bytes()),
        (std::u64::MAX, [0u8; 32]),
    ]
    .iter()
    .cloned()
    .collect();
    //unsigned
    bencher.iter(|| {
        sign_shreds_gpu(&mut batch, &pubkeys, &privkeys, &recycler_cache);
    })
}

#[bench]
fn bench_sigverify_shreds_sign_cpu(bencher: &mut Bencher) {
    let mut packets = Packets::default();
    let num_packets = 32;
    let num_batches = 100;
    let slot = 0xdeadc0de;
    packets.packets.resize(num_packets, Packet::default());
    for p in packets.packets.iter_mut() {
        let shred = Shred::new_from_data(
            slot,
            0xc0de,
            0xdead,
            Some(&[5; SIZE_OF_DATA_SHRED_PAYLOAD]),
            true,
            true,
            1,
            2,
        );
        shred.copy_to_packet(p);
    }
    let mut batch = vec![packets; num_batches];
    let keypair = Keypair::new();
    let pubkeys = [
        (slot, keypair.pubkey().to_bytes()),
        (std::u64::MAX, [0u8; 32]),
    ]
    .iter()
    .cloned()
    .collect();
    let privkeys = [
        (slot, keypair.secret.to_bytes()),
        (std::u64::MAX, [0u8; 32]),
    ]
    .iter()
    .cloned()
    .collect();
    //unsigned
    bencher.iter(|| {
        sign_shreds_cpu(&mut batch, &pubkeys, &privkeys);
    })
}
