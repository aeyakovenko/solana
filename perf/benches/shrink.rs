#![allow(clippy::integer_arithmetic)]
#![feature(test)]

extern crate test;

use {
    rand::prelude::*,
    solana_perf::{
        packet::{to_packet_batches, PacketBatch, PACKETS_PER_BATCH},
        sigverify,
    },
    test::Bencher,
};

const NUM_PACKETS: usize = 1024 * 4;

fn bench_rng() -> BenchRng {
    rand::thread_rng()
}

fn test_packet_with_size<R: Rng>(size: usize, rng: &mut R) -> Vec<u8> {
    // subtract 8 bytes because the length will get serialized as well
    (0..size.checked_sub(8).unwrap())
        .map(|_| rng.gen())
        .collect()
}

fn do_bench_shrink_packets<R: Rng>(bencher: &mut Bencher, mut batches: Vec<PacketBatch>, rng: &mut R) {
    // verify packets
    bencher.iter(|| {
        let _ans = sigverify::shrink_batches(&mut batches);
        batches.iter_mut().for_each(|b| {
            b.packets
                .iter_mut()
                .for_each(|p| p.meta.set_discard(rng.gen()))
        });
    });
}

#[bench]
#[ignore]
fn bench_shrink_diff_small_packets(bencher: &mut Bencher) {
    let mut rng = bench_rng();

    let batches = to_packet_batches(
        &(0..NUM_PACKETS)
            .map(|_| test_packet_with_size(128, &mut rng))
            .collect::<Vec<_>>(),
        PACKETS_PER_BATCH,
    );

    do_bench_shrink_packets(bencher, batches, &mut rng);
}

#[bench]
#[ignore]
fn bench_shrink_diff_big_packets(bencher: &mut Bencher) {
    let mut rng = bench_rng();

    let batches = to_packet_batches(
        &(0..NUM_PACKETS)
            .map(|_| test_packet_with_size(1024, &mut rng))
            .collect::<Vec<_>>(),
        PACKETS_PER_BATCH,
    );

    do_bench_shrink_packets(bencher, batches, &mut rng);
}

#[bench]
#[ignore]
fn bench_shrink_count_packets(bencher: &mut Bencher) {
    let mut rng = bench_rng();

    let mut batches = to_packet_batches(
        &(0..NUM_PACKETS)
            .map(|_| test_packet_with_size(128, &mut rng))
            .collect::<Vec<_>>(),
        PACKETS_PER_BATCH,
    );
    batches.iter_mut().for_each(|b| {
        b.packets
            .iter_mut()
            .for_each(|p| p.meta.set_discard(rng.gen()))
    });

    bencher.iter(|| {
        let _ = sigverify::count_valid_packets(&batches);
    });
}
