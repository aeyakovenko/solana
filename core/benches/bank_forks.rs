#![feature(test)]

extern crate solana;
extern crate solana_runtime;
extern crate solana_sdk;
extern crate test;

use rand::{thread_rng, Rng};
use solana::bank_forks::BankForks;
use solana_runtime::bank::Bank;
use solana_sdk::genesis_block::GenesisBlock;
use solana_sdk::pubkey::Pubkey;
use test::Bencher;

fn gen_forks(size: usize) -> BankForks {
    let (genesis_block, _mint_keypair) = GenesisBlock::new(500);
    let bank = Bank::new(&genesis_block);
    let mut forks = BankForks::new(0, bank);
    for i in 1..size {
        let sample = thread_rng().gen_range(0, i);
        let bank = Bank::new_from_parent(
            forks.get(sample as u64).unwrap(),
            &Pubkey::default(),
            i as u64,
        );
        forks.insert(bank);
    }
    forks
}

#[bench]
fn bench_bank_forks_descendants(bencher: &mut Bencher) {
    let bank_forks = gen_forks(1000);
    bencher.iter(|| {
        let _ = bank_forks.descendants();
    });
}

#[bench]
fn bench_bank_forks_ancestors(bencher: &mut Bencher) {
    let bank_forks = gen_forks(1000);
    bencher.iter(|| {
        let _ = bank_forks.ancestors();
    });
}
