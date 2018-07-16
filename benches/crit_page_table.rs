#[macro_use]
extern crate criterion;
extern crate rand;
extern crate solana;

use criterion::Criterion;
use rand::{thread_rng, RngCore};
use solana::page_table::{Call, Page, PageTable};

const N: usize = 256;
const K: usize = 16;

fn bench_load_and_execute(criterion: &mut Criterion) {
    criterion.bench_function("bench_load_and_execute", move |b| {
        let mut pt = PageTable::new();
        let mut ttx: Vec<Vec<Call>> = (0..N)
            .map(|_| (0..N).map(|_r| Call::random_tx()).collect())
            .collect();
        for transactions in &ttx {
            pt.force_allocate(transactions, true, 1_000_000);
        }
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut to_pages = vec![vec![None; K]; N];
        let mut loaded_page_table: Vec<Vec<_>> = (0..N)
            .map(|_| {
                (0..K)
                    .map(|_| unsafe {
                        // Fill the loaded_page_table with a dummy reference
                        let ptr = 0xfefefefefefefefe as *mut Page;
                        &mut *ptr
                    })
                    .collect()
            })
            .collect();
        b.iter(|| {
            let transactions = &mut ttx[thread_rng().next_u64() as usize % N];
            for tx in transactions.iter_mut() {
                tx.version += 1;
            }
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_call(&transactions, &lock, &mut checked);
            pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut to_pages);
            pt.allocate_keys(&transactions, &checked, &needs_alloc, &mut to_pages);
            pt.load_and_execute(
                &transactions,
                &mut checked,
                &mut to_pages,
                &mut loaded_page_table,
            );
            pt.release_memory_lock(&transactions, &lock);
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(2);
    targets = bench_load_and_execute
);
criterion_main!(benches);
