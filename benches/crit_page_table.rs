#[macro_use]
extern crate criterion;
extern crate rand;
extern crate solana;

use criterion::Criterion;
use rand::{thread_rng, RngCore};
use solana::page_table::{Call, Context, Page, PageTable, N};

fn bench_load_and_execute(criterion: &mut Criterion) {
    criterion.bench_function("bench_load_and_execute", move |b| {
        let mut pt = PageTable::new();
        let mut ttx: Vec<Vec<_>> = (0..N)
            .map(|_| (0..N).map(|_r| Call::random_tx()).collect())
            .collect();
        for transactions in &ttx {
            pt.force_allocate(transactions, true, 1_000_000);
        }
        let mut ctx = Context::default();
        b.iter(|| {
            let transactions = &mut ttx[thread_rng().next_u64() as usize % ttx.len()];
            for tx in transactions.iter_mut() {
                tx.version += 1;
            }
            pt.acquire_validate_find(&transactions, &mut ctx);
            pt.allocate_keys_with_ctx(&transactions, &mut ctx);
            pt.execute_with_ctx(&transactions, &mut ctx);
            pt.commit(
                &transactions,
                &ctx.commit,
                &ctx.to_pages,
                &ctx.loaded_page_table,
            );
            pt.release_memory_lock(&transactions, &ctx.lock);
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(2);
    targets = bench_load_and_execute
);
criterion_main!(benches);
