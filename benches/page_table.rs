#[macro_use]
extern crate criterion;
extern crate rand;
extern crate solana;

use criterion::{Bencher, Criterion};
use rand::{thread_rng, RngCore};
use solana::page_table;
use solana::page_table::{Call, Page, PageTable};

const N: usize = 1024;
fn bench_update_version(bencher: &mut Bencher) {
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    bencher.iter(move || {
        for tx in &mut transactions {
            tx.version += 1;
        }
    });
}
fn bench_mem_lock(bencher: &mut Bencher) {
    let pt = PageTable::new();
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    bencher.iter(move || {
        let mut lock = vec![false; N];
        for tx in &mut transactions {
            tx.version += 1;
        }
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.release_memory_lock(&transactions, &lock);
    });
}
fn bench_validate_call_miss(bencher: &mut Bencher) {
    let mut pt = PageTable::new();
    let fill_table: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    pt.force_allocate(&fill_table, true, 1_000_000);
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    bencher.iter(move || {
        let mut lock = vec![false; N];
        let mut checked = vec![false; N];
        for tx in &mut transactions {
            tx.version += 1;
        }
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.release_memory_lock(&transactions, &lock);
    });
}
fn bench_validate_call_hit(bencher: &mut Bencher) {
    let mut pt = PageTable::new();
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    pt.force_allocate(&transactions, true, 1_000_000);
    bencher.iter(move || {
        let mut lock = vec![false; N];
        let mut checked = vec![false; N];
        for tx in &mut transactions {
            tx.version += 1;
        }
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.release_memory_lock(&transactions, &lock);
    });
}
fn bench_find_new_keys_init(bencher: &mut Bencher) {
    let mut pt = PageTable::new();
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    pt.force_allocate(&transactions, true, 1_000_000);
    bencher.iter(move || {
        let mut _lock = vec![false; N];
        let mut _checked = vec![false; N];
        let mut _needs_alloc = vec![false; N];
        let mut _to_pages: Vec<Vec<Option<usize>>> = vec![vec![None; N]; N];
        for tx in &mut transactions {
            tx.version += 1;
        }
    });
}
fn bench_find_new_keys_needs_alloc(bencher: &mut Bencher) {
    let mut pt = PageTable::new();
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    pt.force_allocate(&transactions, true, 1_000_000);
    bencher.iter(move || {
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut to_pages = vec![vec![None; N]; N];
        for tx in &mut transactions {
            tx.version += 1;
        }
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut to_pages);
        pt.release_memory_lock(&transactions, &lock);
    });
}
fn bench_find_new_keys_no_alloc(bencher: &mut Bencher) {
    let mut pt = PageTable::new();
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    pt.force_allocate(&transactions, true, 1_000_000);
    pt.force_allocate(&transactions, false, 1_000_000);
    bencher.iter(move || {
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut to_pages = vec![vec![None; N]; N];
        for tx in &mut transactions {
            tx.version += 1;
        }
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut to_pages);
        pt.release_memory_lock(&transactions, &lock);
    });
}
fn bench_allocate_new_keys_some_new_allocs(bencher: &mut Bencher) {
    let mut pt = PageTable::new();
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    pt.force_allocate(&transactions, true, 1_000_000);
    bencher.iter(move || {
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut to_pages = vec![vec![None; N]; N];
        for tx in &mut transactions {
            tx.version += 1;
        }
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut to_pages);
        pt.allocate_keys(&transactions, &checked, &needs_alloc, &mut to_pages);
        pt.release_memory_lock(&transactions, &lock);
    });
}
fn bench_allocate_new_keys_no_new_allocs(bencher: &mut Bencher) {
    let mut pt = PageTable::new();
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    pt.force_allocate(&transactions, true, 1_000_000);
    pt.force_allocate(&transactions, false, 1_000_000);
    bencher.iter(move || {
        let mut lock = vec![false; N];
        let mut checked = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut to_pages = vec![vec![None; N]; N];
        for tx in &mut transactions {
            tx.version += 1;
        }
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut to_pages);
        pt.allocate_keys(&transactions, &checked, &needs_alloc, &mut to_pages);
        pt.release_memory_lock(&transactions, &lock);
    });
}
fn bench_load_and_execute_init(bencher: &mut Bencher) {
    let mut pt = PageTable::new();
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    pt.force_allocate(&transactions, true, 1_000_000);
    bencher.iter(move || {
        let mut _lock = vec![false; N];
        let mut _needs_alloc = vec![false; N];
        let mut _checked = vec![false; N];
        let mut _to_pages: Vec<Vec<Option<usize>>> = vec![vec![None; N]; N];
        let mut _loaded_page_table: Vec<Vec<_>> = (0..N)
            .map(|_| {
                (0..N)
                    .map(|_| unsafe {
                        // Fill the loaded_page_table with a dummy reference
                        let ptr = 0xfefefefefefefefe as *mut Page;
                        &mut *ptr
                    })
                    .collect()
            })
            .collect();
        for tx in &mut transactions {
            tx.version += 1;
        }
    });
}
fn bench_load_and_execute(bencher: &mut Bencher) {
    let mut pt = PageTable::new();
    let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
    pt.force_allocate(&transactions, true, 1_000_000);
    bencher.iter(move || {
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut to_pages = vec![vec![None; N]; N];
        let mut loaded_page_table: Vec<Vec<_>> = (0..N)
            .map(|_| {
                (0..N)
                    .map(|_| unsafe {
                        // Fill the loaded_page_table with a dummy reference
                        let ptr = 0xfefefefefefefefe as *mut Page;
                        &mut *ptr
                    })
                    .collect()
            })
            .collect();
        for tx in &mut transactions {
            tx.version += 1;
        }
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut to_pages);
        pt.sanity_check_pages(&transactions, &checked, &to_pages);
        pt.allocate_keys(&transactions, &checked, &needs_alloc, &mut to_pages);
        pt.sanity_check_pages(&transactions, &checked, &to_pages);
        pt.load_and_execute(
            &transactions,
            &mut checked,
            &mut to_pages,
            &mut loaded_page_table,
        );
        pt.release_memory_lock(&transactions, &lock);
    });
}
fn bench_load_and_execute_large_table(criterion: &mut Criterion) {
    let mut pt = PageTable::new();
    let mut ttx: Vec<Vec<_>> = (0..N)
        .map(|_| (0..N).map(|_r| Call::random_tx()).collect())
        .collect();
    for transactions in &ttx {
        pt.force_allocate(transactions, true, 1_000_000);
    }
    criterion.bench_function("with_setup", move |b| {
        b.iter_with_large_setup(
            || {
                let mut transactions = &mut ttx[thread_rng().next_u64() as usize % N];
                let lock = vec![false; N];
                let needs_alloc = vec![false; N];
                let checked = vec![false; N];
                let to_pages = vec![vec![None; N]; N];
                let loaded_page_table: Vec<Vec<_>> = (0..N)
                    .map(|_| {
                        (0..N)
                            .map(|_| unsafe {
                                // Fill the loaded_page_table with a dummy reference
                                let ptr = 0xfefefefefefefefe as *mut Page;
                                &mut *ptr
                            })
                            .collect()
                    })
                    .collect();
                for tx in transactions.iter_mut() {
                    tx.version += 1;
                }
                (
                    transactions,
                    lock,
                    needs_alloc,
                    checked,
                    to_pages,
                    loaded_page_table,
                )
            },
            |(
                transactions,
                mut lock,
                mut needs_alloc,
                mut checked,
                mut to_pages,
                mut loaded_page_table,
            )| {
                pt.acquire_memory_lock(&transactions, &mut lock);
                pt.validate_call(&transactions, &lock, &mut checked);
                pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut to_pages);
                pt.sanity_check_pages(&transactions, &checked, &to_pages);
                pt.allocate_keys(&transactions, &checked, &needs_alloc, &mut to_pages);
                pt.sanity_check_pages(&transactions, &checked, &to_pages);
                pt.load_and_execute(
                    &transactions,
                    &mut checked,
                    &mut to_pages,
                    &mut loaded_page_table,
                );
                pt.release_memory_lock(&transactions, &lock);
            },
        );
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(2);
    targets = bench
);
criterion_main!(benches);
