1. Make sure your rust application uses https://github.com/gnzlbg/jemallocator as the global memory allocator (when in doubt,  grep `jemallocator` in your `Cargo.lock`).
2. Install `jemalloc` (we'll only need `jeprof`), `dot`, `ps2pdf` and `libunwind` on your system. Enable jemallocator's `profiling` feature (if `jemallocator` is an indirect dependency, one trick to do is to add a dependency `jemallocator = { version = "*", features = ["profiling"] }` to your app and let cargo select the `||` of features for you).
3. `export _RJEM_MALLOC_CONF=prof:true,lg_prof_interval:32,lg_prof_sample:19`.
`lg_prof_interval` sets how often profile dump should be written to disk measured in allocated bytes. The value is passed as a power of two, which is 2^32 in our case, i.e. every 4 GiB of allocations of long-lived objects (see https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Heap-Profiling). `lg_prof_sample:19` tells jemalloc to take a profiling sample every 2^19 = 512 KiB.
4. Running your binary should produce a bunch of `jeprof.*.heap` files (depending on your `_RJEM_MALLOC_CONF`).
5. To produce a PDF output, run `jeprof --show_bytes --pdf "/path/to/<binary>" jeprof.*.heap > <binary>.pdf`, where `<binary>` is the binary you are profiling, e.g.
``jeprof --show_bytes --pdf `which ripgrep` jeprof.2805.204.i204.heap > ripgrep.pdf``.
You probably want to select the latest `jeprof.*.heap` file, see http://jemalloc.net/mailman/jemalloc-discuss/2015-November/001205.html.
6. 
```
+[profile.release]
+debug = true
```
7. This should be run on the validator machine that is generating the heap output
./jeprof --text .cargo/bin/solana-validator solana/jeprof.out.25259.90.i90.heap --base solana/jeprof.out.25259.89.i89.heap  | less

