#!/bin/bash
export RUST_LOG=solana=info,solana::crdt=info
sudo sysctl -w net.core.rmem_max=26214400
cargo run --release --bin solana-fullnode --features=cuda -- -l leader.json < genesis.log
