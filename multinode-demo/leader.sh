#!/bin/bash
# if RUST_LOG is unset, default to info
export RUST_LOG=${RUST_LOG:-solana=info}

set -x
[[ $(uname) = Linux ]] && sudo sysctl -w net.core.rmem_max=26214400

cargo run --release --bin solana-fullnode --features=cuda -- \
      -l leader.json < genesis.log tx-*.log > tx-"$(date -u +%Y%m%d%k%M%S%N).log"
