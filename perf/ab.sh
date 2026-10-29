#!/usr/bin/env bash
# Interleaved A/B benchmark: runs each scenario alternately on a baseline checkout and on this
# checkout, in A B B A order, so slow drift in machine speed (heat, background load) affects
# both sides equally. Then merges each side's rounds and writes perf/comparison.md.
#
# usage: [ROUNDS=n] [CAND_CARGO_ARGS=..] [CAND_RUN_ARGS=..] perf/ab.sh <baseline-checkout> <work-dir> <scenario>...
#   Each round runs A B B A; ROUNDS (default 1) repeats it. CAND_CARGO_ARGS and CAND_RUN_ARGS add
#   cargo and harness arguments for the candidate only, e.g. "--features otel" and "--otel".
#   e.g. git worktree add ../rtg-baseline HEAD
#        perf/ab.sh ../rtg-baseline /tmp/ab consume_throughput submit_throughput
set -euo pipefail

baseline=$(cd "$1" && pwd)
work=$2
shift 2
candidate=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$work"

run() { # <side> <checkout> <round> <scenario>
    local cargo_args="" run_args=""
    if [ "$1" = cand ]; then cargo_args=${CAND_CARGO_ARGS:-}; run_args=${CAND_RUN_ARGS:-}; fi
    echo "== $1 round $3: $4"
    (cd "$2" && cargo bench -q --bench load $cargo_args -- $run_args "$4" 2>&1 | grep -E "warning|timed out" || true)
    mkdir -p "$work/$1/r$3"
    cp "$2/perf/results/$4.json" "$work/$1/r$3/"
}

rounds=${ROUNDS:-1}
for scenario in "$@"; do
  for ((k = 1; k <= rounds; k++)); do
    run base "$baseline" $((2 * k - 1)) "$scenario"
    run cand "$candidate" $((2 * k - 1)) "$scenario"
    run cand "$candidate" $((2 * k)) "$scenario"
    run base "$baseline" $((2 * k)) "$scenario"
  done
done

cd "$candidate"
cargo bench -q --bench load -- --merge-median "$work/base/median" "$work"/base/r[0-9]*
cargo bench -q --bench load -- --merge-median "$work/cand/median" "$work"/cand/r[0-9]*
# Noise: how far a single baseline round sits from the baseline median.
cargo bench -q --bench load -- --current "$work/cand/median" \
    --compare "$work/base/median" --noise "$work/base/r1"
