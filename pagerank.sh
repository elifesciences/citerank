#!/bin/bash

set -e

source prepare-shell.sh

OUTPUT_BASENAME="$1"

if [ -z "$OUTPUT_BASENAME" ]; then
  OUTPUT_BASENAME="$TEMP_DIR/citerank-data"
fi

TEMP_DIR=`readlink -m "$TEMP_DIR"`
DATA_PATH=`readlink -m "$DATA_PATH"`
OUTPUT_BASENAME=`readlink -m "$OUTPUT_BASENAME"`

NUMBERED_EDGES_FILE="$OUTPUT_BASENAME-edges.tsv.gz"
GRAPH_NAME=citations-graph

if [ ! -f "$NUMBERED_EDGES_FILE" ]; then
  echo "Not found: $NUMBERED_EDGES_FILE"
  exit 2
fi

RUST_PAGERANK_DIR="$TEMP_DIR/rust-pagerank"
RUST_PAGERANK_REPO=https://github.com/frankmcsherry/pagerank.git
PAGERANK_PEER_FILE_PREFIX="$RUST_PAGERANK_DIR/$GRAPH_NAME-peer"

if [ ! -d "$RUST_PAGERANK_DIR" ]; then
  git clone $RUST_PAGERANK_REPO "$RUST_PAGERANK_DIR"
fi

cd "$RUST_PAGERANK_DIR"

echo "parse to binary representation..."
zcat "$NUMBERED_EDGES_FILE" | cargo run --release --bin parse -- $GRAPH_NAME

WORKER_COUNT=`nproc`

echo "calculating pagerank (using $WORKER_COUNT workers)"
cargo run --release --bin pagerank -- $GRAPH_NAME worker -o "$PAGERANK_PEER_FILE_PREFIX-?.tsv" -w $WORKER_COUNT -i 100
