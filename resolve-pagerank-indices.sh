#!/bin/bash

set -e

source prepare-shell.sh

TEMP_DIR=`realpath "$TEMP_DIR"`
DATA_PATH=`realpath "$DATA_PATH"`

GRAPH_NAME=citations-graph

RUST_PAGERANK_DIR="$TEMP_DIR/rust-pagerank"
PAGERANK_PEER_FILE_PREFIX="$RUST_PAGERANK_DIR/$GRAPH_NAME-peer"
LMDB_ROOT="$TEMP_DIR/links-lmdb/"
RESOLVED_PAGERANK_FILE="$DATA_PATH/$GRAPH_NAME-pagerank.tsv.gz"

remove_pagerank_header() { grep -v 'id'; }

pv $PAGERANK_PEER_FILE_PREFIX-*.tsv | \
  remove_pagerank_header | \
  python -m citerank.resolve_indices --lmdb-root="$LMDB_ROOT" | \
  gzip > "$RESOLVED_PAGERANK_FILE"
