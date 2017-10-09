#!/bin/bash

set -e

source prepare-shell.sh

CITATIONS_FILE="$1"
OUTPUT_BASENAME="$2"

if [ -z "$CITATIONS_FILE" ]; then
  echo "Usage: $0 <citations file.tsv.gz> [<output basename>]"
  exit 1
fi

if [ ! -f "$CITATIONS_FILE" ]; then
  echo "File does not exist: $CITATIONS_FILE"
  exit 2
fi

if [ -z "$OUTPUT_BASENAME" ]; then
  OUTPUT_BASENAME="$TEMP_DIR/citerank-data"
fi

mkdir -p "$TEMP_DIR"

LMDB_ROOT="$TEMP_DIR/links-lmdb/"
NUMBERED_EDGES_FILE="$OUTPUT_BASENAME-edges.tsv.gz"

remove_header() { tail -n +2; }

pv "$CITATIONS_FILE" | zcat - | remove_header | \
  python -m citerank.convert_to_indexed --lmdb-root="$LMDB_ROOT" | \
  gzip > "$NUMBERED_EDGES_FILE"
