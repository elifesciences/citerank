#!/bin/bash

set -e

source prepare-shell.sh

pv "$DATA_PATH/citations-graph-pagerank.tsv.gz" | zcat - | \
  sort -r -g -k 4 -k 1 -t$'\t' -T "$TEMP_DIR" | \
  gzip > "$DATA_PATH/citations-graph-pagerank-sorted.tsv.gz"
