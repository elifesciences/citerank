#!/bin/bash

set -e

source prepare-shell.sh

DOCKER_INSTANCE_NAME=citations-arangodb
DB_ROOT_PASSWORD=
PAGERANK_FILE=$DATA_PATH/citations-graph-pagerank-sorted.tsv.gz
CITATIONS_FILE=$DATA_PATH/crossref-works-citations-cleaned.tsv.gz

remove_header() { tail -n +2; }
add_pagerank_header() { { printf '_key\tdoi\tincoming\toutgoing\tpagerank\n'; cat -; }; }
add_citations_header() { { printf '_from\t_to\tciting_doi\tcited_doi\n'; cat -; }; }
filter_cited_and_citing_doi_not_empty() { grep -P '.+\t.+'; }
add_pagerank_hash_key() {
  python -m citerank.add_hash_column --delimiter=$'\t' --method='sha1' --source-column=1 --target-column=1
}
add_citations_hash_key() {
  python -m citerank.add_hash_column --delimiter=$'\t' --method='sha1' --source-column=2 --target-column=1 | \
  python -m citerank.add_hash_column --delimiter=$'\t' --method='sha1' --source-column=2 --target-column=1
}

echo "importing works"
pv "$PAGERANK_FILE" | zcat - | \
  add_pagerank_hash_key | \
  add_pagerank_header | \
  docker exec -i $DOCKER_INSTANCE_NAME \
  arangoimp --file - --type tsv --collection works --create-collection true \
  --overwrite true --progress false \
  --server.password=$DB_ROOT_PASSWORD

echo "importing citations"
pv "$CITATIONS_FILE" | zcat - | head -100000 | \
  cut -f 1,2 | remove_header | \
  filter_cited_and_citing_doi_not_empty | \
  add_citations_hash_key | \
  add_citations_header | \
  docker exec -i $DOCKER_INSTANCE_NAME \
  arangoimp --file - --type tsv --collection citations --create-collection true \
  --create-collection-type edge \
  --from-collection-prefix works \
  --to-collection-prefix works \
  --overwrite true --progress false \
  --batch-size 33554432 \
  --server.password=$DB_ROOT_PASSWORD
