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

NUMBERED_CITATIONS_FILE="$OUTPUT_BASENAME-citations.tsv.gz"
NUMBERED_DOIS_FILE="$OUTPUT_BASENAME-dois.tsv.gz"

SQLITE_DB="$TEMP_DIR/citations.db"
SQLITE_INIT="$TEMP_DIR/citations.init"

cat > "$SQLITE_INIT" <<EOL
.mode csv
.separator "\t"
EOL

remove_header() { tail -n +2; }
extract_cited_and_citing_doi() { cut -f 1,2; }
filter_cited_and_citing_doi_not_empty() { grep -P '.+\t.+'; }

echo "initialising database $SQLITE_DB"
rm "$SQLITE_DB"
sqlite3 "$SQLITE_DB" "drop table if exists citations;"
sqlite3 "$SQLITE_DB" "create table if not exists citations(citing_doi TEXT, cited_doi TEXT);"

echo "importing from $CITATIONS_FILE"
pv "$CITATIONS_FILE" | zcat - | \
  remove_header | extract_cited_and_citing_doi | filter_cited_and_citing_doi_not_empty | \
  sqlite3 -init "$SQLITE_INIT" "$SQLITE_DB" ".import /dev/stdin citations"

echo "citation links imported:"
sqlite3 "$SQLITE_DB" "select count(*) from citations;"

sqlite3 "$SQLITE_DB" "drop table if exists dois;"
sqlite3 "$SQLITE_DB" "create table if not exists dois(id INTEGER INQUE, doi TEXT PRIMARY KEY);"

# Note: it seems faster to generate unique list using sort
# echo "populating unique list of dois"
# sqlite3 "$SQLITE_DB" "
#   insert or ignore into dois(doi)
#   select citing_doi from citations
#   union all
#   select cited_doi from citations;
# "

flatten_dois() { tr -d '\r' | tr '\t' '\n'; } # flatten dois to one list of dois
remove_blank_lines() { grep -v '^$'; }
sort_drop_duplicates() { LC_ALL=C sort -T "$TEMP_DIR" -u; }
add_line_number_as_id() { awk '{ print (NR - 1) "\t" $0 }'; }
add_header() { { printf 'id\tdoi\n'; cat -; }; }

echo "generating unique dois to $NUMBERED_DOIS_FILE
  (final sort will complete within minutes after 100% progress is reached)"
pv "$CITATIONS_FILE" | zcat - | \
  remove_header | flatten_dois | remove_blank_lines | sort_drop_duplicates | \
  add_line_number_as_id | add_header | \
  gzip > "$NUMBERED_DOIS_FILE"

echo "importing unique dois from $NUMBERED_DOIS_FILE"
pv "$NUMBERED_DOIS_FILE" | zcat - | \
  remove_header | \
  sqlite3 -init "$SQLITE_INIT" "$SQLITE_DB" ".import /dev/stdin dois"

echo "unique dois:"
sqlite3 "$SQLITE_DB" "select count(*) from dois;"

echo "extracting numbered citations to $NUMBERED_CITATIONS_FILE"
sqlite3 -init "$SQLITE_INIT" "$SQLITE_DB" "
  select citing.id as citing_id, cited.id as cited_id
  from citations
  left join dois citing on citing.doi == citing_doi
  left join dois cited on cited.doi == cited_doi
  where citing_doi <> '' and cited_doi <> '';
" | remove_header | pv | gzip > "$NUMBERED_CITATIONS_FILE"
