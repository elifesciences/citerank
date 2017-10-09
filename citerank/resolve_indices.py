from __future__ import absolute_import

import argparse
import csv
import sys

import lmdb

from citerank.convert_to_indexed import (
  encode_index,
  encode_key,
  decode_value,
  decode_value_with_counts,
  TO_INDEX_DB_NAME,
  FROM_INDEX_DB_NAME
)

def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Resolves indices to the original text values using intermediate LMDB Database'
  )
  parser.add_argument(
    '--lmdb-root', type=str,
    default='.temp/links-lmdb/',
    help='output root path of the LMDB database'
  )
  parser.add_argument(
    '--delimiter', type=str,
    default='\t',
    help='delimiter to use'
  )
  parser.add_argument(
    '--columns', type=str,
    default='1',
    help='column indices to convert (1 base)'
  )
  parser.add_argument(
    '--header',
    action='store_true',
    help='if set, first row is expected to be a header and will be passed through'
  )
  parser.add_argument(
    '--include-counts',
    action='store_true',
    help='include counts'
  )
  return parser

def read_items_from(fp, delimiter):
  csv_reader = csv.reader(fp, delimiter=delimiter)
  for item in csv_reader:
    yield item

def map_items_to_resolved_indices(coll, lmdb_root, column_indices, header, include_counts):
  env = lmdb.open(lmdb_root, map_size=int(42e9), create=False, max_dbs=2)
  to_index_db = env.open_db(TO_INDEX_DB_NAME)
  from_index_db = env.open_db(FROM_INDEX_DB_NAME)
  with env.begin(write=False) as txn:
    with txn.cursor(to_index_db) as to_cursor:
      with txn.cursor(from_index_db) as cursor:
        def resolve_index(index):
          encoded_index = encode_index(int(index))
          resolved_index = cursor.get(encoded_index)
          if resolved_index:
            key = decode_value(resolved_index)
            if include_counts:
              _, counts = decode_value_with_counts(
                to_cursor.get(encode_key(key))
              )
              return [key] + counts
            return key
          raise RuntimeError('unable to resolve: {}'.format(index))

        for index, item in enumerate(coll):
          if header and index == 0:
            yield item
          else:
            if include_counts:
              out_a = []
              for i, v in enumerate(item):
                if i in column_indices:
                  for x in resolve_index(v):
                    out_a.append(x)
                else:
                  out_a.append(v)
              yield out_a
            else:
              yield [
                resolve_index(v) if i in column_indices else v
                for i, v in enumerate(item)
              ]

def output_items(coll, fp, delimiter):
  csv_writer = csv.writer(fp, delimiter=delimiter)
  for item in coll:
    csv_writer.writerow(item)

def resolve_indices(argv):
  args = get_args_parser().parse_args(argv)
  output_items(
    map_items_to_resolved_indices(
      read_items_from(
        sys.stdin,
        delimiter=args.delimiter
      ),
      lmdb_root=args.lmdb_root,
      column_indices=[int(x.strip()) - 1 for x in  args.columns.split(',')],
      header=args.header,
      include_counts=args.include_counts
    ),
    sys.stdout,
    delimiter=args.delimiter
  )

def main(argv=None):
  resolve_indices(argv)

if __name__ == "__main__":
  main()
