from __future__ import absolute_import

import argparse
import csv
import sys

import lmdb

from citerank.convert_to_indexed import (
  encode_index,
  decode_value,
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
  return parser

def read_items_from(fp, delimiter):
  csv_reader = csv.reader(fp, delimiter=delimiter)
  for item in csv_reader:
    yield item

def map_items_to_resolved_indices(coll, lmdb_root, column_indices, header):
  env = lmdb.open(lmdb_root, map_size=int(42e9), create=False, max_dbs=2)
  from_index_db = env.open_db(FROM_INDEX_DB_NAME)
  with env.begin(write=False) as txn:
    with txn.cursor(from_index_db) as cursor:
      def resolve_index(index):
        encoded_index = encode_index(int(index))
        resolved_index = cursor.get(encoded_index)
        if resolved_index:
          return decode_value(resolved_index)
        raise RuntimeError('unable to resolve: {}'.format(index))

      for index, item in enumerate(coll):
        if header and index == 0:
          yield item
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
      header=args.header
    ),
    sys.stdout,
    delimiter=args.delimiter
  )

def main(argv=None):
  resolve_indices(argv)

if __name__ == "__main__":
  main()
