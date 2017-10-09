from __future__ import absolute_import

import argparse
import csv
import os
from shutil import rmtree
from itertools import groupby
import sys
import hashlib

import lmdb


def get_args_parser():
  parser = argparse.ArgumentParser(
    description='Convert Text Links to Indexed Links using intermediate LMDB Database'
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
  return parser

def read_grouped_items_from(fp, delimiter):
  csv_reader = csv.reader(fp, delimiter=delimiter)
  for k, v in groupby(csv_reader, key=lambda row: row[0]):
    yield k, [x[1] for x in v]

# The Python LMDB library has a maximum key length that can be changed by recompiling it
MAX_KEY_LENGTH = 511
HASH_F = hashlib.sha384

def encode_key(key):
  encoded_key = key.encode('utf-8')
  if len(encoded_key) > MAX_KEY_LENGTH:
    encoded_key = HASH_F(encoded_key).digest()
  return encoded_key

def map_grouped_items_to_index(coll, lmdb_root):
  if os.path.isdir(lmdb_root):
    rmtree(lmdb_root)

  env = lmdb.open(lmdb_root, map_size=int(42e9))

  with env.begin(write=True) as txn:
    with txn.cursor() as cursor:
      next_id = 0

      def insert_or_get_next_id(s):
        nonlocal next_id

        try:
          encoded_key = encode_key(s)
          if cursor.put(encoded_key, str(next_id).encode(), overwrite=False):
            this_id = next_id
            next_id += 1
            return this_id
          else:
            return cursor.item()[1].decode()
        except:
          raise RuntimeError("failed to insert record: {} ({}: {})".format(s, type(s), len(s)))

      for left, right_list in coll:
        non_empty_right_list = [x for x in right_list if x]
        if non_empty_right_list:
          left_id = insert_or_get_next_id(left)
          right_id_list = [
            insert_or_get_next_id(right)
            for right in non_empty_right_list
          ]
          yield left_id, right_id_list

def output_grouped_items(coll, fp, delimiter):
  csv_writer = csv.writer(fp, delimiter=delimiter)
  for left, right_list in coll:
    for right in right_list:
      csv_writer.writerow([left, right])

def convert_to_indexed(argv):
  args = get_args_parser().parse_args(argv)
  output_grouped_items(
    map_grouped_items_to_index(
      read_grouped_items_from(
        sys.stdin,
        delimiter=args.delimiter
      ),
      lmdb_root=args.lmdb_root
    ),
    sys.stdout,
    delimiter=args.delimiter
  )

def main(argv=None):
  convert_to_indexed(argv)

if __name__ == "__main__":
  main()
