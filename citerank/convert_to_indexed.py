from __future__ import absolute_import

import argparse
import csv
import os
from shutil import rmtree
from itertools import groupby
import sys
import hashlib
import struct
from operator import add

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

def encode_value(value):
  return value.encode('utf-8')

def decode_value(value):
  return value.decode('utf-8')

def encode_value_with_counts(value, counts):
  return struct.pack('Q' * (len(counts) + 1), value, *counts)

def decode_value_with_counts(encoded_value_with_counts):
  value, *counts = struct.unpack('QQQ', encoded_value_with_counts)
  return value, counts

def encode_index(index):
  return struct.pack('Q', index)

def decode_index(encoded_index):
  return struct.unpack('Q', encoded_index)[0]

def get_rev_lmdb_root(lmdb_root):
  return os.path.join(lmdb_root, 'rev')

TO_INDEX_DB_NAME = b'to_index'
FROM_INDEX_DB_NAME = b'from_index'

def update_item_and_return_id(cursor, key, get_next_id_for, counts):
  try:
    encoded_key = encode_key(key)
    value = cursor.get(encoded_key)
    if value is not None:
      current_id, current_counts = decode_value_with_counts(value)
      cursor.put(encoded_key, encode_value_with_counts(
        current_id,
        list(map(add, current_counts, counts))
      ))
      return current_id
    else:
      this_id = get_next_id_for(key)
      cursor.put(encoded_key, encode_value_with_counts(
        this_id,
        counts
      ))
      return this_id
  except:
    raise RuntimeError("failed to insert record: {} ({}: {})".format(key, type(key), len(key)))

def map_grouped_items_to_index(coll, lmdb_root):
  if os.path.isdir(lmdb_root):
    rmtree(lmdb_root)

  env = lmdb.open(lmdb_root, map_size=int(42e9), max_dbs=2, writemap=True)
  to_index_db = env.open_db(TO_INDEX_DB_NAME)
  from_index_db = env.open_db(FROM_INDEX_DB_NAME)

  with env.begin(write=True) as txn:
    with txn.cursor(to_index_db) as to_index_cursor:
      with txn.cursor(from_index_db) as from_index_cursor:
        next_id = 0

        def get_next_id_for(s):
          nonlocal next_id

          this_id = next_id
          next_id += 1
          from_index_cursor.put(encode_index(this_id), encode_value(s))
          return this_id

        for left, right_list in coll:
          non_empty_right_list = [x for x in right_list if x]
          if non_empty_right_list:
            left_id = update_item_and_return_id(
              to_index_cursor,
              left,
              get_next_id_for=get_next_id_for,
              counts=[0, len(non_empty_right_list)]
            )
            right_id_list = [
              update_item_and_return_id(
                to_index_cursor,
                right,
                get_next_id_for=get_next_id_for,
                counts=[1, 0]
              )
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
