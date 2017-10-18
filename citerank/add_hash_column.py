from __future__ import print_function
from __future__ import absolute_import

import argparse
import csv
import sys
from hashlib import md5, sha1
from signal import signal, SIGPIPE, SIG_DFL

def parse_args():
  parser = argparse.ArgumentParser(
    description='Adds a hash column'
  )
  parser.add_argument(
    '--delimiter', type=str, default=',',
    help='delimiter to use'
  )
  parser.add_argument(
    '--source-column', type=int, default=1,
    help='the source column'
  )
  parser.add_argument(
    '--target-column', type=int, default=1,
    help='the target column'
  )
  parser.add_argument(
    '--method', type=str, default='md5',
    choices=['md5', 'sha1'],
    help='the hash function'
  )
  parser.add_argument(
    '--prefix', type=str, default='',
    help='the hash function'
  )
  args = parser.parse_args()
  return args

def get_hash_f(name):
  if name == 'md5':
    return md5
  if name == 'sha1':
    return sha1
  raise ValueError('unrecognised name: {}'.format(name))

def main():
  args = parse_args()
  csv_reader = csv.reader(sys.stdin, delimiter=args.delimiter)
  csv_writer = csv.writer(sys.stdout, delimiter=args.delimiter)
  source_column = args.source_column - 1
  target_column = args.target_column - 1
  h = get_hash_f(args.method)
  prefix = args.prefix
  for row in csv_reader:
    try:
      source_value = row[source_column]
    except IndexError:
      raise IndexError('invalid index: {}, length: {}'.format(source_column, len(row)))
    row.insert(target_column, prefix + h(source_value.encode('utf-8')).hexdigest())
    csv_writer.writerow(row)

if __name__ == "__main__":
  signal(SIGPIPE, SIG_DFL)

  main()
