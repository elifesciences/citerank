from __future__ import print_function
from __future__ import absolute_import

import argparse
import logging

import igraph

from citerank.utils import (
  iter_read_csv_columns,
  write_csv
)

def get_logger():
  return logging.getLogger(__name__)

def parse_args():
  parser = argparse.ArgumentParser(
    description='Calculates the pagerank from citation links (or similar kind of links)'
  )
  parser.add_argument(
    '--links-path', type=str, required=True,
    help='path to links (csv)'
  )
  parser.add_argument(
    '--source-column', type=str, required=False,
    default='citing_doi',
    help='column name of link source'
  )
  parser.add_argument(
    '--target-column', type=str, required=False,
    default='cited_doi',
    help='column name of link target'
  )
  parser.add_argument(
    '--pagerank-output-path', type=str, required=True,
    help='output filename to pagerank file (csv)'
  )
  args = parser.parse_args()
  return args

def main():
  logger = get_logger()

  args = parse_args()
  source_column = args.source_column
  target_column = args.target_column

  logger.info('reading: %s', args.links_path)
  links = list(iter_read_csv_columns(args.links_path, [source_column, target_column]))
  logger.info('links: %d', len(links))

  logger.info('determining unique labels')
  labels = sorted({t[0] for t in links} | {t[1] for t in links})
  logger.info('labels: %d', len(labels))

  logger.info('building index')
  label_to_index_map = {label: index for index, label in enumerate(labels)}

  logger.info('generating graph')
  g = igraph.Graph(
    n=len(labels),
    edges=[
      (label_to_index_map[source_label], label_to_index_map[target_label])
      for source_label, target_label in links
    ],
    directed=True
  )

  logger.info('calculating pagerank')
  pageranked = g.pagerank()

  logger.info('sorting pagerank')
  labels_with_pagerank = sorted(zip(labels, pageranked), key=lambda t: -t[1])

  logger.info('counting incoming/outgoing connections')
  incoming_count_map = {}
  outgoing_count_map = {}
  for source, target in links:
    incoming_count_map[target] = incoming_count_map.get(target, 0) + 1
    outgoing_count_map[source] = outgoing_count_map.get(source, 0) + 1
  label_to_index_map = {label: index for index, label in enumerate(labels)}

  logger.info('writing to %s', args.pagerank_output_path)
  write_csv(
    args.pagerank_output_path,
    [source_column, 'pagerank', 'incoming', 'outgoing'],
    (
      (
        label,
        '{:.20f}'.format(pagerank),
        incoming_count_map.get(label, 0),
        outgoing_count_map.get(label, 0)
      )
      for label, pagerank in labels_with_pagerank
    )
  )

  logger.info('done')

if __name__ == "__main__":
  logging.basicConfig(level='INFO')

  main()
