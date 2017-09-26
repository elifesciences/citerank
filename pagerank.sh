#!/bin/bash

set -e

python -m citerank.igraph.pagerank $@
