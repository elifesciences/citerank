Calculates PageRank over citation data.

# Pre-requsites

* Python 2 or 3 (Python 3 preferred)
* [igraph](http://igraph.org/python/)

# Setup

```bash
pip install -r requirements.txt
```

# Generate PageRank from citation data

First generate or download the citation links, e.g. using [datacapsule-occ](https://github.com/elifesciences/datacapsule-occ).

Then run:

```bash
./pagerank.sh \
  --links-path=data/doi-citation-links.csv \
  --pagerank-output-path=data/doi-citations-pagerank.csv \
  --source-column=citing_doi \
  --target-column=cited_doi
```

The output will be _data/doi-citations-pagerank.csv_ with the following columns:

* _citing_doi_ (or name specified by _source_column_)
* _pagerank_ - the file will be sorted by that value (descending)
* _incoming_ - number of incoming connections
* _outgoing_ - number of outgoing connections

# License

[GPL](citerank/igraph/LICENSE) due to [igraph](http://igraph.org/python/). (This may change in the future when switiching to another library)

Code that doesn't rely on GPL licensed code, may be licensed under [MIT](https://opensource.org/licenses/MIT).
