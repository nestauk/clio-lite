# clio-lite

A contextual search of Elasticsearch data, as described in [this blog](https://medium.com/@joel.klinger/big-fast-nlp-with-elasticsearch-4ed44924e4d5).

For an interactive sense of how this works, see [the arXlive search tool](https://i5mf7l0opc.execute-api.eu-west-1.amazonaws.com/dev/hierarxy/).

## `clio-lite` as python tool

### Installation

`pip install --upgrade git+https://github.com/nestauk/clio-lite`

### Very basic usage

```python
from clio_lite import clio_search

url = "https://URL_FOR_ELASTICSEARCH"
index = "AN_INDEX"
query = "BERT transformers"

total, docs = clio_search(url=url, index=index, query=query)
```

### Less basic usage

In the following example, I am searching arXiv data. In particular, I would like to search for articles that are *like* the seminal BERT paper (published in 2018), but were written in the years preceeding it.

Before you can get started, you really need to know how the contextual search is working:

1) An initial search is made using your `query` string to Elasticsearch. This generates a numbers of "seed" documents to be used in the second step. If you like, you can specify *filters* on these seed documents and the *number of seed documents* to consider.
2) A second "expanded" search is made for documents which are **semantically similar** to the seed documents. Again, you can specify *filters* on these documents.

If you like, you don't need to specify any filters at all, as in the previous very basic example. However, if you're interested in the filtering syntax, I suggest looking at the elasticsearch documentation on filtering, and also taking a look at this very example.

```python
from clio_lite import clio_search
url = "https://URL_FOR_ELASTICSEARCH"
index = "AN_INDEX"

query = "BERT"                                                 # This is the actual query
pre_filters = [{"range":{"year_of_article":{"gte":"2018"}}}]   # Seed search for docs from 2018 onwards
post_filters = [{"range":{"year_of_article":{"lte":"2017"}}}]  # Expanded search for docs before 2018
fields = ["title_of_article", "textBody_abstract_article"]     # Consider the title and abstract only
limit = 2                                                      # Only return the top 2 documents

total, docs = clio_search(url=url, index=index, query=query, 
                          pre_filters=pre_filters, 
                          post_filters=post_filters, 
                          limit=limit)
total, len(docs)

>>> 11, 2                                                      # There are 11 results, but I only 
                                                               # requested the top 2
```

and the results are pretty interesting (check them out on arXiv!):

```python
for d in docs:
    print(d['title_of_article'])
    
>>> "DisSent: Sentence Representation Learning from Explicit Discourse Relations"
>>> "R$^3$: Reinforced Reader-Ranker for Open-Domain Question Answering"
```

### Regular usage

For bulk collection of results, you can use `clio_search_iter`:

```python
docs = [row for row in clio_search_iter(url=url, index=index, query=query, chunksize=100)]
```

The results are streamed nicely, so you could write to disk in chunks as you please.

### Advanced usage


In practice, you will want to play with a whole bunch of hyperparameters in order to make the most of your query.

The basic arguments to `clio_search` (and `clio_search_iter`) are:

```
url (str):        The url at which your elasticsearch endpoint can be found
index (str):      The index that you want to query
query (str):      Your query "search" string
fields=[] (list): A list of fields to search
limit (int):      Limit the number of results in `clio_search`
offset (int):     Offset the number of results from the initial document.
```

There are also the filters for the searches, which could be equal:
```
pre_filters=[]:   Any filters to send to Elasticsearch during the *seed query* 
post_filters=[]:  Any filters to send to Elasticsearch during the *expanded query* 
```

Finally(ish) there are a number of hyperparameters to play with, which all (except `n_seed_docs`) map to variables [documented here](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html#mlt-query-term-selection).
```
n_seed_docs=None (int):       Use a maxmimum of this many seed documents.
min_term_freq=1 (int):        Only consider seed terms which occur in all documents with this frequency.
max_query_terms=10 (int):     Maximum number of important terms to identify in the seed documents.
min_doc_frac=0.001 (float):   Only consider seed terms which appear more than this fraction of the seed docs.
max_doc_frac=0.9 (float):     Only consider seed terms which appear less than this fraction of the seed docs.
min_should_match=0.1 (float): Fraction of important terms from the seed docs explicitly required to match.
stop_words=[] (list):         A supplementary list of terms to ignore.
```

Actually finally, any bonus `kwargs` to pass in the POST request to elasticsearch can be passed in via:
```
**kwargs
```

### Words of warning

* The number of results you get back is not stable. [This is expected behaviour of elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/consistent-scoring.html). If the number of documents returned is very important to you, I would roll up your sleeves and use some statistics to make a cut on the `_score` variable of each document. This should give a more stable number of results.
* If you don't set `fields`, expect strange results since documents could be similar for many reasons not reflected in their main text body.
* If your seed query is too generic, or expansive search too broad, expect huge numbers of irrelevant results.
* If your expansive search too narrow, expect zero results.


## `clio-lite` as a serverless deployment for searchkit via AWS Lambda

There is a modified version of the `clio-lite` which has been designed to be deployed as a serverless interface to Elasticsearch which can then be integrated with [searchkit](http://www.searchkit.co/). A working demonstration [can be found here](https://i5mf7l0opc.execute-api.eu-west-1.amazonaws.com/dev/hierarxy/).

In order to deploy to AWS, you can `bash deploy.sh`. The script will (re)deploy based on tags from your GIT repo, assuming a tag-naming convention of `v[0-9]` e.g. `v0` or `v12`. The 'latest' tag (by version number) will be deployed to AWS Lambda if it has not already been deployed. If you delete the corresponding function alias on AWS Lambda, you can redeploy as function again with the same version number.
