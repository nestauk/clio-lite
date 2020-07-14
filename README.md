# `clio-lite`

Expansive ***contextual searches*** of Elasticsearch data, as described in [this blog](https://towardsdatascience.com/big-fast-nlp-with-elasticsearch-72ffd7ef8f2e). For an interactive sense of how this works, see [the arXlive search tool](https://arxlive.org).

[`clio-lite` as a python tool](https://github.com/nestauk/clio-lite#clio-lite-as-python-tool)

[`clio-lite` as a serverless deployment for searchkit via AWS Lambda](https://github.com/nestauk/clio-lite#clio-lite-as-a-serverless-deployment-for-searchkit-via-aws-lambda)

## `clio-lite` as a python tool

### Installation

`pip install --upgrade git+https://github.com/nestauk/clio-lite.git`

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

If you like, you don't need to specify any filters at all, as in the previous very basic example. However, if you're interested in the filtering syntax, I suggest looking at the elasticsearch [documentation on filtering](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-filter-context.html#query-filter-context-ex), and also looking at the [syntax for ranges](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html) and the [query string syntax](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_ranges). This example is also a good place to start:

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
                          fields=fields,
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

### Keywords: getting under the hood

If you'd like to tune your search well (see "Advanced usage"), it's useful to have an idea what terms are being extracted from the seed documents. By using `clio_keywords`, you can do this:

```python
from clio_lite import clio_keywords
query = "BERT"
filters = [{"range":{"year_of_article":{"gte":"2018"}}}]
keywords = clio_keywords(url=url, index=index, query=query, 
                         fields=['textBody_abstract_article','title_of_article'],
                         filters=filters)

for kw in keywords:
    print(kw)                        

>>> {'key': 'bert', 'score': 6047.572145280995}
>>> {'key': 'elmo', 'score': 296.9805747752674}
>>> {'key': 'emotionx', 'score': 278.41499852955593}
>>> {'key': 'devlin', 'score': 263.85527096668466}
>>> {'key': 'gendered', 'score': 224.96621543342454}
>>> {'key': "bert's", 'score': 159.07909433277806}
>>> {'key': 'contextualized', 'score': 107.54581955071448}
>>> {'key': 'xlnet', 'score': 106.7422132379966}
>>> {'key': 'gpt', 'score': 99.54620840751683}
>>> {'key': 'transformers', 'score': 58.5927866966313}
```

### Stop words

By default stop words (extracted via the `stop-words` package) are used. You can inspect these by importing them:

```python
from clio_lite import STOP_WORDS
```

you can append extra stop words as follows:

```python
STOP_WORDS += ['water', 'sugar', 'pie']
```

and otherwise, you can override them completely by specifying them in the function calls (see 'Advanced usage').

### A note on "relevance" scoring

The scoring is given by the tf-idf weighted document similarity of all documents in Elasticsearch, considering only the top `max_query_terms` terms of the `n_seed_docs` documents retrieved from the initial query. Documents which contain none of these terms are explicitly excluded from the search results. In effect, the document similarity is calculated with respect to the tf-idf weighted centroid of the `n_seed_docs` documents. This is the point of `clio-lite`: this "centroid" document should capture contextually similar terms. Note, that because of this procedure, if many unrelated (i.e. low vocabulary overlap) documents are the 'most relevant' then the highest relevance score will be low. See [here for the explicit formula](https://lucene.apache.org/core/4_9_0/core/org/apache/lucene/search/similarities/TFIDFSimilarity.html) used for the calculation of document similarity.

### Advanced usage

In practice, you will want to play with a whole bunch of hyperparameters in order to make the most of your query.

The basic arguments to `clio_search` (and `clio_search_iter`) are:

```
url (str):        The url at which your elasticsearch endpoint can be found
index (str):      The index that you want to query
query (str):      Your query "search" string
fields=[] (list): A list of fields to search
limit (int):      Limit the number of results in `clio_search`
offset (int):     Offset the number of results from the highest ranking document.
```

There are also the filters for the searches, which could be be the same or different from one another:
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

In order to deploy to AWS, you can `bash deploy.sh`: which will (re)deploy based on tags from your GIT repo, assuming a tag-naming convention of `v[0-9]` e.g. `v0` or `v12`. The 'latest' tag (by version number) will be deployed to AWS Lambda if it has not already been deployed. If you delete the corresponding function alias on AWS Lambda, you can redeploy as function again with the same version number.
