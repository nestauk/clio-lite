from stop_words import get_stop_words
import urllib
import json
import requests
import os
import logging
from collections import defaultdict
import math

from clio_utils import try_pop
from clio_utils import extract_docs
from clio_utils import extract_keywords
from clio_utils import assert_fraction


STOP_WORDS = get_stop_words('english')


def simple_query(endpoint, query, fields, filters,
                 aggregations=None, **kwargs):
    """Perform a simple query on Elasticsearch.

    Args:
        url (str): The Elasticsearch endpoint.
        query (str): The query to make to ES.
        fields (list): List of fields to query.
    Returns:
        The ES request response.
    """
    _query = {
        "_source": False,
        "query": {
            "bool": {
                "must": [{"multi_match": {"query": query.lower(),
                                          "fields": fields}}],
                "filter": filters
            }
        }
    }
    if aggregations is not None:
        _query['aggregations'] = aggregations
        _query['size'] = 0
        _query.pop('_source')

    r = requests.post(url=endpoint, data=json.dumps(_query),
                      params={"search_type": "dfs_query_then_fetch"},
                      **kwargs)
    r.raise_for_status()
    if aggregations is not None:
        return extract_keywords(r)
    else:
        return extract_docs(r)


def more_like_this(endpoint, docs, fields, limit, offset,
                   min_term_freq, max_query_terms,
                   min_doc_frac, max_doc_frac,
                   min_should_match, total, stop_words,
                   filters=[], scroll=None, **kwargs):
    if total == 0:
        return (0, [])
    assert_fraction(min_should_match)
    assert_fraction(min_doc_frac)
    assert_fraction(max_doc_frac)
    if stop_words is None:
        stop_words = STOP_WORDS

    # Formulate the MLT query
    msm = int(min_should_match*100)
    max_doc_freq = int(max_doc_frac*total)
    min_doc_freq = int(min_doc_frac*total)

    if fields == []:
        fields = None
    mlt = {
        "more_like_this": {
            "fields": fields,
            "like": docs,
            "min_term_freq": min_term_freq,
            "max_query_terms": max_query_terms,
            "min_doc_freq": min_doc_freq,
            "max_doc_freq": max_doc_freq,
            "boost_terms": 1,
            "stop_words": stop_words,
            "minimum_should_match": f'{msm}%',
            "include": True,
        }
    }
    _query = {"query": {"bool": {"filter": filters, "must": [mlt]}}}
    params = {"search_type": "dfs_query_then_fetch"}
    if offset is not None and offset < total:
        _query['from'] = offset
    elif scroll is not None:
        params['scroll'] = scroll

    if limit is not None:
        _query['size'] = limit

    logging.debug(_query)
    r = requests.post(url=endpoint,
                      data=json.dumps(_query),
                      params=params,
                      **kwargs)
    # If successful, return
    return extract_docs(r, scroll=scroll, include_score=True)


def find_max(a, b, k):
    return max((a[k], b[k]))


def clio_keywords(url, index, fields, max_query_terms=20,
                  filters=[], stop_words=None, shard_size=5000,
                  **kwargs):
    endpoint = url
    if index is not None:
        endpoint = urllib.parse.urljoin(f'{endpoint}/', index)
    endpoint = urllib.parse.urljoin(f'{endpoint}/', '_search')

    if 'headers' not in kwargs:
        kwargs['headers'] = {}
    kwargs["headers"]["Content-Type"] = "application/json"

    keyword_agg = {
        "_keywords": {
            "sampler": {"shard_size": shard_size},
            "aggregations": {
                "keywords": {
                    "significant_text": {
                        "size": 50,
                        "jlh": {}
                    }
                }
            }
        }
    }
    if stop_words is None:
        stop_words = STOP_WORDS
    data = defaultdict(list)
    for field in fields:
        (keyword_agg['_keywords']['aggregations']['keywords']
                    ['significant_text']['field']) = field
        kws = simple_query(endpoint=endpoint, fields=[field],
                           filters=filters, aggregations=keyword_agg,
                           **kwargs)
        for kw in kws:
            word = kw.pop('key')
            if word in stop_words:
                continue
            data[word].append(kw)

    keywords = []
    for word, info in data.items():
        numerator, denominator = 0, 0
        for row in info:
            s2 = math.pow(row['score'], 2)
            b2 = math.pow(row['bg_count'], 2)  # includes doc_count
            numerator += s2*b2
            denominator += b2
        keywords.append(dict(key=word, score=math.sqrt(numerator/denominator)))

    keywords = sorted(keywords, key=lambda kw: kw['score'], reverse=True)
    if len(keywords) > max_query_terms:
        keywords = keywords[:max_query_terms]
    return keywords


def clio_search(url, index, query,
                fields=[], n_seed_docs=None,
                limit=None, offset=None,
                min_term_freq=1, max_query_terms=20,
                min_doc_frac=0.001, max_doc_frac=0.9,
                min_should_match=0.1, pre_filters=[],
                post_filters=[], stop_words=None,
                scroll=None, **kwargs):
    if "headers" not in kwargs:
        kwargs["headers"] = {}
    kwargs["headers"]["Content-Type"] = "application/json"

    endpoint = url
    if index is not None:
        endpoint = urllib.parse.urljoin(f'{endpoint}/', index)
    endpoint = urllib.parse.urljoin(f'{endpoint}/', '_search')

    total, docs = simple_query(endpoint=endpoint,
                               query=query,
                               fields=fields,
                               filters=pre_filters,
                               **kwargs)
    docs = docs[:n_seed_docs]
    total, docs = more_like_this(endpoint=endpoint,
                                 docs=docs, fields=fields,
                                 limit=limit, offset=offset,
                                 min_term_freq=min_term_freq,
                                 max_query_terms=max_query_terms,
                                 min_doc_frac=min_doc_frac,
                                 max_doc_frac=max_doc_frac,
                                 min_should_match=min_should_match,
                                 total=total,
                                 stop_words=stop_words,
                                 filters=post_filters,
                                 scroll=scroll,
                                 **kwargs)
    return total, docs


def clio_search_iter(url, index, chunksize=1000, scroll='1m', **kwargs):
    try_pop(kwargs, 'limit')
    try_pop(kwargs, 'offset')
    if chunksize > 1000:
        logging.warning('Will not consider chunksize greater than 1000. '
                        'Reverting to chunksize=1000.')
    # First search
    scroll_id, docs = clio_search(url=url, index=index,
                                  limit=chunksize, scroll=scroll, **kwargs)
    for row in docs:
        yield row

    # Keep scrolling if required
    endpoint = urllib.parse.urljoin(f'{url}/', '_search/scroll')
    while len(docs) == chunksize:
        r = requests.post(endpoint,
                          data=json.dumps({'scroll': scroll,
                                           'scroll_id': scroll_id}),
                          headers={'Content-Type': 'application/json'})
        _, docs = extract_docs(r)
        for row in docs:
            yield row
