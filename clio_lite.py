from collections import defaultdict
import json
import logging
import math
import os
import requests
from stop_words import get_stop_words
import urllib

from clio_utils import try_pop
from clio_utils import set_headers
from clio_utils import make_endpoint
from clio_utils import extract_docs
from clio_utils import extract_keywords
from clio_utils import assert_fraction


"""
Stop words: Feel free to add to these

e.g. :obj:`from clio_lite import STOP_WORDS; STOP_WORDS += ['water']`
"""
STOP_WORDS = get_stop_words('english')


"""
Maximum chunksize for doc iterator: Feel free to change.

e.g. :obj:`from clio_lite import MAX_CHUNKSIZE; MAX_CHUNKSIZE = 10`
"""
MAX_CHUNKSIZE = 10000


def combined_score(keyword_scores):
    """Combine Lucene keyword scores according to my own recipe,
    which is calculate a weighted combination of the scores,
    combined in quadrature (i.e. assuming that the scores are
    orthogonal).
    """
    numerator, denominator = 0, 0
    for row in keyword_scores:
        s2 = math.pow(row['score'], 2)
        b2 = math.pow(row['bg_count'], 2)  # includes doc_count
        numerator += s2*b2
        denominator += b2
    return math.sqrt(numerator/denominator)


def simple_query(endpoint, query, fields, filters,
                 size=None, aggregations=None,
                 **kwargs):
    """Perform a simple query on Elasticsearch.

    Args:
        url (str): The Elasticsearch endpoint.
        query (str): The query to make to ES.
        fields (list): List of fields to query.
        filters (list): List of ES filters.
        size (int): Number of documents to return.
        aggregations: Do not use this directly. See :obj:`clio_keywords`.
    Returns:
        {total, docs} (tuple): {total number of docs}, {top :obj:`size` docs}
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
    # Assume that if you want aggregations, you don't want anything else
    if aggregations is not None:
        _query['aggregations'] = aggregations
        _query['size'] = 0
        _query.pop('_source')
    elif size is not None:
        _query['size'] = size
    # Make the query
    logging.debug(_query)
    r = requests.post(url=endpoint, data=json.dumps(_query),
                      params={"search_type": "dfs_query_then_fetch"},
                      **kwargs)
    # "Aggregation mode"
    if aggregations is not None:
        return extract_keywords(r)
    return extract_docs(r)


def more_like_this(endpoint, docs, fields, limit, offset,
                   min_term_freq, max_query_terms,
                   min_doc_frac, max_doc_frac,
                   min_should_match, total,
                   stop_words=STOP_WORDS,
                   filters=[], scroll=None, **kwargs):
    """Make an MLT query

    Args:
        endpoint (str): URL path to _search endpoint
        docs (list): Document index and ids to expand from.
        fields (list): List of fields to query.
        limit (int): Number of documents to return.
        offset (int): Offset from the highest ranked document.
        n_seed_docs (int): Use a maxmimum of this many seed documents.
        min_term_freq (int): Only consider seed terms which occur in all
                               documents with this frequency.
        max_query_terms (int): Maximum number of important terms to
                                  identify in the seed documents.
        min_doc_frac (float): Only consider seed terms which appear more
                                    than this fraction of the seed docs.
        max_doc_frac (float): Only consider seed terms which appear less
                                  than this fraction of the seed docs.
        min_should_match (float): Fraction of important terms from the
                                      seed docs explicitly required to match.
        stop_words (list): A supplementary list of terms to ignore. Defaults
                           to standard English stop words.
        filters (list): ES filters to supply to the query.
        scroll (str): ES scroll time window (e.g. '1m').
    Returns:
        {total, docs} (tuple): {total number of docs}, {top :obj:`size` docs}.
    """
    # If there are no documents to expand from
    if total == 0:
        return (0, [])
    # Check that the fractions are fractions, to avoid weird behaviour
    assert_fraction(min_should_match)
    assert_fraction(min_doc_frac)
    assert_fraction(max_doc_frac)

    # Formulate the MLT query
    msm = int(min_should_match*100)
    max_doc_freq = int(max_doc_frac*total)
    min_doc_freq = int(min_doc_frac*total)
    mlt = {
        "more_like_this": {
            "fields": fields if fields != [] else None,
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
    # Offset assumes no scrolling (since it would be invalid)
    if offset is not None and offset < total:
        _query['from'] = offset
    # If scrolling was specified
    elif scroll is not None:
        params['scroll'] = scroll
    # The number of docs returned
    if limit is not None:
        _query['size'] = limit
    # Make the query
    logging.debug(_query)
    r = requests.post(url=endpoint,
                      data=json.dumps(_query),
                      params=params,
                      **kwargs)
    # If successful, return
    return extract_docs(r, scroll=scroll, include_score=True)


def clio_keywords(url, index, fields, max_query_terms=10,
                  filters=[], stop_words=STOP_WORDS,
                  shard_size=5000,
                  **kwargs):
    """Discover keywords associated with a seed query.

    Args:
        url (str): URL path to bare ES endpoint.
        index (str): Index to query.
        fields (list): List of fields to query.
        max_query_terms (int): Maximum number of important terms to
                               identify in the seed documents.
        stop_words (list): A supplementary list of terms to ignore. Defaults
                           to standard English stop words.
        shard_size (int): ES shard_size (increases sample doc size).
    Returns:
        keywords (list): A list of keywords and their scores.
    """
    set_headers(kwargs)
    endpoint = make_endpoint(url, index)

    # Formulate the aggregation query
    keyword_agg = {
        "_keywords": {
            "sampler": {"shard_size": shard_size},
            "aggregations": {
                "keywords": {
                    "significant_text": {
                        "size": max_query_terms,
                        "jlh": {}
                    }
                }
            }
        }
    }
    # The aggregation can only be performed once per field,
    # so terms can be given multiple scores across fields.
    data = defaultdict(list)  # Mapping of term to scores for that term
    for field in fields:
        # Set the field and make the query
        (keyword_agg['_keywords']['aggregations']['keywords']
                    ['significant_text']['field']) = field
        kws = simple_query(endpoint=endpoint, fields=[field],
                           filters=filters, aggregations=keyword_agg, **kwargs)
        # Append keywords if not stop words
        for kw in kws:
            word = kw.pop('key')
            if word in stop_words:
                continue
            data[word].append(kw)

    # Calculate a combined score for each word, and sort by score
    keywords = sorted((dict(key=word, score=combined_score(info))
                       for word, info in data.items()),
                      key=lambda kw: kw['score'], reverse=True)
    return keywords


def _clio_search(url, index, query,
                fields=[], n_seed_docs=None,
                limit=None, offset=None,
                min_term_freq=1, max_query_terms=10,
                min_doc_frac=0.001, max_doc_frac=0.9,
                min_should_match=0.1, pre_filters=[],
                post_filters=[], stop_words=STOP_WORDS,
                scroll=None, **kwargs):

    

def clio_search(url, index, query,
                fields=[], n_seed_docs=None,
                limit=None, offset=None,
                min_term_freq=1, max_query_terms=10,
                min_doc_frac=0.001, max_doc_frac=0.9,
                min_should_match=0.1, pre_filters=[],
                post_filters=[], stop_words=STOP_WORDS,
                scroll=None, **kwargs):
    """Perform a contextual search of Elasticsearch data.

    Args:
        url (str): URL path to bare ES endpoint.
        index (str): Index to query.
        query (str): The simple text query to Elasticsearch.
        fields (list): List of fields to query.
        n_seed_docs (int): Number of seed documents to retrieve.
        limit (int): Number of documents to return.
        offset (int): Offset from the highest ranked document.
        n_seed_docs (int): Use a maxmimum of this many seed documents.
        min_term_freq (int): Only consider seed terms which occur in all
                               documents with this frequency.
        max_query_terms (int): Maximum number of important terms to
                                  identify in the seed documents.
        min_doc_frac (float): Only consider seed terms which appear more
                                    than this fraction of the seed docs.
        max_doc_frac (float): Only consider seed terms which appear less
                                  than this fraction of the seed docs.
        min_should_match (float): Fraction of important terms from the
                                      seed docs explicitly required to match.
        {pre,post}_filters (list): ES filters to supply to the
                                   {seed,expanded} queries.
        stop_words (list): A supplementary list of terms to ignore. Defaults
                           to standard English stop words.
        scroll (str): ES scroll time window (e.g. '1m').
    Returns:
        {total, docs} (tuple): {total number of docs}, {top :obj:`size` docs}.
    """
    set_headers(kwargs)
    endpoint = make_endpoint(url, index)
    # Make the seed query
    total, docs = simple_query(endpoint=endpoint,
                               query=query,
                               fields=fields,
                               size=n_seed_docs,
                               filters=pre_filters,
                               **kwargs)

    # May as well break out early if there aren't any hits
    if total == 0:
        return total, docs

    # Make the expanded search query
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
    """Perform a *bulk* (streamed) contextual search of Elasticsearch data.

    Args:
        url (str): URL path to bare ES endpoint.
        index (str): Index to query.
        chunksize (int): Chunk size to retrieve from Elasticsearch.
        query (str): The simple text query to Elasticsearch.
        fields (list): List of fields to query.
        n_seed_docs (int): Number of seed documents to retrieve.
        min_term_freq (int): Only consider seed terms which occur in all
                               documents with this frequency.
        max_query_terms (int): Maximum number of important terms to
                                  identify in the seed documents.
        min_doc_frac (float): Only consider seed terms which appear more
                                    than this fraction of the seed docs.
        max_doc_frac (float): Only consider seed terms which appear less
                                  than this fraction of the seed docs.
        min_should_match (float): Fraction of important terms from the
                                      seed docs explicitly required to match.
        {pre,post}_filters (list): ES filters to supply to the
                                   {seed,expanded} queries.
        stop_words (list): A supplementary list of terms to ignore. Defaults
                           to standard English stop words.
        scroll (str): ES scroll time window (e.g. '1m').
    Yields:
        Single rows of data
    """
    try_pop(kwargs, 'limit')  # Ignore limit and offset
    try_pop(kwargs, 'offset')
    if chunksize > MAX_CHUNKSIZE:
        logging.warning(f'Will not consider chunksize greater than {MAX_CHUNKSIZE}. '
                        f'Reverting to chunksize={MAX_CHUNKSIZE}.')
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
