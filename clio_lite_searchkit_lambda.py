import json
from botocore.vendored import requests
import os
from copy import deepcopy
from clio_utils import try_pop
from clio_utils import extract_docs


def format_response(response):
    """Format the :obj:`requests.Response`, as expected by AWS API Gateway"""
    return {
        "isBase64Encoded": False,
        "statusCode": response.status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True
        },
        "body": response.text
    }


def simple_query(url, query, event, fields):
    """Perform a simple query on Elasticsearch.

    Args:
        url (str): The Elasticsearch endpoint.
        query (str): The query to make to ES.
        event (dict): The event passed to the lambda handler.
        fields (list): List of fields to query.
    Returns:
        The ES request response.
    """
    q = deepcopy(query).pop('bool')  # Don't mess with the original query
    q = q["should"][0]["simple_query_string"]["query"]
    q = q.lower()
    new_query = dict(query={"query_string": {"query": q,
                                             "fields":fields}})
    r = requests.post(url, data=json.dumps(new_query),
                      headers=event['headers'],
                      params={"search_type": "dfs_query_then_fetch"})
    return r


def extract_fields(q):
    """Extract which fields are being interrogated 
    by the default searchkit request"""
    return q["bool"]["should"][1]["multi_match"]["fields"]


def pop_upper_lim(post_filter):
    """Strip out any extreme upper limits from the sk post_filter"""
    lim = int(os.environ['RANGE_UPPER_LIMIT'])
    for field, limits in post_filter.items():
        if field.startswith('year') or field.startswith('date'):
            continue
        if 'lte' in limits and int(limits['lte']) >= lim:
            post_filter[field].pop('lte')


def lambda_handler(event, context=None):
    """The 'main' function: Process the API Gateway Event
    passed to Lambda by
    performing an expansion on the original ES query."""

    query = json.loads(event['body'])

    # Strip out any extreme upper limits from the post_filter
    try:
        post_filter = query['post_filter']
    except KeyError:
        pass
    else:
        print(post_filter)
        if 'range' in post_filter:
            pop_upper_lim(post_filter['range'])
        elif 'bool' in post_filter:
            for row in post_filter['bool']['must']:
                if 'range' not in row:
                    continue
                pop_upper_lim(row['range'])

    # Generate the endpoint URL, and validate
    endpoint = event['headers'].pop('es-endpoint')
    if endpoint not in os.environ['ALLOWED_ENDPOINTS'].split(";"):
        raise ValueError(f'{endpoint} has not been registered')

    url = f"https://{endpoint}/{event['pathParameters']['proxy']}"
    # If not a search query, return
    if not url.endswith("_search") or 'query' not in query:
        r = requests.post(url, data=json.dumps(query),
                          headers=event['headers'])
        return format_response(r)

    # Extract info from the query as required
    _from = try_pop(query, 'from')
    _size = try_pop(query, 'size')
    min_term_freq = try_pop(query, 'min_term_freq', 1)
    max_query_terms = try_pop(query, 'max_query_terms', 10)
    min_doc_freq = try_pop(query, 'min_doc_freq', 0.001)
    max_doc_frac = try_pop(query, 'max_doc_frac', 0.90)
    minimum_should_match = try_pop(query, 'minimum_should_match',
                                   '20%')

    # Make the initial request
    old_query = deepcopy(try_pop(query, 'query'))
    fields = extract_fields(old_query)
    r = simple_query(url, old_query, event, fields)
    total, docs = extract_docs(r)
    # If no results, give up
    if total == 0:
        return format_response(r)

    # Formulate the MLT query
    max_doc_freq = int(max_doc_frac*total)
    min_doc_freq = int(min_doc_freq*total)
    mlt_query = {"query":
                 {"more_like_this":
                  {"fields": fields,
                   "like": docs,
                   "min_term_freq": min_term_freq,
                   "max_query_terms": max_query_terms,
                   "min_doc_freq": min_doc_freq,
                   "max_doc_freq": max_doc_freq,
                   "boost_terms": 1.,
                   "minimum_should_match": minimum_should_match,
                   "include": True}}}
    if _from is not None and _from < total:
        mlt_query['from'] = _from
    if _size is not None:
        mlt_query['size'] = _size

    # Make the new query and return
    r_mlt = requests.post(url, data=json.dumps(dict(**query,
                                                    **mlt_query)),
                          headers=event['headers'],
                          params={"search_type": "dfs_query_then_fetch"})
    # If successful, return
    return format_response(r_mlt)
