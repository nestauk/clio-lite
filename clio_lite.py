import json
from botocore.vendored import requests
import os
from copy import deepcopy


def try_pop(d, k, default=None):
    """Pop a key from a dict, with a default
    value if the key doesn't exist

    Args:
        d (dict): The dict to pop
        k: The key to pop from the dict
        default: The default value to return, should `k` not exist in `d`.
    Returns:
        v: A value at key `k`
    """
    try:
        v = d.pop(k)
    except KeyError:
        v = default
    finally:
        return v


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


def extract_docs(r):
    """Extract the raw data and documents from the :obj:`requests.Response`"""    
    data = json.loads(r.text)
    return data, [{'_id': row['_id'], '_index': row['_index']}
                  for row in data['hits']['hits']]


def lambda_handler(event, context=None):
    """The 'main' function: Process the API Gateway Event
    passed to Lambda by
    performing an expansion on the original ES query."""

    query = json.loads(event['body'])

    # Strip out any extreme upper limits from the post_filter
    try:
        lim = int(os.environ['RANGE_UPPER_LIMIT'])
        for field, limits in query['post_filter']['range'].items():
            if field.startswith('year') or field.startswith('date'):
                continue
            if 'lte' in limits and int(limits['lte']) >= lim:
                query['post_filter']['range'][field].pop('lte')
    except KeyError:
        pass

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
    data, docs = extract_docs(r)
    # If no results, give up
    if len(docs) == 0:
        return format_response(r)

    # Formulate the MLT query
    total = data['hits']['total']
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
    _data, docs = extract_docs(r_mlt)
    return format_response(r_mlt)
