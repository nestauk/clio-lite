import json
from botocore.vendored import requests
import os
from copy import deepcopy


def try_pop(x, k, default=None):
    try:
        v = x.pop(k)
    except KeyError:
        v = default
    finally:
        return v


def format_response(response):
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
    q = deepcopy(query).pop('bool')
    q = q["should"][0]["simple_query_string"]["query"]
    new_query = dict(query={"query_string": {"query": q,
                                             "fields":fields}})
    r = requests.post(url, data=json.dumps(new_query),
                      headers=event['headers'],
                      params={"search_type": "dfs_query_then_fetch"})
    return r


def extract_fields(q):
    return q["bool"]["should"][1]["multi_match"]["fields"]


def extract_docs(r):
    data = json.loads(r.text)
    return data, [{'_id': row['_id'], '_index': row['_index']}
                  for row in data['hits']['hits']]


def lambda_handler(event, context=None):
    query = json.loads(event['body'])

    # Extract info from the query as required
    _from = try_pop(query, 'from')
    _size = try_pop(query, 'size')
    min_term_freq = try_pop(query, 'min_term_freq', 1)
    max_query_terms = try_pop(query, 'max_query_terms', 10)
    min_doc_freq = try_pop(query, 'min_doc_freq', 0.001)
    max_doc_frac = try_pop(query, 'max_doc_frac', 0.90)
    minimum_should_match = try_pop(query, 'minimum_should_match',
                                   '30%')

    # Generate the endpoint URL, and validate
    endpoint = event['headers'].pop('es-endpoint')
    if endpoint not in os.environ['ALLOWED_ENDPOINTS'].split(";"):
        raise ValueError(f'{endpoint} has not been registered')
    url = (f"https://{endpoint}/"
           f"{event['pathParameters']['proxy']}")

    if not url.endswith("_search"):
        r = requests.post(url, data=json.dumps(query),
                          headers=event['headers'])
        return format_response(r)

    # Formulate the MLT query
    old_query = deepcopy(try_pop(query, 'query'))
    fields = extract_fields(old_query)
    if old_query is None:
        # Implies that this is just an empty query
        # so re-insert the to/from and return
        query['from'] = _from
        query['size'] = _size
        r = requests.post(url, data=json.dumps(query),
                          headers=event['headers'])
        #print("Default query")
        return format_response(r)

    # Make the initial request
    r = simple_query(url, old_query, event, fields)
    data, docs = extract_docs(r)
    # If no docs, give up
    if len(docs) == 0:
        #print("Initial query failed")
        return format_response(r)

    # Formulate the MLT query
    max_doc_freq = int(max_doc_frac*data['hits']['total'])
    min_doc_freq = int(min_doc_freq*data['hits']['total'])
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
    if _from is not None and _from < len(docs):
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

    if len(docs) > 0:
        #print("MLT query")
        return format_response(r_mlt)
    #print('MLT query failed')
    return format_response(r)
