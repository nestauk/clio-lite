import json
from botocore.vendored import requests
import os


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


def retry_query(r_old, old_query, url, event):
    # Just make a simple query
    try:
        q = old_query.pop("bool")["should"][0]["simple_query_string"]["query"]
        new_query = dict(query={"query_string": {"query": q}},
                         **old_query)
        r_new = requests.post(url, data=json.dumps(new_query),
                              headers=event['headers'])
    except KeyError:
        r_new = r_old
    return r_new


def lambda_handler(event, context=None):
    query = json.loads(event['body'])

    # Extract info from the query as required
    _from = try_pop(query, 'from')
    _size = try_pop(query, 'size')
    min_term_freq = try_pop(query, 'min_term_freq', 1)
    max_query_terms = try_pop(query, 'max_query_terms', 10)
    min_doc_freq = try_pop(query, 'min_doc_freq', 1)
    max_doc_frac = try_pop(query, 'max_doc_frac', 0.90)
    minimum_should_match = try_pop(query, 'minimum_should_match',
                                   '30%')

    # Generate the endpoint URL, and validate
    endpoint = event['headers'].pop('es-endpoint')
    if endpoint not in os.environ['ALLOWED_ENDPOINTS'].split(";"):
        raise ValueError(f'{endpoint} has not been registered')
    url = (f"https://{endpoint}/"
           f"{event['pathParameters']['proxy']}")

    # Make the initial request
    r = requests.post(url, data=json.dumps(query),
                      headers=event['headers'])
    if not url.endswith("_search"):
        return format_response(r)
    data = json.loads(r.text)

    # Formulate the MLT query
    docs = [{'_id': row['_id'], '_index': row['_index']}
            for row in data['hits']['hits']]
    try:
        old_query = query.pop('query')
    except KeyError:
        pass
    else:
        if len(docs) == 0:
            r = retry_query(r, old_query, url, event)
            data = json.loads(r.text)
            docs = [{'_id': row['_id'], '_index': row['_index']}
                    for row in data['hits']['hits']]
    if len(docs) == 0:
        return format_response(r)

    max_doc_freq = int(max_doc_frac*data['hits']['total'])
    mlt_query = {"query":
                 {"more_like_this":
                  {"fields": ["title", "body"],
                   "like": docs,
                   "min_term_freq": min_term_freq,
                   "max_query_terms": max_query_terms,
                   "min_doc_freq": min_doc_freq,
                   "max_doc_freq": max_doc_freq,
                   "boost_terms": 1.,
                   "minimum_should_match": minimum_should_match,
                   "include": True}}}
    if _from is not None:
        mlt_query['from'] = _from
    if _size is not None:
        mlt_query['size'] = _size

    # Make the new query and return
    r_mlt = requests.post(url, data=json.dumps(dict(**query, **mlt_query)),
                          headers=event['headers'])

    # If successful, return
    data = json.loads(r_mlt.text)
    docs = [{'_id': row['_id'], '_index': row['_index']}
            for row in data['hits']['hits']]
    if len(docs) > 0:
        return format_response(r_mlt)
    return format_response(r)
