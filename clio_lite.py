# ENDPOINT IS AN ENVIRONMENTAL PARAMETER
# minimum_should_match is parameters

import json
from botocore.vendored import requests

ENDPOINT = ('https://search-health-scanner-'
            '5cs7g52446h7qscocqmiky5dn4.eu-west-2.es.amazonaws.com')


def try_pop(x, k):
    try:
        v = x.pop(k)
    except KeyError:
        v = None
    finally:
        return v


def lambda_handler(event, context=None):
    url = f"{ENDPOINT}/{event['pathParameters']['proxy']}"
    query = json.loads(event['body'])
    _from = try_pop(query, 'from')
    _size = try_pop(query, 'size')

    # Make the initial request
    r = requests.post(url, data=json.dumps(query),
                      headers=event['headers'])
    data = json.loads(r.text)

    # Formulate the MLT query
    docs = [{'_id': row['_id'], '_index': row['_index']}
            for row in data['hits']['hits']]
    max_doc_freq = int(0.95*data['hits']['total'])
    mlt_query = {"query":
                 {"more_like_this":
                  {"fields": ["body"],
                   "like": docs,
                   "min_term_freq": 2,
                   "max_query_terms": 25,
                   "min_doc_freq": 5,
                   "max_doc_freq": max_doc_freq,
                   "boost_terms": 1.,
                   "minimum_should_match": "50%",
                   "include": True}}}
    if _from is not None:
        mlt_query['from'] = _from
    if _size is not None:
        mlt_query['size'] = _size
    
    # Make the new query and return
    r = requests.post(url, data=json.dumps(mlt_query),
                      headers=event['headers'])
    return {
        "isBase64Encoded": False,
        "statusCode": r.status_code,
        "headers": {},
        "body": r.text
    }
