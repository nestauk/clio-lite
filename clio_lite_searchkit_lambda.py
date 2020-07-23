import json
import requests
import os
from copy import deepcopy
from clio_utils import try_pop
from clio_utils import extract_docs
from clio_utils import unpack_if_safe
from clio_lite import clio_search


def format_response(response):
    """Format the :obj:`requests.Response`, as expected by AWS API Gateway"""
    return {
        "isBase64Encoded": False,
        "statusCode": response.status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True
        },
        "body": make_es7_safe(response)
    }


def extract_fields(q):
    """Extract which fields are being interrogated
    by the default searchkit request"""
    return q["simple_query_string"]["fields"]


def pop_upper_lim(post_filter):
    """Strip out any extreme upper limits from the sk post_filter"""
    lim = int(os.environ['RANGE_UPPER_LIMIT'])
    for field, limits in post_filter.items():
        if field.startswith('year') or field.startswith('date'):
            continue
        if 'lte' in limits and int(limits['lte']) >= lim:
            post_filter[field].pop('lte')


def make_es7_safe(r):
    """Fix hits.total.value breaking change from ES 6.x --> 7.x"""
    data = unpack_if_safe(r)
    total = data['hits']['total']
    if type(total) is dict:
        data['hits']['total'] = total['value']
    return json.dumps(data)


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
    slug = event['pathParameters']['proxy']
    # If not a search query, return
    if not slug.endswith("_search") or 'query' not in query:
        url = f"https://{endpoint}/{slug}"
        r = requests.post(url, data=json.dumps(query),
                          params={"rest_total_hits_as_int": "true"},
                          headers=event['headers'])
        return format_response(r)

    # Convert the request info ready for clio_search
    index = slug[:-8]  # removes "/_search"
    limit = try_pop(query, 'size')
    offset = try_pop(query, 'from')
    min_term_freq = try_pop(query, 'min_term_freq', 1)
    max_query_terms = try_pop(query, 'max_query_terms', 10)
    min_doc_frac = try_pop(query, 'min_doc_frac', 0.001)
    max_doc_frac = try_pop(query, 'max_doc_frac', 0.90)
    min_should_match = try_pop(query, 'minimum_should_match', 0.2)
    old_query = deepcopy(try_pop(query, 'query'))
    fields = extract_fields(old_query)

    # Make the search
    _, r = clio_search(f"https://{endpoint}", index, old_query,
                       fields=fields,
                       limit=limit,
                       offset=offset,
                       min_term_freq=min_term_freq,
                       max_query_terms=max_query_terms,
                       min_doc_frac=min_doc_frac,
                       max_doc_frac=max_doc_frac,
                       min_should_match=min_should_match,
                       post_aggregation=query,
                       response_mode=True,
                       headers=event['headers'])

    return format_response(r)
