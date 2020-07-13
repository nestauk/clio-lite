import json
import urllib


class ElasticsearchError(Exception):
    pass


def set_headers(kwargs):
    """Set standard headers here"""
    if 'headers' not in kwargs:
        kwargs['headers'] = {}
    kwargs["headers"]["Content-Type"] = "application/json"


def make_endpoint(url, index):
    """Combine the endpoint URL and index into the _search endpoint path"""
    endpoint = url
    if index is not None:
        endpoint = urllib.parse.urljoin(f'{endpoint}/', index)
    endpoint = urllib.parse.urljoin(f'{endpoint}/', '_search')
    return endpoint


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


def unpack_if_safe(r):
    data = json.loads(r.text)
    if 'error' in data:
        raise ElasticsearchError("Failed with POST query "
                                 f"{r.request.body}"
                                 f"\n\nResponse from ES was {data}")
    return data


def extract_keywords(r, agg_name='_keywords'):
    data = unpack_if_safe(r)
    return data['aggregations'][agg_name]['keywords']['buckets']


def extract_docs(r, scroll=None, include_score=False):
    """Extract the raw data and documents from the
    :obj:`requests.Response`"""
    data = unpack_if_safe(r)
    docs = []
    _scroll_id = try_pop(data, '_scroll_id')
    for row in data['hits']['hits']:
        _row = dict(_id=row['_id'],
                    _index=row['_index'],
                    **try_pop(row, '_source', {}))
        if include_score:
            _row['_score'] = row['_score']
        docs.append(_row)

    total = data['hits']['total']
    if _scroll_id is not None and scroll is not None:
        total = _scroll_id
    elif type(total) is dict:  # Breaking change from ES 6.x --> 7.x
        total = total['value']
    return total, docs


def assert_fraction(x, name='value'):
    if not (0 < x <= 1):
        raise ValueError(f'{name} must be > 0 and <= 1. '
                         f'Invalid value of "{x}" was provided')
