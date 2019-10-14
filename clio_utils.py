import json


class ElasticsearchError(Exception):
    pass


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


def extract_docs(r, scroll=False, include_score=False):
    """Extract the raw data and documents from the
    :obj:`requests.Response`"""
    data = json.loads(r.text)
    if 'error' in data:
        raise ElasticsearchError("Failed with POST query "
                                 f"{r.request.body}"
                                 f"\n\nResponse from ES was {data}")
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
    if _scroll_id is not None and scroll:
        total = _scroll_id
    return total, docs


def assert_fraction(x, name='value'):
    if not (0 < x <= 1):
        raise ValueError(f'{name} must be > 0 and <= 1. '
                         f'Invalid value of "{x}" was provided')
