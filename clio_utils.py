import json


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


def extract_docs(r, include_source=False):
    """Extract the raw data and documents from the
    :obj:`requests.Response`"""
    data = json.loads(r.text)
    return data['hits']['total'], [dict(_id=row['_id'],
                                        _index=row['_index'],
                                        **row['_source'])
                                   for row in data['hits']['hits']]


def assert_fraction(x, name='value'):
    if not (0 < x <= 1):
        raise ValueError(f'{name} must be > 0 and <= 1. '
                         f'Invalid value of "{x}" was provided')
