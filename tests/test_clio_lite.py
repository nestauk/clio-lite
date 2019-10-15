import mock
import pytest

#from clio_lite_searchkit_lambda import *
from clio_utils import try_pop
from clio_utils import extract_docs
from clio_utils import assert_fraction
from clio_utils import set_headers
from clio_utils import make_endpoint

from clio_lite import simple_query as c_simple_query
from clio_lite import more_like_this as c_more_like_this
from clio_lite import clio_search as c_search
from clio_lite import clio_search_iter
from clio_lite import combined_score
from clio_lite import clio_keywords


@pytest.fixture
def expected_kw_output():
    return [{'key': 'klinger', 'score': 22.59},
            {'key': 'joel', 'score': 1.648}]


@pytest.fixture
def raw_keyword_scores():
    return {'joel': [{'score': 2.3, 'bg_count': 100},
                     {'score': 1000.3, 'bg_count': 1},
                     {'score': 1.3, 'bg_count': 1000}],
            'klinger': [{'score': 0.3, 'bg_count': 100},
                        {'score': 100.8, 'bg_count': 23}]}


@pytest.fixture
def mlt_kwargs():
    return dict(fields=['b', -32],
                docs=['a', None, 3],
                max_query_terms=20,
                min_term_freq=1,
                min_doc_frac=0.1,
                max_doc_frac=0.2,
                min_should_match=0.5,
                offset=20,
                stop_words=[],
                limit=100,
                total=1000)


@pytest.fixture
def mlt_query(mlt_kwargs):
    return {"query":
            {"bool":
             {"must":
              [{"more_like_this":
                {"fields": mlt_kwargs['fields'],
                 "like": mlt_kwargs['docs'],
                 "min_term_freq": mlt_kwargs['min_term_freq'],
                 "max_query_terms": mlt_kwargs['max_query_terms'],
                 "min_doc_freq": 100,
                 "max_doc_freq": 200,
                 "boost_terms": 1.,
                 "minimum_should_match": '50%',
                 "stop_words":mlt_kwargs['stop_words'],
                 "include": True}}]
              }
             }
            }


def test_set_headers():
    h = 'headers'
    ctype = 'Content-Type'
    appjson = 'application/json'
    for x in ({}, {h: {ctype: appjson}}):
        set_headers(x)
        assert x[h][ctype] == appjson


def test_make_endpoint():
    for (url, index) in (('http://example.com', 'index'),
                         ('http://another.com', 'another_idx')):
        assert make_endpoint(url, index) == f'{url}/{index}/_search'
    assert make_endpoint(url, index=None) == f'{url}/_search'


def test_combined_score(raw_keyword_scores, expected_kw_output):
    assert pytest.approx(combined_score(raw_keyword_scores['joel']), 0.001) == expected_kw_output[1]['score']
    assert pytest.approx(combined_score(raw_keyword_scores['klinger']), 0.01) == expected_kw_output[0]['score']


@mock.patch('clio_lite.simple_query')
def test_clio_keywords(mocked_search, expected_kw_output, raw_keyword_scores):
    mocked_search.side_effect = ([{'key': 'klinger', **raw_keyword_scores['klinger'][1]},
                                  {'key': 'joel', **raw_keyword_scores['joel'][1]}],
                                 [{'key': 'joel', **raw_keyword_scores['joel'][2]}],
                                 [{'key': 'joel', **raw_keyword_scores['joel'][0]},
                                  {'key': 'klinger', **raw_keyword_scores['klinger'][0]}])

    fields = ['a', 'b', 'c']
    kwargs = dict(url='http://www.example.com', query='something',
                  fields=fields, index='blah',
                  filters={'a_pre_filter': None},
                  n_seed_docs=134, max_query_terms=123,
                  post_filters={'a_post_filter': None})
    output_data = clio_keywords(**kwargs)
    assert mocked_search.call_count == len(fields)
    for row in output_data:
        row['score'] = pytest.approx(row['score'], 0.01)
    assert output_data == expected_kw_output


@mock.patch('clio_lite.json')
@mock.patch('clio_lite.requests')
@mock.patch('clio_lite.extract_docs')
def test_c_simple_query_no_filters(mocked_extract, mocked_reqs,
                                   mocked_json):
    # Make an input query fixture and output query fixture
    c_simple_query(endpoint='someurl.com', query='a query',
                   fields=[], filters={},
                   a_kwarg='something', another_kwarg='something')

    args, kwargs = mocked_reqs.post.call_args
    assert len(args) == 0
    assert set(kwargs) == set(['url', 'data', 'params',
                               'a_kwarg', 'another_kwarg'])


@mock.patch('clio_lite.json.dumps', side_effect=lambda x: x)
@mock.patch('clio_lite.requests')
@mock.patch('clio_lite.extract_docs')
def test_c_simple_query_filters(mocked_extract, mocked_reqs,
                                mocked_json):
    # Make an input query fixture and output query fixture
    filters = {'this is a filter': 'abc'}
    c_simple_query(endpoint='someurl.com', query='a query',
                   fields=[], filters=filters,
                   a_kwarg='something', another_kwarg='something')

    args, kwargs = mocked_reqs.post.call_args
    assert len(args) == 0
    assert set(kwargs) == set(['url', 'data', 'params',
                               'a_kwarg', 'another_kwarg'])
    query = kwargs['data']
    assert query['query']['bool']['filter'] == filters


def test_assert_fraction():
    for x in [0.1, 0.2, 0.3, 0.8, 1]:
        assert_fraction(x)
    for x in [-2, -0.2, 0, 1.2, 1.0001]:
        with pytest.raises(ValueError):
            assert_fraction(x)


@mock.patch('clio_lite.json.dumps', side_effect=lambda x: x)
@mock.patch('clio_lite.requests')
@mock.patch('clio_lite.extract_docs')
def test_c_more_like_this_filters(mocked_extract, mocked_reqs,
                                  mocked_json,
                                  mlt_kwargs, mlt_query):
    # Make an input query fixture and output query fixture
    mlt_kwargs['filters'] = [None, None, 23]
    c_more_like_this(endpoint='someurl.com', a_kwarg=None,
                     another_kwarg=None, **mlt_kwargs)
    args, kwargs = mocked_reqs.post.call_args
    assert len(args) == 0
    assert set(kwargs) == set(['url', 'data', 'params',
                               'a_kwarg', 'another_kwarg'])
    query = kwargs['data']
    assert query.pop('from') == mlt_kwargs['offset']
    assert query.pop('size') == mlt_kwargs['limit']
    assert query['query']['bool'].pop('filter') == mlt_kwargs['filters']
    assert query == mlt_query


@mock.patch('clio_lite.json.dumps', side_effect=lambda x: x)
@mock.patch('clio_lite.requests')
@mock.patch('clio_lite.extract_docs')
def test_c_more_like_this_no_filters(mocked_extract, mocked_reqs,
                                     mocked_json,
                                     mlt_kwargs, mlt_query):
    # Make an input query fixture and output query fixture
    c_more_like_this(endpoint='someurl.com', **mlt_kwargs)
    args, kwargs = mocked_reqs.post.call_args
    assert len(args) == 0
    assert set(kwargs) == set(['url', 'data', 'params'])
    query = kwargs['data']
    assert query.pop('from') == mlt_kwargs['offset']
    assert query.pop('size') == mlt_kwargs['limit']
    assert query['query']['bool'].pop('filter') == []
    assert query == mlt_query


@mock.patch('clio_lite.json.dumps', side_effect=lambda x: x)
@mock.patch('clio_lite.requests')
@mock.patch('clio_lite.extract_docs')
def test_c_more_like_this_bad_limit(mocked_extract, mocked_reqs,
                                    mocked_json,
                                    mlt_kwargs, mlt_query):
    mlt_kwargs['offset'] = 100000
    # Make an input query fixture and output query fixture
    c_more_like_this(endpoint='someurl.com', **mlt_kwargs)
    args, kwargs = mocked_reqs.post.call_args
    assert len(args) == 0
    assert set(kwargs) == set(['url', 'data', 'params'])
    query = kwargs['data']
    assert 'from' not in query
    assert query.pop('size') == mlt_kwargs['limit']
    assert query['query']['bool'].pop('filter') == []
    assert query == mlt_query


@mock.patch('clio_lite.json.dumps', side_effect=lambda x: x)
@mock.patch('clio_lite.requests')
@mock.patch('clio_lite.extract_docs')
def test_c_more_like_this_zero_total(mocked_extract, mocked_reqs,
                                     mocked_json,
                                     mlt_kwargs, mlt_query):
    mlt_kwargs['total'] = 0
    # Make an input query fixture and output query fixture
    total, docs = c_more_like_this(endpoint='someurl.com', **mlt_kwargs)
    assert total == 0
    assert docs == []


@mock.patch('clio_lite.simple_query', return_value=(23, []))
@mock.patch('clio_lite.more_like_this', return_value=(10, [1, 2, 3]))
def test_search(mocked_mlt_query, mocked_simple_query):
    # test endpoint
    expected_endpoint = 'http://www.example.com/blah/_search'
    kwargs = dict(url='http://www.example.com', query='something',
                  fields=['a', 'b', 'c'], index='blah',
                  pre_filters={'a_pre_filter': None},
                  n_seed_docs=134,
                  post_filters={'a_post_filter': None},
                  bonus_kwarg1=None, bonus_kwarg2=None)
    total, docs = c_search(**kwargs)
    assert total == 10
    assert docs == [1, 2, 3]

    _args, _kwargs = mocked_simple_query.call_args
    assert len(_args) == 0
    assert _kwargs.pop('headers') == {'Content-Type':
                                      'application/json'}
    assert _kwargs.pop('size') == kwargs.pop('n_seed_docs')
    assert _kwargs == dict(endpoint=expected_endpoint,
                           query=kwargs['query'],
                           fields=kwargs['fields'],
                           filters=kwargs['pre_filters'],
                           bonus_kwarg1=kwargs['bonus_kwarg1'],
                           bonus_kwarg2=kwargs['bonus_kwarg2'])

    _args, _kwargs = mocked_mlt_query.call_args
    assert len(_args) == 0
    assert _kwargs['endpoint'] == expected_endpoint
    assert _kwargs['filters'] == kwargs['post_filters']
    assert _kwargs['bonus_kwarg1'] == kwargs['bonus_kwarg1']
    assert _kwargs['bonus_kwarg2'] == kwargs['bonus_kwarg2']


@mock.patch('clio_lite.requests')
@mock.patch('clio_lite.extract_docs')
@mock.patch('clio_lite.clio_search')
def test_search_iter(mocked_search, mocked_extract_docs, mocked_requests):
    chunksize = 1000
    remainder = 23
    responses = ((None, [None]*chunksize),
                 (None, ['a']*chunksize),
                 (None, ['b']*remainder))
    mocked_search.return_value = (None, [0]*chunksize)
    mocked_extract_docs.side_effect = responses
    data = [row for row in clio_search_iter('https://something.com', 'an_index')]
    assert len(data) == 3*chunksize + remainder  # all rows yielded
    assert mocked_search.call_count == 1  # 3 extract calls + 1 initial call
    assert set(data) == set([0, None, 'a', 'b'])


def test_try_pop():
    data = {'a': 'A', 'b': 'B', 'c': 'C'}
    assert try_pop(data, 'a', 'AA') == 'A'
    assert try_pop(data, 'b', 'BB') == 'B'
    assert try_pop(data, 'c', 'CC') == 'C'

    assert try_pop(data, 'a', 'BB') == 'BB'
    assert try_pop(data, 'b', 'AB') == 'AB'
    assert try_pop(data, 'c', 'CA') == 'CA'

    assert try_pop(data, 'd', 'DD') == 'DD'
    assert try_pop(data, 'd') is None


@mock.patch('clio_utils.json')
def test_extract_docs(mocked_json):
    mocked_response = mock.MagicMock()
    hits = [{'_id': 'something', '_index': 'something',
             '_source': {'something': 'else'}}]*100
    _total = 10
    mocked_json.loads.return_value = {'hits': {'total': _total,
                                               'hits': hits}}
    total, docs = extract_docs(mocked_response)
    assert total == _total
    assert len(docs) == len(hits)
