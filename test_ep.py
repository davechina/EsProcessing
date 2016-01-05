# -*- coding:utf-8 -*-

import pytest
from unittest import mock
from EsProcessing import EsProcessing as EP

__author__ = "lqs"


# @pytest.fixture(params=['http://localhost:9200', 'test', '/tmp/fake'], scope='module')
# def ep(request):
#     return EP(request.param)

@pytest.fixture(scope='module')
def ep():
    return EP('http://localhost:9200', index_name='test', dirname='/tmp/fake')


@pytest.fixture(scope='module')
def mappings():
    data = {
        'test': {
            'mappings': {
                '_default_': {},
                '_all': {},
                'properties': {},
                'data': {
                    'properties': {
                        'content': {},
                        'env': {}
                    }
                }
            }
        }
    }

    return data


def test_make_url():
    url = 'uri/index/_mapping'
    assert url == EP.make_url('uri', 'index', '_mapping')

    url_with_params = 'uri/index/_search?format=pretty'
    assert url_with_params == EP.make_url('uri', 'index', '_search',
                                          format='pretty')


def test_make_fp():
    fp = 'dir/test.json'
    assert fp == EP.make_fp('dir', 'test')


def test_query_from_url(ep, monkeypatch, mappings):
    r = mock.MagicMock()
    r.json.return_value = mappings
    monkeypatch.setattr(ep.session, 'get', mock.MagicMock(return_value=r))

    assert 'test' in ep.query_from_url(timeout=10)


def test_query_from_url_err(ep, monkeypatch):
    r = mock.MagicMock()
    r.json.return_value = {'error': 'err_msg', 'status': 404}
    monkeypatch.setattr(ep.session, 'get', mock.MagicMock(return_value=r))

    with pytest.raises(AttributeError):
        ep.query_from_url(timeout=10)


def test_query_mappings(ep, monkeypatch, mappings):
    monkeypatch.setattr(ep, 'query_from_url',
                        mock.MagicMock(return_value=mappings))
    expected_value = {
        'index': 'test',
        'type': 'data',
        'schema': {
            'content': {},
            'env': {}
        }
    }

    m = ep.query_mappings()
    assert next(m) == expected_value


def  test_dump_mapping(ep, mappings, monkeypatch, tmpdir):
    monkeypatch.setattr(ep, 'query_from_url', mock.MagicMock(return_value=mappings))

    # ``tmpdir`` is a `py.path.local` object, when use it, just wrap it into a
    # str and then pass it to `os.path.join`.
    # we don't usually follow the example about ``tmpdir`` in the
    # documentation of pytest.
    # refer to the answers in the stackoverflow:
    # http://stackoverflow.com/questions/27034001/os-path-join-fails-with-typeerror-object-of-type-localpath-has-no-len
    # http://stackoverflow.com/questions/30012999/combine-httpretty-with-pytest-tmpdir
    import os.path
    fp = os.path.join(str(tmpdir), 'mapping.json')
    monkeypatch.setattr(ep, 'make_fp', mock.MagicMock(return_value=fp))

    ep.dump_mapping()
    assert len(tmpdir.listdir()) > 0

def test_upload_err(ep):
    with pytest.raises(IOError):
        ep.upload()
