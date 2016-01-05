# -*- coding:utf8 -*-

import os
import json
import requests

__author__ = "lqs"

TIMEOUT = 10


class EsProcessing:
    """Dump data and mappings in the type level.

    The class handle all these two cases:
    1) dump data and mappings of all the types of the specified index
    2) dump data and mappings of all the types of all the indices

    Design to work with Elasticsearch API 2.0.
    """
    def __init__(self, uri, index_name=None, dirname=None):
        """
        If the ``index_name`` is None, it will dump data and mappings of all
        the types of all the indices.

        :param uri: es url
        :param index_name: index, default to None.
        :param dirname: the place where to persist the mappings and data.
        :return:
        """
        self.uri = uri
        self.dirname = dirname
        self.index_name = index_name

        self.session = requests.Session()

    @staticmethod
    def make_url(uri, *path, **params):
        if not uri.endswith('/'):
            uri += '/'
        path = '/'.join([i for i in path if i])
        params = '&'.join(['{0}={1}'.format(k, v) for k, v in params.items()])

        if not params:
            return ''.join([uri, path])
        return ''.join([uri, path, '?', params])

    @staticmethod
    def make_fp(path, *args, ext='.json'):
        fp = '.'.join([i.strip('.') for i in args if i])
        return os.path.join(path, fp) + ext

    def query_from_url(self, timeout=TIMEOUT):
        # The url may look like ``/UR/<index>/_mapping`` or ``/URL/_mapping``.
        # If the index is not set, it will query mappings of all types of all
        # indices.
        # Refer to: https://www.elastic.co/guide/en/elasticsearch/reference/2.0/indices-get-mapping.html
        url = self.make_url(self.uri, self.index_name, '_mapping')
        with self.session as s:
            resp = s.get(url, timeout=timeout)

        # if an invalid index_name is given, ES will response an error, and
        # if the mapping of the index_name has not been set, it will
        # response a result with an empty value of mappings.
        data = resp.json()
        if 'error' in data:
            raise AttributeError('Invalid url path')

        if self.index_name and not data[self.index_name].get('mappings'):
            raise AttributeError('Mapping of index {0} has not been set'
                                 .format(self.index_name))

        return data

    def query_mappings(self):
        data = self.query_from_url()

        # Exclude the fields of the index level.
        excludes = ['_default_', '_all', 'properties']
        for i in data:
            mappings = data[i]['mappings']

            for m in mappings:
                if m in excludes:
                    continue

                yield {'index': i, 'type': m,
                       'schema': mappings[m].get('properties')}

    def dump_mapping(self):
        # Persist mapping to local with a name: ``<index>.<type>.mapping.json``
        for i in self.query_mappings():
            fp = self.make_fp(self.dirname, i['index'], i['type'], 'mapping')

            print('Start to dump mapping of {0}.{1}...'
                  .format(i['index'], i['type']))
            with open(fp, 'w') as f:
                json.dump({'properties': i['schema']}, f, ensure_ascii=False)
            print('Dump mapping done.')

    def scroll_data(self, uri, index_name, type_name, scroll='1m'):
        """Use scroll API to query large data.

        We get the scroll_id first, and then, in a while loop, we get the
        documents using the scroll_id returned by the every request util there
        is no scroll_id or document returned.

        Refer to: https://www.elastic.co/guide/en/elasticsearch/reference/2.0/search-request-scroll.html
        """
        type_url = self.make_url(uri, index_name, type_name, '_search',
                                 scroll=scroll, search_type='scan')
        type_resp = self.session.get(type_url).json()

        scroll_id = type_resp.get('_scroll_id')
        if scroll_id is None:
            return

        while True:
            # Argument ``search_type=scan`` is deprecated since Elasticsearch
            # 2.1. Instead, it use ``sort=_doc`` as an optimization of scroll
            # requests.
            # Refer to:
            # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-search-type.html
            # https://www.elastic.co/guide/en/elasticsearch/reference/2.1/search-request-scroll.html
            url = self.make_url(uri, '_search/scroll', search_type='scan',
                                scroll=scroll, scroll_id=scroll_id)
            resp = self.session.get(url).json()

            for hits in resp['hits']['hits']:
                yield hits

            scroll_id = resp.get('_scroll_id')
            if scroll_id is None or not resp['hits']['hits']:
                break

    def dump_data(self):
        # Persist document data to local with a name: ``<index>.<type>.json``
        for i in self.query_mappings():
            fp = self.make_fp(self.dirname, i['index'], i['type'])

            print('Start to dump document of {0}.{1}...'
                  .format(i['index'], i['type']))
            # The size of document of every type of a specified index is
            # usually small, and need not to worry about it will run out of
            # memory.
            with open(fp, 'w') as f:
                data = []
                for hits in self.scroll_data(self.uri, i['index'], i['type']):
                    data.append(hits)
                json.dump(data, f, ensure_ascii=False)
            print('Dump document done.')

    def save(self):
        if not (self.dirname and os.path.exists(self.dirname)):
            raise IOError('Dir {0} does not exist.'.format(self.dirname))

        try:
            self.dump_mapping()
            self.dump_data()
        except Exception:
            raise

    def upload_mapping(self, type_name):
        # Read from the json file ``<index>.<type>.mapping.json`` in the
        # ``self.dirname``, and put the mapping into the relevant mapping uri
        # of the type.
        # Refer to: https://www.elastic.co/guide/en/elasticsearch/reference/2.0/indices-put-mapping.html
        url = self.make_url(self.uri, self.index_name, '_mapping', type_name)
        fp = self.make_fp(self.dirname, self.index_name, type_name, 'mapping')

        if not os.path.exists(fp):
            raise IOError('Mapping file of {0} does not exist.'
                          .format(type_name))

        with open(fp) as f:
            print('Start to upload mapping of {0}.{1}...'
                  .format(self.index_name, type_name))
            r = self.session.put(url, json=json.load(f))
            print(r.status_code, r.text)

    def upload_data(self, type_name):
        # Read from json file ``<index>.<type>.json`` in the ``self.dirname``,
        # and put the json document into the relevant type uri.
        # Refer to: https://www.elastic.co/guide/en/elasticsearch/reference/2.0/docs-index_.html
        fp = self.make_fp(self.dirname, self.index_name, type_name)

        if not os.path.exists(fp):
            raise IOError('Document file of {0} does not exist.'
                          .format(type_name))

        with open(fp) as f:
            data = json.load(f)

            for i in data:
                type_id = i.get('_id')
                type_source = i.get('_source')
                url = self.make_url(self.uri, self.index_name, type_name,
                                    type_id, op_type='create')

                print('Start to upload document of {0}.{1}...'
                      .format(self.index_name, type_name))
                r = self.session.put(url, json=type_source)
                print(r.status_code, r.text)

    def upload(self, type_name=None):
        """PUT data and mapping of the specified ``type_name``.

        If the ``type_name`` is not set, it will search and read every json
        file and put them into the relevant uris.
        """
        if not (self.dirname and os.path.exists(self.dirname)):
            raise IOError('Dir {0} does not exist.'.format(self.dirname))

        try:
            if type_name:
                self.upload_mapping(type_name)
                self.upload_data(type_name)
            else:
                for root, dirs, files in os.walk(self.dirname):
                    for f in files:
                        if not f.endswith('.json'):
                            continue
                        type_name = os.path.splitext(f)[0].rsplit('.')[1]
                        self.upload_mapping(type_name)
                        self.upload_data(type_name)
        except Exception:
            raise

if __name__ == '__main__':
    # ep = EsProcessing(uri='http://192.168.67.4:9200', index_name='ems-other_log-2015.09.15', dirname='/Users/dave/Test/es')
    # ep.save()

    ep = EsProcessing(uri='http://192.168.67.4:9200', index_name='test', dirname='/Users/dave/Test/es')
    ep.upload(type_name='data')