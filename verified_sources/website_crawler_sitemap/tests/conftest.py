from pytest import fixture

@fixture()
def valid_connection_object():
    yield {
        'site_url':'https://docs.datachannel.co/getting-started/1.0.0/index.html',
        'filter':{
            'max_depth':'1',
            'prefix':'None',
        }
    }

@fixture()
def valid_catalog_object():
    yield {'document_streams': [
        {
            'name':'crawler_sitemap',
            'namespace':'pytest',
            'read_sync_mode':'INCREMENTAL',
            'write_sync_mode':'APPEND',
            'advanced':{
                'splitter_settings': {
                    'splitter_settings':'SPLIT_BY_CHARACTER',
                    'separator': '\\n\\n'
                }
            }
        }
    ]}