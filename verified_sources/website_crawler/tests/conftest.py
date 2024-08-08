from pytest import fixture

@fixture()
def valid_connection_object():
    yield {'site_url': 'https://blog-test-123-1.blogspot.com/'}

@fixture()
def valid_catalog_object():
    yield {'document_streams': [
        {
            'name':'url_crawler',
            'namespace': 'pytest',
            'read_sync_mode': 'INCREMENTAL',
            'write_sync_mode': 'REPLACE',
            'advanced': {
                'splitter_settings': {
                    'strategy': 'SPLIT_BY_CHARACTER'
                }
            }
        },
    ]}