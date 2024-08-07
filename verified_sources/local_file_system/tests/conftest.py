from pytest import fixture

@fixture()
def valid_connection_object():
    yield {
        'file_name': 'breathless.txt',
        'bucket_name': 'test',
        'file_path': 'hello/hunny/bunny.txt'
    }

@fixture()
def valid_catalog_object():
    yield {'document_streams': [
        {
            'name': 'txt',
            'namespace': 'my-txt-stream',
            'dir_prefix': 'tmp/dat',
            'read_sync_mode': 'INCREMENTAL',
            'write_sync_mode': 'REPLACE',
            'advanced': {
                'splitter_settings': {
                    'splitter_settings': 'SPLIT_BY_CHARACTER'
                }
            }
        }
    ]}
