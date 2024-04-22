from pytest import fixture

@fixture()
def valid_connection_object():
    import os
    yield dict(
        aws_access_key = os.environ.get('AWS_ACCESS_KEY'),
        aws_secret_key = os.environ.get('AWS_SECRET_KEY'),
        bucket_name = os.environ.get('S3_BUCKET_NAME'),
        region_name = os.environ.get('S3_REGION'),
    )

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
                    'strategy': 'SPLIT_BY_CHARACTER'
                }
            }
        }
    ]}

@fixture
def valid_stream_state_object():
    import datetime
    yield {
        'data': {
            'dat_last_modified': int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp())
        },
    }