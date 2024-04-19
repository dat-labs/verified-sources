from pytest import fixture

@fixture()
def valid_connection_object():
    import os
    yield dict(
        aws_access_key = os.environ.get('AWS_ACCESS_KEY'),
        aws_secret_key = os.environ.get('AWS_SECRET_KEY'),
        bucket_name = os.environ.get('S3_BUCKET_NAME'),
        region_name = os.environ.get('S3_REGION'),
        prefix = 'tmp/dat'
    )

@fixture()
def valid_catalog_object():
    yield {'document_streams': [
        {
            'name': 'txt',
            'dir_prefix': 'tmp/dat'
        }
    ]}