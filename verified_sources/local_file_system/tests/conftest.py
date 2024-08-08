import os
from pytest import fixture
from minio import Minio

@fixture()
def valid_connection_object():
    # create a temp file and upload it into minio
    client = Minio(
        endpoint=os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_USER'),
        secret_key=os.getenv('MINIO_PASS'),
        secure=False
    )
    file_path = 'test_connection.txt'
    bucket_name = os.getenv('MINIO_BUCKET')

    # write a file
    with open(file_path, 'w') as f:
        f.write('A Thing of Beauty is joy forever - John Keats')
    client.fput_object(bucket_name, file_path, file_path)

    yield {
        'obj_file_path': file_path,
        'usr_file_path': file_path  # usr_file_path is dummy as it is not used in the source code
    }

    # delete the file from minio
    client.remove_object(bucket_name, file_path)

    # delete the file from local if exists
    if os.path.exists(file_path):
        os.remove(file_path)


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
        },
                {
            'name': 'txt',
            'namespace': 'my-txt-stream',
            'dir_prefix': 'tmp/dat',
            'read_sync_mode': 'FULL_REFRESH',
            'write_sync_mode': 'APPEND',
            'advanced': {
                'splitter_settings': {
                    'splitter_settings': 'SPLIT_BY_CHARACTER_RECURSIVELY'
                }
            }
        }
    ]}
