import os
from pytest import fixture
from minio import Minio


@fixture()
def valid_connection_object():
    yield {
    }


@fixture()
def valid_catalog_object():

    # create connection to minio
    client = Minio(
        endpoint=os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_USER'),
        secret_key=os.getenv('MINIO_PASS'),
        secure=False
    )
    txt_file_path = 'test_connection.txt'
    bucket_name = os.getenv('MINIO_BUCKET')

    # create a text file
    with open(txt_file_path, 'w') as f:
        # always give credit to the author
        f.write('A Thing of Beauty is a joy forever - John Keats')
    client.fput_object(bucket_name, txt_file_path, txt_file_path)

    # create a csv file 
    csv_file_path = 'test_connection.csv'
    with open(csv_file_path, 'w') as f:
        f.write('name,age\nGanesh Gaitonde,23\nCrimsy,4')
    client.fput_object(bucket_name, csv_file_path, csv_file_path)

    yield {
        'document_streams': [
            {
                "name": "txt",
                "json_schema": {},
                "namespace": "idk4",
                "read_sync_mode": "INCREMENTAL",
                "write_sync_mode": "APPEND",
                "cursor_field": "string",
                "advanced": {
                    "splitter_settings": {
                        "splitter_settings": "SPLIT_BY_CHARACTER",
                        "config": {
                            "separator": "\\n\\n"
                        }
                    }
                },
                "usr_file_path": txt_file_path,
                "obj_file_path": txt_file_path
            },
            {
                "name": "csv",
                "json_schema": {},
                "namespace": "idk4",
                "read_sync_mode": "INCREMENTAL",
                "write_sync_mode": "APPEND",
                "cursor_field": "string",
                "advanced": {
                    "splitter_settings": {
                        "splitter_settings": "SPLIT_BY_CHARACTER",
                        "config": {
                            "separator": "\\n"
                        }
                    }
                },
                "usr_file_path": csv_file_path,
                "obj_file_path": csv_file_path
            }
    ]}

    # delete file from minio
    client.remove_object(bucket_name, txt_file_path)
    client.remove_object(bucket_name, csv_file_path)

    # delete file from local
    os.remove(txt_file_path)
    os.remove(csv_file_path)
