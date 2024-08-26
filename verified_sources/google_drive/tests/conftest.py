from pytest import fixture
import os

@fixture()
def valid_connection_object():
    yield {
            "client_id": os.getenv('GOOGLE_DRIVE_CLIENT_ID'),
            "client_secret": os.getenv('GOOGLE_DRIVE_CLIENT_SECRET'),
            "refresh_token": os.getenv('GOOGLE_DRIVE_REFRESH_TOKEN'),
    }

@fixture()
def valid_catalog_object():
    yield {
        'document_streams': [
            {
                'name': 'pdf',
                'namespace': 'pytest',
                'dir_uris': ['bak/MySQL/STAGING/for-dat-gdrive-test', ],
                'read_sync_mode': 'INCREMENTAL',
                'write_sync_mode': 'APPEND',
            },
            {
                'name': 'txt',
                'namespace': 'pytest',
                'dir_uris': ['bak/MySQL/STAGING/for-dat-gdrive-test', ],
                'read_sync_mode': 'INCREMENTAL',
                'write_sync_mode': 'APPEND',
            }
        ]
    }