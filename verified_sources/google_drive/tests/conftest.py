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
                'advanced': {
                    'splitter_settings': {
                        'splitter_settings': 'SPLIT_BY_CHARACTER'
                    }
                }
            }
        ]
    }

@fixture
def valid_stream_state_object():
    import datetime
    yield {
        'data': {
            'dat_last_modified': int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp())
        },
    }

def pytest_sessionfinish(session, exitstatus):
    parent = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    temp_specs = f'tests{os.path.sep}tmp_spec_model.py'
    try:
        os.remove(f'{parent}{os.path.sep}{temp_specs}')
    except FileNotFoundError:
        pass