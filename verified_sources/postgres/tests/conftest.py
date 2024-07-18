import os
from pytest import fixture

@fixture()
def valid_connection_object():
    yield {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'schemas': ['public']
    }

@fixture()
def valid_catalog_object():
    yield {'document_streams': [
        {
            'name': 'actors',
            'namespace': 'postgres_actors',
            'read_sync_mode': 'FULL_REFRESH',
            'write_sync_mode': 'APPEND',
            'advanced': {
                'splitter_settings': {
                    'strategy': 'SPLIT_BY_CHARACTER'
                }
            }
        }
    ]}

@fixture()
def valid_incremental_catalog_object():
    yield {'document_streams': [
        {
            'name': 'actors',
            'namespace': 'postgres_actors',
            'read_sync_mode': 'INCREMENTAL',
            'cursor_field': 'updated_at',
            'write_sync_mode': 'APPEND',
            'advanced': {
                'splitter_settings': {
                    'strategy': 'SPLIT_BY_CHARACTER'
                }
            }
        }
    ]}

@fixture
def valid_stream_state_object():
    _date = "2024-06-04 23:31:05.025"
    yield {
        'data': {
            'updated_at': _date
        },
    }
