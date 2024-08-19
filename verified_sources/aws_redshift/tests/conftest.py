import os
from pytest import fixture


@fixture()
def valid_connection_object():
    yield {
        'host': os.getenv('AWS_REDSHIFT_HOST'),
        'port': os.getenv('AWS_REDSHIFT_PORT'),
        'database': os.getenv('AWS_REDSHIFT_DB'),
        'user': os.getenv('AWS_REDSHIFT_USER'),
        'password': os.getenv('AWS_REDSHIFT_PASSWORD'),
        'schemas': [
            'dc_dc_warehouse_testing',
        ]
    }


@fixture()
def valid_catalog_object():
    yield {
        'document_streams': [
            {
                'name': 'aa_sponsored_brands_keywords_wh_rs',
                'namespace': None,
                'read_sync_mode': 'INCREMENTAL',
                'write_sync_mode': 'REPLACE',
                'advanced': {
                    'splitter_settings': {
                        'strategy': 'SPLIT_BY_CHARACTER'
                    }
                },
                'json_schema': {
                    "row_id": {
                        "type": "string"
                    },
                    "campaignid": {
                        "type": "string"
                    },
                    "day": {
                        "type": "date"
                    },
                    "campaignname": {
                        "type": "string"
                    },
                    "ts_created": {
                        "type": "string"
                    }
                }
            }
        ]
    }


@fixture()
def valid_incremental_catalog_object():
    yield {
        'document_streams': [
            {
                'name': 'aa_sponsored_brands_keywords_wh_rs',
                'namespace': None,
                'read_sync_mode': 'INCREMENTAL',
                'cursor_field': 'ts_created',
                'write_sync_mode': 'APPEND',
                'advanced': {
                    'splitter_settings': {
                        'strategy': 'SPLIT_BY_CHARACTER'
                    }
                },
                'json_schema': {
                    "row_id": {
                        "type": "string"
                    },
                    "campaignid": {
                        "type": "string"
                    },
                    "day": {
                        "type": "date"
                    },
                    "campaignname": {
                        "type": "string"
                    },
                    "ts_created": {
                        "type": "string"
                    }
                }
            }
        ]
    }


@fixture
def valid_stream_state_object():
    _date = "2024-06-04 23:31:05.025"
    yield {
        'data': {
            'ts_created': _date
        },
    }
