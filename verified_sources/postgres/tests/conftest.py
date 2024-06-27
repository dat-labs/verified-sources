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
    yield {'document_streams': []}