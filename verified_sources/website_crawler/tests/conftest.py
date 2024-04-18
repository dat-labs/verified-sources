from pytest import fixture

@fixture()
def valid_connection_object():
    yield {'site_url': 'https://www.example.com'}

@fixture()
def valid_catalog_object():
    yield {'document_streams': []}