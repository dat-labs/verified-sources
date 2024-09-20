from verified_sources.google_cloud_storage.source import GoogleCloudStorage
from verified_sources.google_cloud_storage.catalog import GoogleCloudStorageCatalog
from verified_sources.google_cloud_storage.specs import GoogleCloudStorageSpecification
from dat_core.pydantic_models import DatConnectionStatus, DatDocumentStream
from conftest import *


def test_check(valid_connection_object):
    check_connection_tpl = GoogleCloudStorage().check(
        config=GoogleCloudStorageSpecification(
            name='GoogleCloudStorage',
            connection_specification=valid_connection_object,
            module_name='google_cloud_storage'
        )
    )
    assert isinstance(check_connection_tpl, DatConnectionStatus)
    assert check_connection_tpl.status.name == 'SUCCEEDED'


def test_discover(valid_connection_object):
    _d = GoogleCloudStorage().discover(
        config=GoogleCloudStorageSpecification(
            name='GoogleCloudStorage',
            connection_specification=valid_connection_object,
            module_name='google_cloud_storage'
        )
    )
    assert isinstance(_d, dict)


def test_read(valid_connection_object, valid_catalog_object):
    config = GoogleCloudStorageSpecification(
        name='GoogleCloudStorage',
        connection_specification=valid_connection_object,
        module_name='google_cloud_storage'
    )

    google_cloud_storage = GoogleCloudStorage()
    records = google_cloud_storage.read(
        config=config,
        catalog=GoogleCloudStorageCatalog(**valid_catalog_object),
    )
    print(records)
    for record in records:
        assert DatDocumentStream.model_validate(record)
