from verified_sources.amazon_s3.source import AmazonS3
from verified_sources.amazon_s3.catalog import AmazonS3Catalog
from verified_sources.amazon_s3.specs import AmazonS3Specification
from dat_core.pydantic_models import (
    DatConnectionStatus, DatCatalog,
    DatDocumentStream, DatMessage
)
from conftest import *


def test_check(valid_connection_object):
    check_connection_tpl = AmazonS3().check(
        config=AmazonS3Specification(
            name='AmazonS3',
            connection_specification=valid_connection_object,
            module_name='amazon_s3'
        )
    )
    assert isinstance(check_connection_tpl, DatConnectionStatus)
    assert check_connection_tpl.status.name == 'SUCCEEDED'


def test_discover(valid_connection_object):
    _d = AmazonS3().discover(
        config=AmazonS3Specification(
            name='AmazonS3',
            connection_specification=valid_connection_object,
            module_name='amazon_s3'
        )
    )
    assert isinstance(_d, dict)


def test_read(valid_connection_object, valid_catalog_object):
    config = AmazonS3Specification(
        name='AmazonS3',
        connection_specification=valid_connection_object,
        module_name='amazon_s3'
    )

    amazon_s3 = AmazonS3()
    records = amazon_s3.read(
        config=config,
        catalog=AmazonS3Catalog(**valid_catalog_object),
    )
    assert DatMessage.model_validate(next(records))


def test_read_incremental(valid_connection_object, valid_catalog_object, valid_stream_state_object):
    from dat_core.connectors.state_managers import LocalStateManager
    from dat_core.pydantic_models import StreamState

    config = AmazonS3Specification(
        name='AmazonS3',
        connection_specification=valid_connection_object,
        module_name='amazon_s3'
    )
    _combined_state = {
        'my-txt-stream': StreamState(**valid_stream_state_object)
    }
    amazon_s3 = AmazonS3()
    records = amazon_s3.read(
        config=config,
        catalog=AmazonS3Catalog(**valid_catalog_object),
        state=_combined_state
    )
    for record in records:
        if record.type == 'STATE':
            LocalStateManager().save_stream_state(
                record.state.stream, record.state.stream_state)

        assert DatMessage.model_validate(record)
