from verified_sources.local_file_system.source import LocalFileSystem
from verified_sources.local_file_system.catalog import LocalFileSystemCatalog
from verified_sources.local_file_system.specs import LocalFileSystemSpecification
from dat_core.pydantic_models import DatConnectionStatus, DatMessage
from conftest import *

  
def test_check(valid_connection_object):
    check_connection_tpl = LocalFileSystem().check(
        config=LocalFileSystemSpecification(
            name='LocalFileSystem',
            connection_specification=valid_connection_object,
            module_name='local_file_system'
        )
    )
    assert isinstance(check_connection_tpl, DatConnectionStatus)
    assert check_connection_tpl.status.name == 'SUCCEEDED'


def test_discover(valid_connection_object):
    _d = LocalFileSystem().discover(
        config=LocalFileSystemSpecification(
            name='LocalFileSystem',
            connection_specification=valid_connection_object,
            module_name='local_file_system'
        )
    )
    assert isinstance(_d, dict)


def test_read(valid_connection_object, valid_catalog_object):
    config = LocalFileSystemSpecification(
        name='LocalFileSystem',
        connection_specification=valid_connection_object,
        module_name='local_file_system'
    )

    local_file_system = LocalFileSystem()
    records = local_file_system.read(
        config=config,
        catalog=LocalFileSystemCatalog(**valid_catalog_object),
    )
    for record in records:
        assert DatMessage.model_validate(record)
        print(record.model_dump_json())
