from verified_sources.postgres.source import Postgres
from verified_sources.postgres.catalog import PostgresCatalog
from verified_sources.postgres.specs import PostgresSpecification
from dat_core.pydantic_models import DatConnectionStatus, DatDocumentStream


def test_check(valid_connection_object):
    check_connection_tpl = Postgres().check(
        config=PostgresSpecification(
            name='Postgres',
            connection_specification=valid_connection_object,
            module_name='postgres'
        )
    )
    assert isinstance(check_connection_tpl, DatConnectionStatus)
    assert check_connection_tpl.status.name == 'SUCCEEDED'


def test_discover(valid_connection_object):
    _d = Postgres().discover(
        config=PostgresSpecification(
            name='Postgres',
            connection_specification=valid_connection_object,
            module_name='postgres'
        )
    )
    assert isinstance(_d, dict)
    print(_d)
    assert False


def test_read(valid_connection_object, valid_catalog_object):
    config = PostgresSpecification(
        name='Postgres',
        connection_specification=valid_connection_object,
        module_name='postgres'
    )

    postgres = Postgres()
    records = postgres.read(
        config=config,
        catalog=PostgresCatalog(**valid_catalog_object),
    )
    for record in records:
        assert DatDocumentStream.model_validate(record)
