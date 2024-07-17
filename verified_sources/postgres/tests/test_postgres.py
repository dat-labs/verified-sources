from verified_sources.postgres.source import Postgres
from verified_sources.postgres.specs import PostgresSpecification
from verified_sources.postgres.catalog import PostgresCatalog
from dat_core.pydantic_models import (
    DatConnectionStatus,
    DatMessage, StreamState
)


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
    # Check if the result is a dictionary
    assert isinstance(_d, dict)
    print(f"Discovered: {_d}")
    with open('discovered_postgres.json', 'w') as f:
        import json
        f.write(json.dumps(_d, indent=1))
    assert isinstance(_d['properties']['document_streams']['items']['anyOf'], list)
    assert isinstance(_d['properties']['document_streams']['items']['anyOf'][0], dict)
    assert 'properties' in _d['properties']['document_streams']['items']['anyOf'][0]
    assert 'name' in _d['properties']['document_streams']['items']['anyOf'][0]['properties']
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
    cnt_records = 0
    for record in records:
        if record.type.name == 'RECORD':
            cnt_records += 1
            assert DatMessage.model_validate(record)
            assert hasattr(record, 'record')
            doc_chunk = record.record.data.document_chunk
            assert isinstance(doc_chunk, str)
            dat_document_entity = record.record.data.metadata.dat_document_entity
    assert cnt_records == 7


def test_read_incremental(valid_connection_object, valid_incremental_catalog_object, valid_stream_state_object):
    config = PostgresSpecification(
        name='Postgres',
        connection_specification=valid_connection_object,
        module_name='postgres'
    )
    _combined_state = {
        'public.actors': StreamState(**valid_stream_state_object)
    }
    postgres = Postgres()
    records = postgres.read(
        config=config,
        catalog=PostgresCatalog(**valid_incremental_catalog_object),
        state=_combined_state
    )
    cnt_records = 0
    for record in records:
        if record.type.name == 'RECORD':
            cnt_records += 1
            assert DatMessage.model_validate(record)
            assert hasattr(record, 'record')
            doc_chunk = record.record.data.document_chunk
            assert isinstance(doc_chunk, str)
            dat_document_entity = record.record.data.metadata.dat_document_entity
            assert 'public' in dat_document_entity
    assert cnt_records == 5
