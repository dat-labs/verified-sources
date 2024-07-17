import psycopg
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
    valid_catalog = PostgresCatalog(**valid_catalog_object)
    postgres = Postgres()
    records = postgres.read(
        config=config,
        catalog=valid_catalog,
    )
    expected_count = get_record_count(valid_connection_object, f"public.{valid_catalog.document_streams[0].name}")
    cnt_records = 0
    for record in records:
        if record.type.name == 'RECORD':
            cnt_records += 1
            assert DatMessage.model_validate(record)
            assert hasattr(record, 'record')
            doc_chunk = record.record.data.document_chunk
            assert isinstance(doc_chunk, str)
    assert cnt_records == expected_count


def test_read_incremental(valid_connection_object, valid_incremental_catalog_object, valid_stream_state_object):
    config = PostgresSpecification(
        name='Postgres',
        connection_specification=valid_connection_object,
        module_name='postgres'
    )
    _combined_state = {
        'actors': StreamState(**valid_stream_state_object)
    }
    valid_catalog = PostgresCatalog(**valid_incremental_catalog_object)
    postgres = Postgres()
    records = postgres.read(
        config=config,
        catalog=valid_catalog,
        state=_combined_state
    )
    query = (f"SELECT COUNT(*) FROM public.{valid_catalog.document_streams[0].name}"
             f" WHERE {valid_catalog.document_streams[0].cursor_field} >"
             f" '{valid_stream_state_object['data']['updated_at']}'")
    expected_count = get_record_count(valid_connection_object, f"public.{valid_catalog.document_streams[0].name}", query)
    cnt_records = 0
    for record in records:
        if record.type.name == 'RECORD':
            cnt_records += 1
            assert DatMessage.model_validate(record)
            assert hasattr(record, 'record')
            doc_chunk = record.record.data.document_chunk
            assert isinstance(doc_chunk, str)
    assert cnt_records == expected_count

def get_record_count(connection_details, table_name, query=None):
    required_keys = ['host', 'port', 'dbname', 'user', 'password']
    connection_dict = {key: connection_details[key] for key in required_keys if key in connection_details}

    conn = psycopg.connect(**connection_dict)
    cursor = conn.cursor()
    if not query:
        query = f"SELECT COUNT(*) FROM {table_name}"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count
