import redshift_connector
from verified_sources.aws_redshift.source import AWSRedshift
from verified_sources.aws_redshift.specs import AWSRedshiftSpecification
from verified_sources.aws_redshift.catalog import AWSRedshiftCatalog
from dat_core.pydantic_models import (
    DatConnectionStatus,
    DatMessage, StreamState
)


def test_check(valid_connection_object):
    check_connection_tpl = AWSRedshift().check(
        config=AWSRedshiftSpecification(
            name='AWSRedshift',
            connection_specification=valid_connection_object,
            module_name='aws_redshift'
        )
    )
    assert isinstance(check_connection_tpl, DatConnectionStatus)
    assert check_connection_tpl.status.name == 'SUCCEEDED'


def test_discover(valid_connection_object):
    _d = AWSRedshift().discover(
        config=AWSRedshiftSpecification(
            name='AWSRedshift',
            connection_specification=valid_connection_object,
            module_name='aws_redshift'
        )
    )
    # Check if the result is a dictionary
    assert isinstance(_d, dict)
    logger.debug(f"Discovered: {_d}")
    with open('discovered_aws_redshift.json', 'w') as f:
        import json
        f.write(json.dumps(_d, indent=1))
    assert isinstance(_d['properties']['document_streams']['items']['anyOf'], list)
    assert isinstance(_d['properties']['document_streams']['items']['anyOf'][0], dict)
    assert 'properties' in _d['properties']['document_streams']['items']['anyOf'][0]
    assert 'name' in _d['properties']['document_streams']['items']['anyOf'][0]['properties']

def test_read(valid_connection_object, valid_catalog_object):
    config = AWSRedshiftSpecification(
        name='AWSRedshift',
        connection_specification=valid_connection_object,
        module_name='aws_redshift'
    )
    valid_catalog = AWSRedshiftCatalog(**valid_catalog_object)
    aws_redshift = AWSRedshift()
    records = aws_redshift.read(
        config=config,
        catalog=valid_catalog,
    )
    expected_count = get_record_count(valid_connection_object, f"dc_dc_warehouse_testing.{valid_catalog.document_streams[0].name}")
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
    config = AWSRedshiftSpecification(
        name='AWSRedshift',
        connection_specification=valid_connection_object,
        module_name='aws_redshift'
    )
    _combined_state = {
        'actors': StreamState(**valid_stream_state_object)
    }
    valid_catalog = AWSRedshiftCatalog(**valid_incremental_catalog_object)
    aws_redshift = AWSRedshift()
    records = aws_redshift.read(
        config=config,
        catalog=valid_catalog,
        state=_combined_state
    )
    query = (f"SELECT COUNT(*) FROM dc_dc_warehouse_testing.{valid_catalog.document_streams[0].name}"
             f" WHERE {valid_catalog.document_streams[0].cursor_field} >"
             f" '{valid_stream_state_object['data']['ts_created']}'")
    expected_count = get_record_count(valid_connection_object, f"dc_dc_warehouse_testing.{valid_catalog.document_streams[0].name}", query)
    cnt_records = 0
    for record in records:
        if record.type.name == 'RECORD':
            cnt_records += 1
            assert DatMessage.model_validate(record)
            assert hasattr(record, 'record')
            doc_chunk = record.record.data.document_chunk
            assert isinstance(doc_chunk, str)
    assert cnt_records == expected_count


def get_record_count(connection_details, table_name=None, query=None):
    required_keys = ['host', 'port', 'database', 'user', 'password']
    connection_dict = {key: connection_details[key] for key in required_keys if key in connection_details}
    with redshift_connector.connect(**connection_dict) as conn:
        with conn.cursor() as cursor:
            if not query:
                query = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(query)
            return cursor.fetchone()[0]
