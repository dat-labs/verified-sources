from typing import Any, Generator
import psycopg
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import (
    DatCatalog, DatDocumentStream,
    DatMessage
)
from dat_core.loggers import logger
from verified_sources.postgres.specs import PostgresSpecification


class PostgresStream(Stream):
    """
    postgresStream0 class for crawling and processing URLs.

    Attributes:
        _name (str): The name of the postgresStream0 stream ('url_crawler').

    Methods:
        __init__: Initializes a new postgresStream0 object.
        read_records: Reads records from the configured stream and yields DatMessage objects.
    """

    # _name = 'public.actors' #Leaving this as is; we are setting it dynamically in the connector
    _schema = ""
    _table_name = ""

    def __init__(self, config: PostgresSpecification) -> None:
        """
        Initializes a new postgresStream0 object.

        Parameters:
            config (PostgresSpecification): The configuration object for URL crawling.
        """
        self._config = config
        self.connection = psycopg.connect(
            conninfo=(
                f"host={config.connection_specification.host} port={config.connection_specification.port} "
                f"dbname={config.connection_specification.dbname} user={config.connection_specification.user} "
                f"password={config.connection_specification.password}"
            )
        )

    def read_records(self,
        catalog: DatCatalog,
        configured_stream: DatDocumentStream,
        cursor_value: Any = None
    ) -> Generator[DatMessage, Any, Any]:
        """
        Reads records from the configured stream and yields DatMessage objects.

        Parameters:
            catalog (DatCatalog): The DatCatalog object.
            configured_stream (DatDocumentStream): The configured DatDocumentStream object.
            cursor_value (Any, optional): The cursor value (default: None).

        Yields:
            Generator[DatMessage, Any, Any]: A generator yielding DatMessage objects.
        """
        cursor = self.connection.cursor()
        cursor_field = getattr(configured_stream, 'cursor_field', None)
        upsert_keys = getattr(configured_stream, 'upsert_keys', [])
        fields = ", ".join(getattr(configured_stream, 'json_schema', {}).keys())
        if cursor_field and cursor_value is not None:
            query = f"SELECT {fields} FROM {self._schema}.{self._table_name} WHERE {cursor_field} > %s order by {cursor_field} ASC"
            logger.debug(f"Query: {query}")
            cursor.execute(query, (cursor_value,))
        else:
            query = f"SELECT {fields} FROM {self._schema}.{self._table_name}"
            logger.debug(f"Query: {query}")
            cursor.execute(query)

        records = cursor.fetchall()

        for record in records:
            record_dict = dict(
                zip([column.name for column in cursor.description], record))
            record_str = ", ".join(
                [f"{k}: {v}" for k, v in record_dict.items()])
            extra_metadata = {}
            extra_data = {}
            if cursor_field:
                cursor_value = record_dict[cursor_field]
                extra_data = {cursor_field: cursor_value} #It should not become part of metadata
            if upsert_keys:
                extra_metadata["dat_record_id"] = '_'.join([str(record_dict[u_k]) for u_k in upsert_keys])
            else:
                extra_metadata["dat_record_id"] = "not_set"
            yield self.as_record_message(
                configured_stream=configured_stream,
                doc_chunk=record_str,
                data_entity=f"{self._schema}_{self._table_name}",
                extra_metadata=extra_metadata,
                extra_data=extra_data
            )

        cursor.close()
