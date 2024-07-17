from typing import Any, Generator
import psycopg
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import (
    DatCatalog, DatDocumentStream,
    DatMessage, StreamState,
    ReadSyncMode
)
from dat_core.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
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
        import pdb;pdb.set_trace()
        cursor = self.connection.cursor()
        cursor_field = getattr(configured_stream, 'cursor_field', None)
        # import pdb;pdb.set_trace()
        if cursor_field and cursor_value is not None:
            query = f"SELECT * FROM {configured_stream.name} WHERE {cursor_field} > %s ORDER BY {cursor_field} ASC"
            cursor.execute(query, (cursor_value,))
        else:
            query = f"SELECT * FROM {configured_stream.name}"
            cursor.execute(query)

        records = cursor.fetchall()

        for record in records:
            record_dict = dict(
                zip([column.name for column in cursor.description], record))
            record_str = ", ".join([f"{k}: {v}" for k, v in record_dict.items()])

            # Update cursor_value to the current record's cursor field value if cursor_field is present
            if cursor_field:
                cursor_value = record_dict[cursor_field]

            yield self.as_record_message(
                configured_stream=configured_stream,
                doc_chunk=record_str,
                data_entity=f"{configured_stream.name}",
            )

        cursor.close()
