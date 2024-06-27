from typing import Any, Generator
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, DatMessage, StreamState
from dat_core.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.postgres.specs import PostgresSpecification

class PostgresStream0(Stream):
    """
    postgresStream0 class for crawling and processing URLs.

    Attributes:
        _name (str): The name of the postgresStream0 stream ('url_crawler').

    Methods:
        __init__: Initializes a new postgresStream0 object.
        read_records: Reads records from the configured stream and yields DatMessage objects.
    """

    _name = 'postgres_stream_0'

    def __init__(self, config: PostgresSpecification) -> None:
        """
        Initializes a new postgresStream0 object.

        Parameters:
            config (PostgresSpecification): The configuration object for URL crawling.
        """
        self._config = config
    
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