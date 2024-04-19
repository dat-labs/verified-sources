from typing import Any, Generator
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, DatMessage, StreamState
from dat_core.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.amazon_s3.specs import AmazonS3Specification


class S3TxtStream(Stream):
    """
    S3TxtStream class for crawling and processing URLs.

    Attributes:
        _name (str): The name of the S3TxtStream stream ('url_crawler').

    Methods:
        __init__: Initializes a new S3TxtStream object.
        read_records: Reads records from the configured stream and yields DatMessage objects.
    """

    _name = 'txt'

    def __init__(self, config: AmazonS3Specification) -> None:
        """
        Initializes a new S3TxtStream object.

        Parameters:
            config (AmazonS3Specification): The configuration object for URL crawling.
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
        _doc_loader_and_splitter = doc_splitter_factory.create(
            filepath='',
            loader_key=DocLoaderType.S3_DIR_LOADER,
            splitter_key=TextSplitterType.SPLIT_BY_CHARACTER,
            loader_config=dict(
                aws_access_id=self._config.connection_specification.aws_access_key,
                aws_access_secret=self._config.connection_specification.aws_secret_key,
                bucket=self._config.connection_specification.bucket_name,
                prefix=configured_stream.dir_prefix
            )
        )
        for doc in _doc_loader_and_splitter.load():
            for _doc_chunk in _doc_loader_and_splitter.split_text(doc.page_content):
                yield self.as_record_message(
                    configured_stream=configured_stream,
                    doc_chunk=_doc_chunk,
                    data_entity=doc.filepath
                )
