import os
import tempfile
from typing import Any, Generator
from minio import Minio
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, DatMessage, StreamState
from dat_core.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.local_file_system.specs import LocalFileSystemSpecification

class LocalFileSystemStream(Stream):
    """
    local_file_systemStream class for crawling and processing URLs.

    Attributes:
        _name (str): The name of the local_file_systemStream stream ('url_crawler').

    Methods:
        __init__: Initializes a new local_file_systemStream object.
        read_records: Reads records from the configured stream and yields DatMessage objects.
    """

    _name = 'local_file_system_stream'

    def __init__(self, config: LocalFileSystemSpecification) -> None:
        """
        Initializes a new local_file_systemStream object.

        Parameters:
            config (LocalFileSystemSpecification): The configuration object for URL crawling.
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
        file_path = self._config.connection_specification.obj_file_path
        bucket_name = os.getenv('MINIO_BUCKET')

        client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT'),
            access_key=os.getenv('MINIO_USER'),
            secret_key=os.getenv('MINIO_PASS'),
            secure=False
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file = os.path.join(tmp_dir, file_path)
            client.fget_object(bucket_name, file_path, tmp_file)
            _doc_loader_and_splitter = doc_splitter_factory.create(
                loader_key=self._doc_loader,
                splitter_key=configured_stream.advanced.splitter_settings.splitter_settings,
                loader_config=dict(
                    file_path=tmp_file
                )
            )

            for _doc_chunk in _doc_loader_and_splitter.load_and_chunk():
                yield self.as_record_message(
                    configured_stream=configured_stream,
                    doc_chunk=_doc_chunk,
                    data_entity=file_path,
                    dat_last_modified=int(os.path.getmtime(tmp_file))
                )

class LocalFileSystemTxtStream(LocalFileSystemStream):
    """
    A strean for reading text files from the local file system.
    """
    _name = 'txt'
    _doc_loader = DocLoaderType.TEXT
