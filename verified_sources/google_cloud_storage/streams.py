import os
import tempfile
from typing import Any, Generator
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, DatMessage
from verified_sources.common.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.google_cloud_storage.specs import GoogleCloudStorageSpecification
from google.cloud import storage
from google.oauth2.credentials import Credentials


class GoogleCloudStorageStream(Stream):
    """
    google_cloud_storageStream0 class for crawling and processing URLs.

    Attributes:
        _name (str): The name of the google_cloud_storageStream0 stream ('url_crawler').

    Methods:
        __init__: Initializes a new google_cloud_storageStream0 object.
        read_records: Reads records from the configured stream and yields DatMessage objects.
    """

    _name = 'document_stream'

    def __init__(self, config: GoogleCloudStorageSpecification) -> None:
        """
        Initializes a new google_cloud_storageStream0 object.

        Parameters:
            config (GoogleCloudStorageSpecification): The configuration object for URL crawling.
        """
        self._config = config
        creds = Credentials(
            token=config.connection_specification.token,
            client_id=config.connection_specification.client_id,
            client_secret=config.connection_specification.client_secret,
            refresh_token=config.connection_specification.refresh_token,
            token_uri='https://oauth2.googleapis.com/token'
        )
        self.storage_client = storage.Client(
            credentials=creds,
            project=config.connection_specification.project_id
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
        map_doc_loader = {
            'pdf': DocLoaderType.PYPDF,
            'txt': DocLoaderType.TEXT,
            'csv': DocLoaderType.TEXT,
            'log': DocLoaderType.TEXT,
            'html': DocLoaderType.HTML,
            'md': DocLoaderType.MARKDOWN,
        }
        bucket_client = self.storage_client.get_bucket(
            self._config.connection_specification.bucket_name)
        dir_prefix = configured_stream.folder_path
        path_list = dir_prefix.split('/')
        path_list = [path for path in path_list if path]
        path = os.path.join(*path_list) + '/'
        file_name = f'{configured_stream.file_name}.{configured_stream.file_type.value}'
        blob = bucket_client.blob(path+file_name)
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = f"{temp_dir}/{blob.name}"
            print(blob.name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            blob.download_to_filename(file_path)

            _doc_loader_and_splitter = doc_splitter_factory.create(
                loader_key=map_doc_loader.get(
                    configured_stream.file_type.value),
                splitter_key=TextSplitterType.SPLIT_BY_CHARACTER_RECURSIVELY.value,
                loader_config=dict(
                    file_path=file_path,
                )
                # splitter_config=configured_stream.advanced.splitter_settings.config
            )
            for _doc_chunk in _doc_loader_and_splitter.load_and_chunk():
                yield self.as_record_message(
                    configured_stream=configured_stream,
                    doc_chunk=_doc_chunk,
                    data_entity=blob.name,
                    dat_last_modified=blob.updated
                )
