import datetime
import tempfile
import os
from typing import Any, Generator, Iterable
import boto3
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatCatalog, DatDocumentStream, DatMessage
from dat_core.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.amazon_s3.specs import AmazonS3Specification


class S3BaseStream(Stream):
    """
    S3TxtStream class for crawling and processing URLs.

    Attributes:
        _name (str): The name of the S3TxtStream stream ('url_crawler').

    Methods:
        __init__: Initializes a new S3TxtStream object.
        read_records: Reads records from the configured stream and yields DatMessage objects.
    """
    _default_cursor = 'dat_last_modified'

    def __init__(self, config: AmazonS3Specification) -> None:
        """
        Initializes a new S3TxtStream object.

        Parameters:
            config (AmazonS3Specification): The configuration object for URL crawling.
        """
        self._config = config
        self.s3_client = boto3.client(
            's3',
            region_name=config.connection_specification.region_name,
            aws_access_key_id=config.connection_specification.aws_access_key,
            aws_secret_access_key=config.connection_specification.aws_secret_key,
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
        objects = list()
        for directory in configured_stream.dir_prefix:
            objects += self.s3_client.list_objects_v2(
            Bucket=self._config.connection_specification.bucket_name,
            Prefix=directory
            )['Contents']
        objects = sorted(objects, key=lambda obj: obj['LastModified'].timestamp())

        with tempfile.TemporaryDirectory() as temp_dir:
            for obj in self._filter_objects_to_process(objects, cursor_value):
                file_path = f"{temp_dir}/{obj['Key']}"
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                self.s3_client.download_file(
                    self._config.connection_specification.bucket_name, obj['Key'], file_path)

                _doc_loader_and_splitter = doc_splitter_factory.create(
                        loader_key=self._doc_loader,
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
                        data_entity=obj['Key'],
                        dat_last_modified=obj['LastModified'].timestamp()
                    )
    
    def _filter_objects_to_process(self,
        objects: Iterable[dict], cursor_value: int) -> Generator[dict, Any, Any]:
        """
        Filters objects based on their last modification timestamp and supported file types.

        Args:
            self: The current object instance.
            objects (Iterable[dict]): Iterable of objects to filter.
            cursor_value (int): Timestamp to compare with each object's last modified timestamp.

        Yields:
            Generator[dict, Any, Any]: A generator yielding filtered objects.

        Returns:
            None
        """
        for obj in objects:
            if cursor_value and obj['LastModified'].timestamp() < cursor_value:
                continue
            for _doc_type in self._supported_file_types:
                if obj['Key'].endswith(_doc_type):
                    yield obj



class S3TxtStream(S3BaseStream):
    _name = 'txt'
    _doc_loader = DocLoaderType.TEXT
    _supported_file_types = ('.txt', '.log', '.csv')

class S3PdfStream(S3BaseStream):
    _name = 'pdf'
    _doc_loader = DocLoaderType.PYPDF
    _supported_file_types = ('.pdf',)

