import os
from minio import Minio
from typing import (Any, Optional, Tuple, Mapping, List)
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import DatMessage, DatLogMessage, Type, Level
from verified_sources.local_file_system.specs import LocalFileSystemSpecification
from verified_sources.local_file_system.catalog import LocalFileSystemCatalog
from verified_sources.local_file_system.streams import LocalFileSystemTxtStream


class LocalFileSystem(SourceBase):
    _spec_class = LocalFileSystemSpecification
    _catalog_class = LocalFileSystemCatalog

    def check_connection(self, config: LocalFileSystemSpecification) -> Tuple[bool, Optional[Any]]:
        """
        Check if the connection to the source is successful.

        Args:
            config (LocalFileSystemSpecification): The configuration object for the source.

        Returns:
            Tuple[bool, Optional[Any]]: A tuple containing a boolean indicating if the connection was successful and an optional message.
        """

        connected, message = False, 'Connection failed'

        file_name = config.connection_specification.file_name
        bucket_name = config.connection_specification.bucket_name
        endpoint = os.getenv('MINIO_ENDPOINT')      
        access_key = os.getenv('MINIO_USER')
        secret = os.getenv('MINIO_PASS')
        try:
            client = Minio(endpoint, access_key=access_key, secret_key=secret, secure=False)
            client.stat_object(bucket_name, file_name)
            _msg = DatMessage(
                type=Type.LOG,
                log=DatLogMessage(
                    level=Level.INFO,
                    message=f"Successfully connected to {bucket_name}/{file_name}"
                )
            )
            print(_msg.model_dump_json(), flush=True)
            connected, message = True, 'Connection established'

        except Exception as e:
            _msg = DatMessage(
                type=Type.LOG,
                log=DatLogMessage(
                    level=Level.ERROR,
                    message=repr(e)
                )
            )
            print(_msg.model_dump_json(), flush=True)

        return connected, message

    def streams(self, config: Mapping[str, Any], json_schemas: Mapping[str, Mapping[str, Any]]=None) -> List[Stream]:
        return [
            LocalFileSystemTxtStream(config),
        ]
