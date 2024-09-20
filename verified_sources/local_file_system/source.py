import os
from minio import Minio
from typing import (Any, Optional, Tuple, Mapping, List)
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from verified_sources.local_file_system.specs import LocalFileSystemSpecification
from verified_sources.local_file_system.catalog import LocalFileSystemCatalog
from verified_sources.local_file_system.streams import LocalFileSystemTxtStream, LocalFileSystemCsvStream


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
        return True, "Connection successful"

    def streams(self, config: Mapping[str, Any], json_schemas: Mapping[str, Mapping[str, Any]]=None) -> List[Stream]:
        return [
            LocalFileSystemTxtStream(config),
            LocalFileSystemCsvStream(config),
        ]
