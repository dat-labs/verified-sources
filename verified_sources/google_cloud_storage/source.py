
import os
from typing import List
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from verified_sources.google_cloud_storage.specs import GoogleCloudStorageSpecification
from verified_sources.google_cloud_storage.catalog import GoogleCloudStorageCatalog
from verified_sources.google_cloud_storage.streams import GoogleCloudStorageStream
from google.oauth2.credentials import Credentials
from google.cloud import storage
from dat_core.pydantic_models import (
    ConnectorSpecification, DatConnectionStatus, DatMessage, DatLogMessage, Level, Type
)
from dat_core.loggers import logger

class GoogleCloudStorage(SourceBase):
    _spec_class = GoogleCloudStorageSpecification
    _catalog_class = GoogleCloudStorageCatalog

    def check_connection(self, config: GoogleCloudStorageSpecification) -> DatConnectionStatus:
        # Implement your connection check logic here
        try:
            conn_status = True
            message = "Connection SuccessFul"
            creds = Credentials(
                token=config.connection_specification.token,
                client_id=config.connection_specification.client_id,
                client_secret=config.connection_specification.client_secret,
                refresh_token=config.connection_specification.refresh_token,
                token_uri='https://oauth2.googleapis.com/token'
            )
            storage_client = storage.Client(
                credentials=creds,
                project=config.connection_specification.project_id
            )
            # import pdb
            # pdb.set_trace()
            self.bucket_client = storage_client.get_bucket(
                config.connection_specification.bucket_name)
            bucket_list = storage_client.list_buckets()
            logger.debug(bucket_list)
        except Exception as exc:
            conn_status = False
            message = repr(exc)
            logger.error(message)

        return conn_status, message

    def streams(self, config: ConnectorSpecification) -> List[Stream]:
        return [
            GoogleCloudStorageStream(config),
        ]
