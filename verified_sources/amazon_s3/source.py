import os
import json
from typing import (Any, Optional, Tuple, Mapping, List, Dict)
import boto3
import jsonref
from dat_core.pydantic_models import DatMessage, DatLogMessage, Type, Level
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from verified_sources.amazon_s3.streams import S3TxtStream, S3PdfStream
from verified_sources.amazon_s3.specs import AmazonS3Specification
from verified_sources.amazon_s3.catalog import AmazonS3Catalog


class AmazonS3(SourceBase):

    _catalog_class = AmazonS3Catalog
    _spec_class = AmazonS3Specification

    def check_connection(self, config: AmazonS3Specification) -> Tuple[bool, Optional[Any]]:
        connected, message = False, 'Connection failed'
        try:
            s3_client = boto3.client(
                's3',
                region_name=config.connection_specification.region_name,
                aws_access_key_id=config.connection_specification.aws_access_key,
                aws_secret_access_key=config.connection_specification.aws_secret_key,
            )
            for obj in s3_client.list_objects_v2(
                    Bucket=config.connection_specification.bucket_name, MaxKeys=5)['Contents']:
                _msg = DatMessage(
                    type=Type.LOG,
                    log=DatLogMessage(
                        level=Level.INFO,
                        message=obj['Key']
                    )
                )
                print(_msg.model_dump_json(), flush=True)
                break
            connected, message = True, 'Connection establised'

        except Exception as exc:
            _msg = DatMessage(
                type=Type.LOG,
                log=DatLogMessage(
                    level=Level.ERROR,
                    message=repr(exc)
                )
            )
            print(_msg.model_dump_json(), flush=True)

        return connected, message

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            S3TxtStream(config),
            S3PdfStream(config)
        ]