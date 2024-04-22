import os
import json
from typing import (Any, Optional, Tuple, Mapping, List)
import boto3
from dat_core.pydantic_models import DatMessage, DatLogMessage, Type, Level
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from dat_core.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.amazon_s3.streams import S3TxtStream, S3PdfStream
from verified_sources.amazon_s3.specs import AmazonS3Specification


class AmazonS3(SourceBase):

    _spec_file = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'specs.yml')
    _catalog_file = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'catalog.yml')

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


if __name__ == '__main__':
    from verified_sources.amazon_s3.specs import AmazonS3Specification, ConnectionSpecification
    from verified_sources.amazon_s3.catalog import AmazonS3Catalog, TxtDatStream, PdfDatStream
    from dat_core.pydantic_models import ReadSyncMode, WriteSyncMode
    from dat_core.connectors.state_managers import LocalStateManager

    state_manager = LocalStateManager()
    _specs = AmazonS3Specification(
        name='AmazonS3',
        module_name='amazon_s3',
        connection_specification=ConnectionSpecification(
            aws_access_key = os.environ.get('AWS_ACCESS_KEY'),
            aws_secret_key = os.environ.get('AWS_SECRET_KEY'),
            bucket_name = os.environ.get('S3_BUCKET_NAME'),
            region_name = os.environ.get('S3_REGION'),
        )
    )
    _pdf_stream = PdfDatStream(
        namespace='my-s3-pdf-files',
        dir_prefix='tmp/',
        read_sync_mode=ReadSyncMode.INCREMENTAL,
        write_sync_mode=WriteSyncMode.REPLACE
    )
    _txt_stream = TxtDatStream(
        namespace='my-s3-txt-files',
        dir_prefix='tmp/',
        read_sync_mode=ReadSyncMode.INCREMENTAL,
        write_sync_mode=WriteSyncMode.REPLACE
        )
    _catalog = AmazonS3Catalog(
        document_streams=[
            _pdf_stream, _txt_stream
        ]
    )
    combined_state = {
        _pdf_stream.namespace: state_manager.get_stream_state(_pdf_stream),
        _txt_stream.namespace: state_manager.get_stream_state(_txt_stream),
    }
    for msg in AmazonS3().read(_specs, _catalog, state=combined_state):
        if msg.type == Type.STATE:
            state_manager.save_stream_state(msg.state.stream, msg.state.stream_state)
        print(msg.model_dump_json())