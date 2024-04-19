import os
from typing import (Any, Optional, Tuple, Mapping, List)
from dat_core.pydantic_models import DatMessage, DatLogMessage, Type, Level
from dat_core.connectors.sources.base import SourceBase
from dat_core.connectors.sources.stream import Stream
from dat_core.doc_splitters.factory import doc_splitter_factory, DocLoaderType, TextSplitterType
from verified_sources.amazon_s3.streams import S3TxtStream
from verified_sources.amazon_s3.specs import AmazonS3Specification

class AmazonS3(SourceBase):

    _spec_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'specs.yml')
    _catalog_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'catalog.yml')

    def check_connection(self, config: AmazonS3Specification) -> Tuple[bool, Optional[Any]]:
        connected, message = False, 'Connection failed'

        _doc_loader_and_splitter = doc_splitter_factory.create(
            filepath='',
            loader_key=DocLoaderType.S3_DIR_LOADER,
            splitter_key=TextSplitterType.SPLIT_BY_CHARACTER,
            loader_config=dict(
                aws_access_id=config.connection_specification.aws_access_key,
                aws_access_secret=config.connection_specification.aws_secret_key,
                bucket=config.connection_specification.bucket_name,
                prefix=config.connection_specification.prefix # TODO Pass a testing URL in connection specification itself
            )
        )
        try:
            next(_doc_loader_and_splitter.load_and_chunk())
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
        ]