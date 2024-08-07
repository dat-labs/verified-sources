# generated by datamodel-codegen:
#   filename:  catalog.yml
#   timestamp: 2024-05-27T08:56:40+00:00

from __future__ import annotations

from typing import List, Union, Optional
from pydantic import Field

from dat_core.pydantic_models import DatDocumentStream, DatCatalog


class PdfDatStream(DatDocumentStream):
    name: Optional[str] = Field(
        'pdf',
        description='The name of the document stream.',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )
    dir_prefix: List[str]


class TxtDatStream(DatDocumentStream):
    name: Optional[str] = Field(
        'txt',
        description='The name of the document stream.',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )
    dir_prefix: List[str]

class AmazonS3Catalog(DatCatalog):
    document_streams: List[Union[TxtDatStream, PdfDatStream]]
