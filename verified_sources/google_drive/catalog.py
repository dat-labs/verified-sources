# generated by datamodel-codegen:
#   filename:  catalog.yml
#   timestamp: 2024-04-09T10:07:59+00:00

from __future__ import annotations

from typing import List, Optional, Union, Literal

from pydantic import Field
from dat_core.pydantic_models import DatDocumentStream, DatCatalog, Advanced


class PdfStream(DatDocumentStream):
    name: Optional[str] = Field(
        'pdf',
        description='The name of the document stream.',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )
    dir_uris: List[str] = Field(
        ...,
        description="Google Drive directory URIs",
        title="Directory URIs"
    )

class TxtStream(DatDocumentStream):
    name: Optional[str] = Field(
        'txt',
        description='The name of the document stream.',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )
    dir_uris: List[str] = Field(
        ...,
        description="Google Drive directory URIs",
        title="Directory URIs"
    )


class GoogleDriveCatalog(DatCatalog):
    document_streams: Optional[List[Union[PdfStream, TxtStream]]] = None
