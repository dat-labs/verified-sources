# generated by datamodel-codegen:
#   filename:  catalog.yml
#   timestamp: 2024-04-09T10:07:59+00:00

from __future__ import annotations

from typing import List, Optional, Union, Literal

from pydantic import Field
from dat_core.pydantic_models import DatDocumentStream, DatCatalog, Advanced


class PdfStream(DatDocumentStream):
    name: Literal['pdf']
    namespace: Optional[str] = Field(
        None, description='namespace the data is associated with'
    )
    dir_uris: List[str]
    advanced: Optional[Advanced] = Field(
        None, description='Additional optional settings'
    )


class TxtStream(DatDocumentStream):
    name: Literal['txt']
    namespace: Optional[str] = Field(
        None, description='namespace the data is associated with'
    )
    dir_uris: List[str]
    advanced: Optional[Advanced] = Field(
        None, description='Additional optional settings'
    )


class GoogleDriveCatalog(DatCatalog):
    document_streams: Optional[List[Union[PdfStream, TxtStream]]] = None
