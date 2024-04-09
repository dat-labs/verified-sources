# generated by datamodel-codegen:
#   filename:  catalog.yml
#   timestamp: 2024-04-09T10:07:59+00:00

from __future__ import annotations

from typing import List, Optional, Union

from pydantic import Field
from dat_core.pydantic_models import DatDocumentStream, DatCatalog


class PdfStream(DatDocumentStream):
    name: Optional[str] = 'pdf'
    namespace: Optional[str] = Field(
        None, description='namespace the data is associated with'
    )
    dir_uris: List[str]


class TxtStream(DatDocumentStream):
    name: Optional[str] = 'txt'
    namespace: Optional[str] = Field(
        None, description='namespace the data is associated with'
    )
    dir_uris: List[str]


class GoogleDriveCatalog(DatCatalog):
    streams: Optional[List[Union[PdfStream, TxtStream]]] = None
