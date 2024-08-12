# generated by datamodel-codegen:
#   filename:  catalog.yml
#   timestamp: 2024-04-18T11:39:43+00:00

from __future__ import annotations

from typing import List, Optional, Union
from pydantic import Field
from dat_core.pydantic_models import DatDocumentStream, DatCatalog


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

    usr_file_path: str = Field(
        ...,
        description='The path of the file to be read.',
        title='File Path'
    )

    obj_file_path: Optional[str] = Field(None,
        json_schema_extra={'ui-opts': {'hidden': True}}
    )


class CsvDatStream(DatDocumentStream):
    name: Optional[str] = Field(
        'csv',
        description='The name of the document stream.',
        json_schema_extra={
            'ui-opts': {
                'hidden': True,
            }
        }
    )

    usr_file_path: str = Field(
        ...,
        description='The path of the file to be read.',
        title='File Path'
    )

    obj_file_path: Optional[str] = Field(None,
        json_schema_extra={'ui-opts': {'hidden': True}}
    )

class LocalFileSystemCatalog(DatCatalog):
    document_streams: List[Union[TxtDatStream]]
