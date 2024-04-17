# generated by datamodel-codegen:
#   filename:  catalog.yml
#   timestamp: 2024-04-17T14:11:59+00:00

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, RootModel
from dat_core.pydantic_models import DatDocumentStream
from dat_core.pydantic_models.base import EnumWithStr


class ByHtmlHeaderConfig(BaseModel):
    headers_to_split_on: Optional[List[str]] = Field(
        ['h2', 'h3'],
        description='list of headers we want to track mapped to (arbitrary) keys for metadata. Allowed header values: h1, h2, h3, h4, h5, h6',
    )


class ByHtmlHeader(BaseModel):
    strategy: Optional[str] = 'SPLIT_BY_HTML_HEADER'
    config: Optional[ByHtmlHeaderConfig] = None


class ByCharacterConfig(BaseModel):
    separator: Optional[str] = '\\n\\n'


class ByCharacter(BaseModel):
    strategy: Optional[str] = 'SPLIT_BY_CHARACTER'
    config: Optional[ByCharacterConfig] = None


class Advanced(BaseModel):
    chunking_strategy: Optional[Union[ByCharacter, ByHtmlHeader]] = None


class Crawler(DatDocumentStream):
    name: Optional[str] = 'url_crawler'
    namespace: Optional[str] = Field(
        None, description='namespace the data is associated with'
    )
    advanced: Optional[Advanced] = Field(
        None, description='Additional optional settings'
    )


class WebCrawlerCatalog(BaseModel):
    document_streams: List[Crawler]
