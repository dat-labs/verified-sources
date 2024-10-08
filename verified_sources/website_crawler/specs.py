# generated by datamodel-codegen:
#   filename:  specs.yml
#   timestamp: 2024-05-09T12:59:37+00:00

from __future__ import annotations

from typing import Optional, Literal

from pydantic import AnyUrl, BaseModel, Field
from dat_core.pydantic_models import ConnectionSpecification


class ConnectionSpecificationModel(ConnectionSpecification):
    site_url: str


class WebsiteCrawlerSpecification(BaseModel):
    class Config:
        extra = 'allow'

    documentation_url: Optional[str] = (
        'https://datlabs.gitbook.io/datlabs/integrations/sources/website-crawler'
    )
    name: Literal['WebsiteCrawler']
    module_name: Literal['website_crawler']
    connection_specification: ConnectionSpecificationModel = Field(
        ...,
        description='ConnectorDefinition specific blob. Must be a valid JSON string.',
    )
