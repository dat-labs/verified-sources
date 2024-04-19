# generated by datamodel-codegen:
#   filename:  specs.yml
#   timestamp: 2024-04-19T03:16:33+00:00

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import AnyUrl, BaseModel, Field


class ConnectionSpecification(BaseModel):
    class Config:
        extra = 'allow'

    aws_access_key: str
    aws_secret_key: str
    bucket_name: str
    region_name: str


class AmazonS3Specification(BaseModel):
    class Config:
        extra = 'allow'

    documentation_url: Optional[AnyUrl] = None
    name: str = Field(
        ...,
        description='The name of the specific connector to which this ConnectorSpecification belongs.',
    )
    module_name: str = Field(
        ..., description='Name of the python module for this connector'
    )
    connection_specification: ConnectionSpecification = Field(
        ...,
        description='ConnectorDefinition specific blob. Must be a valid JSON string.',
    )
