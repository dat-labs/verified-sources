# generated by datamodel-codegen:
#   filename:  specs.yml
#   timestamp: 2024-05-27T08:56:26+00:00

from __future__ import annotations

from typing import Optional, Literal

from pydantic import AnyUrl, BaseModel, Field


class ConnectionSpecification(BaseModel):
    class Config:
        extra = 'allow'

    aws_access_key: str = Field(
        ..., description='AWS access key for the project', title='AWS access key'
    )
    aws_secret_key: str = Field(
        ..., description='AWS secret key for the project', title='AWS secret key'
    )
    bucket_name: str = Field(
        ..., description='Bucket name for the project', title='Bucket name'
    )
    region_name: str = Field(
        ..., description='Region name for the project', title='Region name'
    )


class AmazonS3Specification(BaseModel):
    class Config:
        extra = 'allow'

    documentation_url: Optional[AnyUrl] = None
    name: Literal['AmazonS3']
    module_name: Literal['amazon_s3']
    connection_specification: ConnectionSpecification = Field(
        ...,
        description='ConnectorDefinition specific blob. Must be a valid JSON string.',
    )
