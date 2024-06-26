# generated by datamodel-codegen:
#   filename:  specs.yml
#   timestamp: 2024-05-09T13:06:11+00:00

from __future__ import annotations

from typing import Any, Optional, Literal

from pydantic import BaseModel, Field
from dat_core.pydantic_models import ConnectionSpecification



class ConnectionSpecificationModel(ConnectionSpecification):
    client_id: str = Field(
        ..., description='client id for the project', title='Client ID'
    )
    client_secret: str = Field(
        ..., description='client secret for the project', title='Client Secret'
    )
    refresh_token: str = Field(
        ..., description='refresh token for the project', title='Refresh Token'
    )


class GoogleDriveSpecification(BaseModel):
    class Config:
        extra = 'allow'

    name: Literal['GoogleDrive']
    module_name: Literal['google_drive']
    protocol_version: Optional[Any] = None
    documentation_url: Optional[str] = (
        'https://datlabs.gitbook.io/datlabs/integrations/sources/google-drive'
    )
    changelog_url: Optional[str] = 'www.example.com'
    connection_specification: ConnectionSpecificationModel = Field(
        ...,
        description='ConnectorDefinition specific blob. Must be a valid JSON string.',
    )
