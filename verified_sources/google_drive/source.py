import os
from typing import List, Tuple, Any
from dat_core.connectors.sources.stream import Stream
from googleapiclient.discovery import build
from google.oauth2 import credentials
from dat_core.connectors.sources.base import SourceBase
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models.dat_connection_status import DatConnectionStatus, Status
from dat_core.auth.oauth2_authenticator import BaseOauth2Authenticator

class GoogleDrive(SourceBase):
    """
    GoogleDrive as a source
    """
    _spec_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'specs.yml')
    _catalog_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'catalog.yml')

    def check_connection(self, config: ConnectorSpecification) -> DatConnectionStatus:
        """Checks the connection to Google Drive using the provided configuration.

        Args:
            config (ConnectorSpecification): The configuration specifying the connection details.

        Returns:
            DatConnectionStatus: The connection status.
        """
        try:
            auth = BaseOauth2Authenticator(
                client_id=config.connectionSpecification.get('client_id'),
                client_secret=config.connectionSpecification.get('client_secret'),
                token_refresh_endpoint='https://oauth2.googleapis.com/token',
                scopes=[
                    'https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/drive.file', 
                    'https://www.googleapis.com/auth/drive.appdata',
                    ]
            )
            auth.refresh_token = config.connectionSpecification.get('refresh_token')
            auth.token_refresh()
            creds_info = {
                "refresh_token": auth.refresh_token,
                "client_id": auth._client_id,
                "client_secret": auth._client_secret
                }
            g_creds = credentials.Credentials.from_authorized_user_info(creds_info)
            service = build("drive", "v3", credentials=g_creds)
            results = (
                service.files()
                .list(pageSize=10, fields="nextPageToken, files(id, name)")
                .execute()
            )
            items = results.get("files", [])
            conn_status = True
            message = 'List files successful'
        except Exception as exc:
            # TODO: Raise or log proper exception
            conn_status = False
            message = str(exc)

        return conn_status, message
    
    def streams(self, config: os.Mapping[str, Any], json_schemas: os.Mapping[str, os.Mapping[str, Any]] = None) -> List[Stream]:
        return None


if __name__ == '__main__':
    import os
    gdrive = GoogleDrive()
    conn_details = {
        'client_id': os.environ.get('GOOGLE_DRIVE_CLIENT_ID'),
        'client_secret': os.environ.get('GOOGLE_DRIVE_CLIENT_SECRET'),
        'refresh_token': os.environ.get('GOOGLE_DRIVE_REFRESH_TOKEN'),
    }
    print(gdrive.check(config=ConnectorSpecification(name='GoogleDrive', connectionSpecification=conn_details)))
