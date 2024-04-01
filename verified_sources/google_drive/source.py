import os
import requests
from typing import List, Tuple, Any, Mapping
from dat_core.connectors.sources.stream import Stream
from dat_core.connectors.sources.base import SourceBase
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models.dat_connection_status import DatConnectionStatus, Status
from dat_core.auth.oauth2_authenticator import BaseOauth2Authenticator
from dat_core.pydantic_models.dat_catalog import DatCatalog
from verified_sources.google_drive.streams import GDrivePdfStream, GDriveTxtStream
class GoogleDrive(SourceBase):
    """
    GoogleDrive as a source
    """
    _spec_file = 'https://raw.githubusercontent.com/dc-rahul/verified-sources/feature/google-drive-and-wikipedia-forked/verified_sources/google_drive/specs.yml'
    _catalog_file = 'https://raw.githubusercontent.com/dc-rahul/verified-sources/feature/google-drive-and-wikipedia-forked/verified_sources/google_drive/catalog.yml'

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
            params = {
                'fields': 'nextPageToken, files(id, name)'
            }
            # print(auth.get_auth_header())
            resp = requests.get('https://www.googleapis.com/drive/v3/files', headers=auth.get_auth_header(), params=params)
            if resp.status_code == 200:
                print(resp.json())
                conn_status = True
                message = 'List files successful'
            else:
                print(resp.text)
                conn_status = True
                message = 'List files unsuccessful'

        except Exception as exc:
            # TODO: Raise or log proper exception
            conn_status = False
            message = str(exc)

        return conn_status, message
    
    def streams(self, config: ConnectorSpecification) -> List[Stream]:
        return [
            GDrivePdfStream(config),
            GDriveTxtStream(config)
        ]


        
if __name__ == '__main__':
    import os
    from dat_core.pydantic_models.dat_catalog import DatCatalog, DatDocumentStream
    from dat_core.pydantic_models.dat_document_stream import ReadSyncMode
    from dat_core.connectors.state_managers import LocalStateManager
    from dat_core.pydantic_models import Type
    state_manager = LocalStateManager()
    gdrive = GoogleDrive()
    conn_details = {
        'client_id': os.environ.get('GOOGLE_DRIVE_CLIENT_ID'),
        'client_secret': os.environ.get('GOOGLE_DRIVE_CLIENT_SECRET'),
        'refresh_token': os.environ.get('GOOGLE_DRIVE_REFRESH_TOKEN'),
    }
    config = ConnectorSpecification(name='GoogleDrive', connectionSpecification=conn_details, module_name='google_drive')
    # print(gdrive.check(config=config))
    pdf_stream = DatDocumentStream(
                name='pdf',
                namespace='my-gdrive-pdf-files',
                dir_uris=['bak/MySQL/STAGING/for-dat-gdrive-test', ],
                read_sync_mode=ReadSyncMode.incremental,
                # cursor_field='updated_at',
            )
    txt_stream = DatDocumentStream(
                name='txt',
                namespace='my-gdrive-txt-files',
                dir_uris=['bak/MySQL/STAGING/for-dat-gdrive-test', ],
                read_sync_mode=ReadSyncMode.incremental,
                # cursor_field='updated_at',
            )
    combined_state = {
        pdf_stream.namespace: state_manager.get_stream_state(pdf_stream),
        txt_stream.namespace: state_manager.get_stream_state(txt_stream),
    }
    configured_catalog = DatCatalog(
        document_streams=[
            txt_stream,
            pdf_stream,
        ]
    )
    for msg in gdrive.read(config=config, catalog=configured_catalog, state=combined_state):
        if msg.type == Type.STATE:
            state_manager.save_stream_state(msg.state.stream, msg.state.stream_state)
        print(msg.model_dump_json())