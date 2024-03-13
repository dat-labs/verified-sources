import requests
import time
from typing import Any, Generator, List, Mapping, Optional
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models.dat_message import DatMessage, Type, DatDocumentMessage, Data
from dat_core.pydantic_models.dat_log_message import DatLogMessage, Level
from dat_core.pydantic_models.configured_document_stream import ConfiguredDocumentStream
from dat_core.auth.oauth2_authenticator import BaseOauth2Authenticator
from dat_core.pydantic_models.dat_catalog import DatCatalog, DatDocumentStream

class GoogleDriveStream(Stream):
    
    def __init__(self, config: ConnectorSpecification) -> None:
        self._config = config
        self.auth = BaseOauth2Authenticator(
                client_id=config.connectionSpecification.get('client_id'),
                client_secret=config.connectionSpecification.get('client_secret'),
                token_refresh_endpoint='https://oauth2.googleapis.com/token',
                scopes=[
                    'https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/drive.file', 
                    'https://www.googleapis.com/auth/drive.appdata',
                    ]
            )
        self.auth.refresh_token = config.connectionSpecification.get('refresh_token')
    
    def read_records(self,
        catalog: DatCatalog,
        configured_stream: DatDocumentStream,
        stream_state: Optional[Mapping[str, Any]] = None
    ) -> Generator[DatMessage, Any, Any]:
        access_token = self.auth.get_access_token()
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        params = {
            'fields': 'nextPageToken, files(id, name)',
            'q': f"mimeType='application/vnd.google-apps.folder'",# and name='{configured_stream.dir_uris[0]}'",
        }
        resp = requests.get('https://www.googleapis.com/drive/v3/files', headers=headers, params=params)
        if resp.status_code == 200:
            files = resp.json().get('files', [])
            for file in files:
                doc_msg = DatDocumentMessage(
                    stream=self.as_pydantic_model(),
                    data=Data(
                        document_chunk=file['name'],
                        metadata=self.get_metadata(
                                specs=self._config,
                                document_chunk=file['name'],
                                data_entity=configured_stream.dir_uris[0]
                                )
                            ),
                    emitted_at=int(time.time()),
                    namespace='test_1'
                    )
                yield DatMessage(
                    type=Type.RECORD,
                    record=doc_msg
                )
        else:
            print(resp.text) 
    



class GDrivePdfStream(GoogleDriveStream):
    pass

class GDriveTxtStream(GoogleDriveStream):
    pass



