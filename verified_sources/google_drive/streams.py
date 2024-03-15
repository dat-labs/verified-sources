import requests
import time
from typing import Any, Generator, List, Mapping, Optional, Dict
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models.connector_specification import ConnectorSpecification
from dat_core.pydantic_models.dat_message import DatMessage, Type, DatDocumentMessage, Data
from dat_core.pydantic_models.dat_log_message import DatLogMessage, Level
from dat_core.pydantic_models.configured_document_stream import ConfiguredDocumentStream
from dat_core.auth.oauth2_authenticator import BaseOauth2Authenticator
from dat_core.pydantic_models.dat_catalog import DatCatalog, DatDocumentStream

class GoogleDriveStream(Stream):
    
    __supported_mimetype__ = 'application/txt'
    __required_scopes__ = [
                    'https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/drive.file', 
                    'https://www.googleapis.com/auth/drive.appdata',
                    ]
    
    def __init__(self, config: ConnectorSpecification) -> None:
        self._config = config
        self.auth = BaseOauth2Authenticator(
                client_id=config.connectionSpecification.get('client_id'),
                client_secret=config.connectionSpecification.get('client_secret'),
                token_refresh_endpoint='https://oauth2.googleapis.com/token',
                scopes=self.__required_scopes__
            )
        self.auth.refresh_token = config.connectionSpecification.get('refresh_token')
    
    def read_records(self,
        catalog: DatCatalog,
        configured_stream: DatDocumentStream,
        stream_state: Optional[Mapping[str, Any]] = None
    ) -> Generator[DatMessage, Any, Any]:
        folder_id = self._traverse_folder_path(configured_stream.dir_uris[0])
        params = {
            'fields': 'nextPageToken, files(id, name)',
            'q': f"mimeType='{self.__supported_mimetype__}' and '{folder_id}' in parents",
        }
        print('line:38', params)
        files = self._list_gdrive_objects(params)
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
                namespace=configured_stream.namespace
                )
            yield DatMessage(
                type=Type.RECORD,
                record=doc_msg
            )
    
    def _list_gdrive_objects(self, params) -> List[Dict]:
        headers = {
            'Authorization': f'Bearer {self.auth.get_access_token()}'
        }
        resp = requests.get('https://www.googleapis.com/drive/v3/files', headers=headers, params=params)
        if resp.status_code == 200:
            return resp.json().get('files', [])
        else:
            print(resp.text)
    
    def _traverse_folder_path(self, folder_path) -> int:
        folder_id = 'root'
        if folder_path == '/':
            return
        
        path_list = folder_path.split('/')
        path_list = [path for path in path_list if path]
        for ele in path_list:
            params = {
                'fields': 'nextPageToken, files(id, name)',
                'q': f"mimeType='application/vnd.google-apps.folder' and name='{ele}' and '{folder_id}' in parents and trashed=false",
                'spaces': 'drive'
            }
            print('line:82', params)
            folders = self._list_gdrive_objects(params)
            if folders:
                folder_id = folders[0]['id']
                print('Found folder:', folders[0]['name'])
        
        return folder_id



class GDrivePdfStream(GoogleDriveStream):
    __supported_mimetype__ = 'application/pdf'

class GDriveTxtStream(GoogleDriveStream):
    __supported_mimetype__ = 'application/txt'



