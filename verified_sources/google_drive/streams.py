import requests
import datetime
import os
from contextlib import contextmanager
from typing import Any, Generator, List, Mapping, Optional, Dict
from dat_core.connectors.sources.stream import Stream
from dat_core.pydantic_models import (
    ConnectorSpecification,
    DatMessage,
    DatCatalog,
    DatDocumentStream,
)
from dat_core.auth.oauth2_authenticator import BaseOauth2Authenticator
from dat_core.doc_splitters import PdfSplitter, BaseSplitter, TxtSplitter

class GoogleDriveStream(Stream):
    """
    A stream for reading data from Google Drive.

    Args:
        config (ConnectorSpecification): The connector configuration.

    Attributes:
        _config (ConnectorSpecification): The connector configuration.
        auth (BaseOauth2Authenticator): The OAuth2 authenticator instance.
    """
    TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
    __supported_mimetypes__ = ('text/plain', )
    __required_scopes__ = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive.appdata',
    ]
    _doc_splitter = BaseSplitter
    _default_cursor = 'dat_last_modified'

    def __init__(self, config: ConnectorSpecification) -> None:
        """
        Initialize the GoogleDriveStream instance.

        Args:
            config (ConnectorSpecification): The connector configuration.
        """
        self._config = config
        self.auth = BaseOauth2Authenticator(
            client_id=config.connection_specification.get('client_id'),
            client_secret=config.connection_specification.get('client_secret'),
            token_refresh_endpoint='https://oauth2.googleapis.com/token',
            scopes=self.__required_scopes__
        )
        self.auth.refresh_token = config.connection_specification.get('refresh_token')

    def read_records(
        self,
        catalog: DatCatalog,
        configured_stream: DatDocumentStream,
        cursor_value: Any = None
    ) -> Generator[DatMessage, Any, Any]:
        """
        Read records from Google Drive.

        Args:
            catalog (DatCatalog): The data catalog.
            configured_stream (DatDocumentStream): The configured document stream.
            stream_state (Optional[Mapping[str, Any]]): The stream state.

        Yields:
            Generator[DatMessage, Any, Any]: A generator of DatMessage instances.
        """
        folder_id = self._traverse_folder_path(configured_stream.dir_uris[0])
        mimetype_q = ' or '.join([f"mimeType='{_mt}'" for _mt in self.__supported_mimetypes__])
        params = {
            'fields': 'nextPageToken, files(id, name, createdTime, modifiedTime)',
            'q': f"{mimetype_q} and '{folder_id}' in parents",
        }
        files = self.list_gdrive_objects(params)
        if cursor_value:
            files = self._slice_based_on_attr(files, _attr='modifiedTime', _value=cursor_value)
        for file in files:
            with self.download_gdrive_file(file_id=file['id']) as temp_file:
                data_entity=f'{configured_stream.dir_uris[0]}/{file["name"]}'
                extra_metadata = {'updated_at': file['modifiedTime'], 'created_at': file['createdTime']}
                for doc_chunk in self._doc_splitter(
                    filepath=temp_file, strategy='page').yield_chunks():
                    yield self.as_record_message(
                        configured_stream=configured_stream,
                        doc_chunk=doc_chunk,
                        data_entity=data_entity,
                        dat_last_modified=file['modifiedTime'],
                        extra_metadata=extra_metadata
                        )
    
    def _slice_based_on_attr(self, _list: List, _attr: str, _value: Any) -> List:
        """
        Given a general list of dicts, slice based on a specific key:value pair
        The list must be in a certain order
        """
        _index = 0
        _slice_from = 0
        for item in _list:
            if item.get(_attr) == _value:
                _slice_from = _index
                break
            _index += 1
        
        return _list[_slice_from:]

    def list_gdrive_objects(self, params) -> List[Dict]:
        """
        List objects in Google Drive.

        Args:
            params: The request parameters.

        Returns:
            List[Dict]: A list of objects in Google Drive.
        """
        headers = self.auth.get_auth_header()
        resp = requests.get('https://www.googleapis.com/drive/v3/files', headers=headers, params=params)
        if resp.status_code == 200:
            files = resp.json().get('files', [])
            for file in files:
                file['modifiedTime'] = int(datetime.datetime.strptime(
                    file['modifiedTime'], self.TIMESTAMP_FORMAT).timestamp())
                file['createdTime'] = int(datetime.datetime.strptime(
                    file['createdTime'], self.TIMESTAMP_FORMAT).timestamp())

            files = sorted(files, key=lambda file: file['modifiedTime'])
            return files
        else:
            print(resp.text)

    def _traverse_folder_path(self, folder_path) -> int:
        """
        Traverse the folder path in Google Drive.

        Args:
            folder_path: The folder path.

        Returns:
            int: The folder ID.
        """
        folder_id = 'root'
        if folder_path == '/':
            return

        path_list = folder_path.split('/')
        path_list = [path for path in path_list if path]
        for ele in path_list:
            params = {
                'fields': 'nextPageToken, files(id, name, createdTime, modifiedTime)',
                'q': f"mimeType='application/vnd.google-apps.folder' and name='{ele}' and '{folder_id}' in parents and trashed=false",
                'spaces': 'drive'
            }
            folders = self.list_gdrive_objects(params)
            if folders:
                folder_id = folders[0]['id']
                print('Found folder:', folders[0]['name'])

        return folder_id

    @contextmanager
    def download_gdrive_file(self, file_id) -> Generator[str, Any, Any]:
        """
        Download a file from Google Drive.

        Args:
            file_id: The file ID.

        Yields:
            Generator[str, Any, Any]: A generator yielding the downloaded file path.
        """
        from tempfile import NamedTemporaryFile
        headers = self.auth.get_auth_header()
        params = {
            'alt': 'media'
        }
        temp_file = None
        try:
            resp = requests.get(f'https://www.googleapis.com/drive/v3/files/{file_id}', headers=headers, params=params)
            if resp.status_code == 200:
                temp_file = NamedTemporaryFile(mode='wb+', delete=False)
                temp_file.write(resp.content)
                yield temp_file.name
            else:
                print(resp.text)
        finally:
            if temp_file:
                os.remove(temp_file.name)


class GDrivePdfStream(GoogleDriveStream):
    """
    A stream for reading PDF files from Google Drive.

    Attributes:
        Inherits all attributes from GoogleDriveStream.
    """
    _name = 'pdf'
    __supported_mimetypes__ = ('application/pdf',)
    _doc_splitter = PdfSplitter


class GDriveTxtStream(GoogleDriveStream):
    """
    A stream for reading text files from Google Drive.

    Attributes:
        Inherits all attributes from GoogleDriveStream.
    """
    _name = 'txt'
    __supported_mimetypes__ = ('application/txt', 'text/plain', 'text/x-log')
    _doc_splitter = TxtSplitter
