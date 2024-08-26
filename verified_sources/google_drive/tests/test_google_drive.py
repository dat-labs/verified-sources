from verified_sources.google_drive.source import GoogleDrive
from verified_sources.google_drive.catalog import GoogleDriveCatalog
from verified_sources.google_drive.specs import GoogleDriveSpecification
from dat_core.pydantic_models import DatConnectionStatus, DatMessage, Type
from datamodel_code_generator import InputFileType, generate, DataModelType
from pathlib import Path
from tempfile import TemporaryDirectory
import os

def test_check(valid_connection_object):
    check_connection_tpl = GoogleDrive().check(
        config=GoogleDriveSpecification(
            name='GoogleDrive',
            connection_specification=valid_connection_object,
            module_name='google_drive'
        )
    )
    assert isinstance(check_connection_tpl, DatConnectionStatus)
    assert check_connection_tpl.status.name == 'SUCCEEDED'


def test_discover(valid_connection_object):
    _d = GoogleDrive().discover(
        config=GoogleDriveSpecification(
            name='GoogleDrive',
            connection_specification=valid_connection_object,
            module_name='google_drive'
        )
    )
    assert isinstance(_d, dict)

