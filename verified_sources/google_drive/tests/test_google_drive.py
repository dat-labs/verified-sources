from verified_sources.google_drive.source import GoogleDrive
from verified_sources.google_drive.specs import GoogleDriveSpecification, ConnectionSpecificationModel
from verified_sources.google_drive.catalog import GoogleDriveCatalog, PdfStream, TxtStream
from dat_core.pydantic_models import DatConnectionStatus
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

def test_specs_file():
    temp_specs = f'tests{os.path.sep}tmp_spec_model.py'
    yml_to_py('specs.yml', temp_specs)
            
    from verified_sources.google_drive.tests.tmp_spec_model import GoogleDriveSpecification as GDSpec_temp
    from verified_sources.google_drive.tests.tmp_spec_model import ConnectionSpecificationModel as ConnSpec_temp

    assert GDSpec_temp.model_fields == GoogleDriveSpecification.model_fields
    assert ConnSpec_temp.model_fields == ConnectionSpecificationModel.model_fields

def test_catalog_file():
    temp_catalog = f'tests{os.path.sep}tmp_catalog_model.py'
    yml_to_py('catalog.yml', temp_catalog)

    from verified_sources.google_drive.tests.tmp_catalog_model import Model as GDCatalog_temp
    from verified_sources.google_drive.tests.tmp_catalog_model import DocumentStream as PdfStream_temp
    from verified_sources.google_drive.tests.tmp_catalog_model import DocumentStream1 as TxtStream_temp

    assert GDCatalog_temp.model_fields == GoogleDriveCatalog.model_fields
    assert PdfStream_temp.model_fields == PdfStream.model_fields
    assert TxtStream_temp.model_fields == TxtStream.model_fields

def yml_to_py(yml_file: str, output_file: str):
    parent = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    with open(f'{parent}{os.path.sep}{yml_file}', 'r') as file:
        datamodel = file.read()
    with TemporaryDirectory() as temporary_directory_name:
        temporary_directory = Path(temporary_directory_name)
        output = Path(temporary_directory / 'model.py')
        generate(
            datamodel,
            input_file_type=InputFileType.JsonSchema,
            output=output,
            output_model_type=DataModelType.PydanticV2BaseModel,
        )
        model: str = output.read_text()
        with open(f'{parent}{os.path.sep}{output_file}', 'w') as f:
            f.write(model)