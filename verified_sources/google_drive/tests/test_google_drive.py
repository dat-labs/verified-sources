from verified_sources.google_drive.source import GoogleDrive
from verified_sources.google_drive.specs import GoogleDriveSpecification, ConnectionSpecificationModel
from verified_sources.google_drive.catalog import GoogleDriveCatalog, PdfStream, TxtStream
from dat_core.pydantic_models import DatConnectionStatus
from datamodel_code_generator import InputFileType, generate, DataModelType
from pydantic import BaseModel
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
    
    assert compare_models(GDSpec_temp, GoogleDriveSpecification) is True
    # assert compare_models(ConnSpec_temp, ConnectionSpecificationModel) is True


def yml_to_py(yml_file: str, output_file: str) -> None:
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
    
def compare_models(model1: BaseModel, model2: BaseModel) -> bool:
    if model1.model_fields.keys() != model2.model_fields.keys():
        return False
    
    model1_fields = [str(item) for item in model1.model_fields.values()]
    model2_fields = [str(item) for item in model2.model_fields.values()]
    if model1_fields != model2_fields:
        return False
    return True