from verified_sources.google_drive.source import GoogleDrive
from verified_sources.google_drive.specs import GoogleDriveSpecification
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

def test_model_files():
    parent = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    with open(f'{parent}{os.path.sep}specs.yml', 'r') as file:
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
        with open(f'{parent}{os.path.sep}tests/tmp_spec_model.py', 'w') as f:
            f.write(model)
            
    from verified_sources.google_drive.tests.tmp_spec_model import GoogleDriveSpecification as GDSpec_temp
    assert compare_class_structures(GDSpec_temp, GoogleDriveSpecification)
    os.remove(f'{parent}{os.path.sep}tests/tmp_spec_model.py')


def compare_class_structures(class1, class2):
    # Get the list of attributes and methods for both classes
    class1_attrs = set(dir(class1))
    class2_attrs = set(dir(class2))
    
    # Filter out attributes that are automatically added by Python
    filtered_attrs_class1 = sorted({attr for attr in class1_attrs if not attr.startswith('__')})
    filtered_attrs_class2 = sorted({attr for attr in class2_attrs if not attr.startswith('__')})
    # Compare the sets of attributes and methods
    if filtered_attrs_class1 == filtered_attrs_class2:
        return True
    else:
        return False