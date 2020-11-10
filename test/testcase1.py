import pytest, sys
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType


from pathlib import Path # if you haven't already done so
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

from  utils.avroschematostruct import converttoStruct, inpschema

@pytest.fixture
def inputschema():
    inppath = r'..\data\simpleavro.avsc'
    return inpschema(inppath)


def test_converttoStruct(inputschema):
    outSchema = StructType().add("Name",StringType(),True).add("Age",IntegerType(),True)
    testschema = converttoStruct(inputschema)
    print(testschema)
    assert outSchema == testschema



