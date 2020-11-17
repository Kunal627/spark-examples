import pytest, sys
from pathlib import Path # if you haven't already done so
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

import avro.schema
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json

schemapath = r'.\data\simpleavro.avsc'
#csvpath = r".\data\people.txt"

def inpschema(schemapath):
    schema1 = open(schemapath, "r").read()
    return json.loads(schema1)

# convert avro datatypes to equivalent Sql types
def avrotoSqldtypes(avrodtype):
    switcher = { 
        "string": StringType(), 
        "int":    IntegerType(),
        "boolean": BooleanType(),
    } 
    
    return switcher.get(avrodtype, NullType)

def mandatory(inpschema):
    try:
        if inpschema["default"] == None:
            return True
    except KeyError:
        return True
    return False

# converts Json object of avro schema to Struct Type
def converttoStruct(inpSchema):
  
    if inpSchema['type'] == "record":
        return StructType([converttoStruct(f) for f in inpSchema['fields']])


    if type(inpSchema['type']) == dict:
        return StructField(inpSchema['name'],converttoStruct(inpSchema['type']))
        
    if inpSchema['type'] ==  "array":
        return ArrayType(converttoStruct(inpSchema['items']), mandatory(inpSchema))
     
    if inpSchema['type'] in ["int", "string"]:
        return StructField(inpSchema['name'], avrotoSqldtypes(inpSchema['type']), mandatory(inpSchema))
  
  #This takes the union type, it takes the datatype at position 1 in the list. This need to be changed to infer datatype from the avroschema
    if type(inpSchema['type']) == list:

        inpSchema['type'].remove('null')
        if inpSchema['type'][0] in ["int", "string","boolean"]:
            return StructField(inpSchema['name'], avrotoSqldtypes(inpSchema['type'][0]), mandatory(inpSchema))
        else:
            return StructField(inpSchema['name'],converttoStruct(inpSchema['type'][0]), mandatory(inpSchema))

schemaobj = inpschema(schemapath)
x = converttoStruct(schemaobj)
print(x)

# Test the schema 

#sc = SparkContext('local[*]', 'Avro schema converter')
#sqlContext = SQLContext(sc)


#lines = sc.textFile(csvpath)
#parts = lines.map(lambda l: l.split(","))
# Each line is converted to a tuple.
#people = parts.map(lambda p: (p[0], int(p[1])))

# The schema is encoded in a string.
#schemaString = "name age"

# Apply the schema to the RDD.
#dfpeople = sqlContext.createDataFrame(people, x)
#dfpeople.show()