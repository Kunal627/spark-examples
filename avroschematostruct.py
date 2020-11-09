import avro.schema
from pyspark.sql.types import *
import json

schemapath = 'avroschema.avsc'

schema1 = open(schemapath, "r").read()
schemaobj = json.loads(schema1)

# convert avro datatypes to equivalent Sql types
def avrotoSqldtypes(avrodtype):
    switcher = { 
        "string": StringType(), 
        "int":    IntegerType(),
        "boolean": BooleanType(),
    } 
    
    return switcher.get(avrodtype, NullType)

# converts Json object of avro schema to Struct Type
def converttoStruct(inpSchema):
    if inpSchema['type'] == "record":
        return StructType([converttoStruct(f) for f in inpSchema['fields']])


    if type(inpSchema['type']) == dict:
        return StructField(inpSchema['name'],converttoStruct(inpSchema['type']))
        
    if inpSchema['type'] ==  "array":
        return ArrayType(converttoStruct(inpSchema['items']), False)
     
    if inpSchema['type'] in ["int", "string"]:
        return StructField(inpSchema['name'], avrotoSqldtypes(inpSchema['type']), False)
  
  #This takes the union type, it takes the datatype at position 1 in the list. This need to be changed to infer datatype from the avroschema
    if type(inpSchema['type']) == list:
        if inpSchema['type'][1] in ["int", "string","boolean"]:
            return StructField(inpSchema['name'], avrotoSqldtypes(inpSchema['type'][1]), False)
        else:
            return StructField(inpSchema['name'],converttoStruct(inpSchema['type'][1]), False)


x = converttoStruct(schemaobj)
print(x)
