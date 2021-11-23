###############################################
# to get a schema block from avro schema file
###############################################

import json

schemapath = r'.\data\avroschema.avsc'

schema = open(schemapath, "r").read()
schema = json.loads(schema)
schema = schema["fields"][0]["type"]["fields"]
schemablock = {}
for f in  schema:
#    print(f)
    if f["name"] == "parameter":
        schemablock = f

print(schemablock)