#EX1:  Read a file where every row is a Json String and explode it into columns without external schema

from pyspark import SparkContext
#from pyspark.sql.functions import
from pyspark.sql import SQLContext
#from pyspark.sql.functions import from_json, col
json_file_path = r'.\data\inp.json'
sc = SparkContext('local[*]', 'Avro schema converter')
sqlContext = SQLContext(sc)
df=sqlContext.read.json(json_file_path)
#logJson is column name
new_df1 = sqlContext.read.json(df.rdd.map(lambda row : row.logJson))
new_df1.printSchema()
new_df1.show()
schema = new_df1.schema
