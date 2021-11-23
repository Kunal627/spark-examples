from pyspark.sql import SparkSession
#from pyspark.sql.functions import
from pyspark.sql import SQLContext
#from pyspark.sql.functions import from_json, col

spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()
columns = ["Emp","Sal"]
data = [("1", "20000"), ("2", "100000"), ("3", "3000")]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF()
df.show()
#df.write.format("csv").save('mycsv')