
# tornar o pyspark "importável"
import findspark
findspark.init

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 


if __name__ == "__main__":
    spark = SparkSession.builder.appName("exemplo").getOrCreate()
    df = spark.createDataFrame([1,2,3,4,5],IntegerType())
    df.write.format("console").save()
    spark.stop()