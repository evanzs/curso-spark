
# tornar o pyspark "importável"
import findspark
findspark.init

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 


if __name__ == "__main__":
    spark = SparkSession.builder.appName("exemplo").getOrCreate()
    arqSchema = "id INT,nome STRING,status STRING,cidade STRING, vendas INT,data STRING"
    desp = spark.read.csv("./../../datasets/despachantes.csv",header=False,schema=arqSchema)
    result = desp.groupBy('data').agg(sum('vendas'))

    result.write.format("console").save()
    spark.stop()