import sys,getopt
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import * 


if  __name__ == "__main__":
    spark = SparkSession.builder.appName("conversor").getOrCreate()
    opts,args = getopt.getopt(sys.argv[1:],"t:i:o:")
    print("opts",opts)
    print("args",args)
    format,infile,outdir = "","",""

    for opt,arg in opts:
        if(opt == '-t'): #formato
            format = arg
        elif opt == '-i':
            infile = arg #diretorio original
        elif opt == '-o':
            outdir = arg #diretorio destino


    dados = spark.read.csv(infile,header=False,inferSchema = True)
    dados.write.format("console").save()
    print("print: ", format,infile,outdir)
    dados.write.mode('append').format(format).save(outdir)   
    
    spark.stop()








