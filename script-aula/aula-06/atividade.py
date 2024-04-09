## 1. Crie um banco de dados no PostgreSQL: Tabelas

## 2. Crie uma aplicação que,quando executada, recebe como parâmetro o caminho
# completo de um arquivo parquet que vai estar em um diretório qualquer, e grava no
# banco de dados Tabelas do PostreSQL, com o nome de tabela que você também deve
# passar como parâmetro.


## OBS: farei essa atividade na plataforma do mongoDB pela facilidade de acessor, usando apenas a url

URL = "mongodb+srv://evanfidk:<minha_senha>@diagnostico.1bfvz9d.mongodb.net/diagnostico.users?retryWrites=true&w=majority&appName=diagnostico"
import sys,getopt
from pyspark.sql import SparkSession

def getParams():
    format,infile,outfile = "","",""
    opts,args = getopt.getopt(sys.argv[1:],"o:,i:")
    print("params: ",opts,args)
    for opt,arg in opts:
        if(opt == "-i"):
            infile = arg
        elif(opt == "-o"):
            outfile = arg

    print("origem: ",infile)
    print("destino: ",outfile)

    return format,infile,outfile;



## partirei do principio que o arquivo será sempre PARQUET

if __name__ == "__main__":

    infile,outfile = getParams();
    spark = SparkSession.builder.appName("atividade06").config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").getOrCreate()
    
    uri = "mongodb+srv://evanfidk:99032303@diagnostico.1bfvz9d.mongodb.net/diagnostico.{}?retryWrites=true&w=majority&appName=diagnostico".format(outfile)

    dados = spark.read.parquet(infile)
    dados.write.format('mongo').option("uri", uri).option("dbtable",outfile).save()

    # lendo bd salvo 
    df_salvo = spark.read.format('mongo').option("uri", uri).load()
    df_salvo.show(5)
    spark.stop()
   

