from pyspark import SparkConf
from pyspark.sql import SparkSession

import extract
import transform
import load

from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
from pyspark.sql.functions import split
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd

conf = SparkConf()
conf.setAppName('etl-bi-neoway')
conf.set('spark.mongodb.read.connection.uri', 'mongodb+srv://etl-neoway:pip20po0@mongo-teste.1xrdbcs.mongodb.net/?retryWrites=true&w=majority')
conf.set('spark.mongodb.write.connection.uri', 'mongodb+srv://etl-neoway:pip20po0@mongo-teste.1xrdbcs.mongodb.net/?retryWrites=true&w=majority')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2,com.microsoft.azure:spark-mssql-connector_2.12:1.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.InstanceProfileCredentialsProvider')

spark = SparkSession.builder.config(conf=conf).getOrCreate()



if __name__ == '__main__':
    extract.carga_empresas_azure(spark)
    extract.extract_empresas_azure(spark)
    extract.extract_cnae(spark)
    extract.extract_empresas_calc(spark)
    extract.extract_setores_e_ramos_de_atividades(spark)
    transform.transform_setores_e_ramos_de_atividades(spark)
    transform.transform_empresas_calculo(spark)
    transform.transform_cnae(spark)
    transform.transform_empresas(spark)
    load.criar_dimensoes(spark)
    load.criar_fato(spark)
