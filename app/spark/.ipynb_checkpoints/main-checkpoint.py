from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType

conf = SparkConf()
conf.setAppName("etl-bi-neoway")
conf.setMaster("local")
conf.set("spark.ui.port", "5372")
conf.set("spark.driver.bindAddress","localhost")
conf.set("spark.mongodb.read.connection.uri", "mongodb+srv://etl-neoway:pip20po0@mongo-teste.1xrdbcs.mongodb.net/?retryWrites=true&w=majority")
conf.set("spark.mongodb.write.connection.uri", "mongodb+srv://etl-neoway:pip20po0@mongo-teste.1xrdbcs.mongodb.net/?retryWrites=true&w=majority")

spark = SparkSession.builder.config(conf=conf).getOrCreate()