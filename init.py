from pyspark.sql.functions import col, to_timestamp, current_timestamp, expr
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc
from pyspark.storagelevel import StorageLevel
from time import sleep
import time

# Création de la SparkSession
spark = SparkSession \
    .builder \
    .appName("spark-al") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", 4) \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()

# Path du fichier full.csv
data = "/app/input/full.csv"

## lecture du fichier
data_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data, format="csv") \
    .repartition(10) 

data_df.write \
.option("compression", "none") \
.mode("overwrite") \
.save("/app/output/full.parquet")