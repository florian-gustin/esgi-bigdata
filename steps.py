from pyspark.sql.functions import col, to_timestamp, current_timestamp, expr
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc
from pyspark.sql.functions import desc, explode, lower
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from time import sleep
import time
from pyspark.storagelevel import StorageLevel

# Création de la SparkSession
spark = SparkSession \
    .builder \
    .appName("spark-al") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "3g") \
    .config("spark.executor.cores", 4) \
    .config("spark.driver.memory", "3g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()

start_time = time.time()

data_df = spark.read.parquet("/app/output/full.parquet")

# 1. Les 10 projets Github pour lesquels il y a eu le plus de commits

## transformation et affichage
top_10_commits = data_df.filter(data_df.commit.isNotNull()) \
                    .filter(data_df.repo.isNotNull())\
                    .groupBy("repo").count() \
                    .sort(desc("count")).show(n=10, truncate=False)

print("--- %s seconds ---" % (time.time() - start_time))

# 2. Le plus gros contributeur sur le projet "apache/spark"
top_contributor = data_df.filter(col("repo") == "apache/spark") \
                    .groupBy("author").count().sort(desc("count")).show(n=1, truncate=False)

print("--- %s seconds ---" % (time.time() - start_time))



# 3. Les plus gros contributeurs du projet apache/spark sur les 4 dernières années

# transformation et affichage
data_df_converted = data_df.withColumn("date", to_timestamp(
    col("date"), "EEE MMM dd HH:mm:ss yyyy Z"))

data_filtered = data_df_converted.filter((col("date") >= (current_timestamp() - expr("INTERVAL 4 YEARS")))
                        & (col("date") <= current_timestamp()))

top_contributors_4y = data_filtered.filter(
    col("repo") == "apache/spark").groupBy("author").count().sort(desc("count"))

top_contributors_4y.show()

print("--- %s seconds ---" % (time.time() - start_time))


# 4. Stopwords

# Changement fichier de stopwords
stopwords_file = "/app/input/englishST.txt"
with open(stopwords_file, "r") as f:
    stopwords = [word.strip().lower() for word in f.readlines()]

# Tokeniser les messages de validation en mots
tokenizer = Tokenizer(inputCol="commit", outputCol="words")
df_words = tokenizer.transform(data_df)

# Création de l'instance StopWordsRemover (BONUS !!!)
stopwords_remover = StopWordsRemover(
    inputCol="words", outputCol="filtered_words", stopWords=stopwords)

# Appliquer la suppression des mots vides pour valider les mots
df_filtered = stopwords_remover.transform(df_words).select("filtered_words")

# Divisez les mots de validation filtrés en mots individuels et comptez les occurrences de mots
word_counts = df_filtered.select(explode("filtered_words").alias("word")) \
    .filter((lower("word") != "") & (lower("word") != "*") & (lower("word") != ">") & (lower("word") != "-"))  \
    .groupBy("word").count().orderBy(desc("count")).limit(10)

word_counts.show(truncate=False)

print("--- %s seconds ---" % (time.time() - start_time))

sleep(1000)