from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

spark = SparkSession.builder \
    .appName("Q4-WordCount") \
    .getOrCreate()

df = spark.read.text("/opt/spark/data/wordcount.txt")

words = df.select(
    explode(split(col("value"), r"\s+")).alias("word")
)

result = (
    words
    .filter(col("word") != "")
    .groupBy("word")
    .count()
    .orderBy(col("count").desc())
)

result.show(20, truncate=False)

spark.stop()