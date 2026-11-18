from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, regexp_replace, split


def main():
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_topic = 'topic1'


    df = spark.read.text("input.txt")

    word_counts = (
        df.select(
            explode(
                split(
                    lower(regexp_replace(col("value"), r"[^\w\s]", "")), 
                    r"\s+"
                )
            ).alias("word")
        )
        .filter(col("word") != "")
        .groupBy("word")
        .count()
        .orderBy(col("count").desc())
    )

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "127.0.0.1:9094")
           .option("subscribe", kafka_topic)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    text = F.col("value").cast("string")

    word_counts.show(20)

    spark.stop()
