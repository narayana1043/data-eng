import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, row_number
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

# -------------------------
# Arguments
# -------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--kafka-bootstrap", required=True)
parser.add_argument("--topic", required=True)
parser.add_argument("--delta-table", required=True)
parser.add_argument("--checkpoint", required=True)
args = parser.parse_args()

# -------------------------
# Spark Session
# -------------------------
spark = (
    SparkSession.builder
    .appName("Kafka-Debezium-Delta-Merge")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Kafka Read
# -------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", args.kafka_bootstrap)
    .option("kafka.bootstrap.servers", "host.docker.internal:9092")
    .option("subscribe", args.topic)
    .option("startingOffsets", "earliest")
    .load()
)

json_df = raw_df.selectExpr("CAST(value AS STRING) as json")

# -------------------------
# Debezium Payload Schema (minimal)
# -------------------------
payload_schema = StructType([
    StructField("after", StructType([
        StructField("film_id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("description", StringType(), True),
        StructField("release_year", IntegerType(), True),
        StructField("language_id", ShortType(), False),
        StructField("rental_duration", ShortType(), False),
        StructField("length", ShortType(), True),
        StructField("rating", StringType(), True),
        StructField("last_update", LongType(), False),
        StructField("special_features", ArrayType(StringType()), True)
    ]), True),
    StructField("op", StringType(), False),
    StructField("ts_ms", LongType(), True)
])

parsed_df = json_df.select(
    from_json(col("json"), StructType([
        StructField("payload", payload_schema)
    ])).alias("data")
)

cdc_df = parsed_df.select("data.payload.*")

# -------------------------
# Flatten
# -------------------------
flat_df = (
    cdc_df
    .select(
        col("after.film_id").alias("film_id"),
        col("after.title").alias("title"),
        col("after.description").alias("description"),
        col("after.release_year").alias("release_year"),
        col("after.language_id").alias("language_id"),
        col("after.rental_duration").alias("rental_duration"),
        col("after.length").alias("length"),
        col("after.rating").alias("rating"),
        (col("after.last_update") / 1_000_000).cast("timestamp").alias("last_update"),
        col("after.special_features").alias("special_features"),
        col("op").alias("cdc_op"),
        col("ts_ms").alias("event_time")
    )
)

# -------------------------
# Deduplicate (latest event wins)
# -------------------------
# window = Window.partitionBy("film_id").orderBy(col("event_time").desc())

# dedup_df = (
#     flat_df
#     .withColumn("rn", row_number().over(window))
#     .filter("rn = 1")
#     .drop("rn")
# )

# -------------------------
# MERGE INTO logic
# -------------------------
def merge_to_delta(batch_df, batch_id):
    window = Window.partitionBy("film_id").orderBy(col("event_time").desc())

    dedup_df = (
        batch_df
        .withColumn("rn", row_number().over(window))
        .filter("rn = 1")
        .drop("rn")
    )

    dedup_df.createOrReplaceTempView("film_staging")

    dedup_df.sparkSession.sql(f"""
        MERGE INTO {args.delta_table} AS tgt
        USING film_staging AS src
        ON tgt.film_id = src.film_id

        WHEN MATCHED AND src.cdc_op = 'd' THEN DELETE

        WHEN MATCHED AND src.cdc_op IN ('u','r') THEN
          UPDATE SET *

        WHEN NOT MATCHED AND src.cdc_op IN ('c','r') THEN
          INSERT *
    """)

# -------------------------
# Start Streaming
# -------------------------
(
    flat_df
    .writeStream
    .foreachBatch(merge_to_delta)
    .option("checkpointLocation", args.checkpoint)
    .start()
    .awaitTermination()
)
