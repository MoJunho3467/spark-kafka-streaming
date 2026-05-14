from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
import os
import shutil
import time
import json

# Docker Spark 컨테이너 안에서 Kafka 컨테이너에 접속하므로 컨테이너 이름 사용
BOOTSTRAP = "kafka-week7:9092"

spark = SparkSession.builder \
    .appName("ColabKafkaTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def stop_all_streams():
    for q in spark.streams.active:
        print(f"Stopping active query: {q.name}")
        q.stop()
    time.sleep(2)

def reset_path(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"Deleted: {path}")

def debug_batch(df, epoch_id):
    print(f"\n==== Batch {epoch_id} ====")
    try:
        if df.isEmpty():
            print("이 배치에는 데이터가 없습니다.")
            return

        if "window_start" in df.columns:
            sorted_df = df.orderBy("window_start")
        elif "event_time_ts" in df.columns:
            sorted_df = df.orderBy("event_time_ts")
        elif "impression_time_ts" in df.columns:
            sorted_df = df.orderBy("impression_time_ts")
        else:
            sorted_df = df

        sorted_df.show(20, truncate=False)

    except Exception as e:
        print(f"배치 {epoch_id} 표시 중 오류 발생: {e}")


# Stream-Stream Join 예제

stop_all_streams()
reset_path("/tmp/ckpt/stream_stream_join")

impression_schema = StructType([
    StructField("impression_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("ad_id", StringType(), True),
    StructField("impression_time", StringType(), True),
    StructField("campaign", StringType(), True),
])

click_schema = StructType([
    StructField("click_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("ad_id", StringType(), True),
    StructField("click_time", StringType(), True),
    StructField("device", StringType(), True),
])

raw_impressions = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", "impressions")
    .option("startingOffsets", "latest")
    .load()
)

raw_clicks = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", "clicks")
    .option("startingOffsets", "latest")
    .load()
)

impressions_df = (
    raw_impressions
    .select(F.from_json(F.col("value").cast("string"), impression_schema).alias("data"))
    .select("data.*")
    .withColumn("impression_time_ts", F.to_timestamp("impression_time"))
    .withWatermark("impression_time_ts", "2 minutes")
)

clicks_df = (
    raw_clicks
    .select(F.from_json(F.col("value").cast("string"), click_schema).alias("data"))
    .select("data.*")
    .withColumn("click_time_ts", F.to_timestamp("click_time"))
    .withWatermark("click_time_ts", "2 minutes")
)

joined_df = (
    impressions_df.alias("i")
    .join(
        clicks_df.alias("c"),
        on=[
            F.col("i.user_id") == F.col("c.user_id"),
            F.col("i.ad_id") == F.col("c.ad_id"),
            F.col("c.click_time_ts") >= F.col("i.impression_time_ts"),
            F.col("c.click_time_ts") <= F.col("i.impression_time_ts") + F.expr("INTERVAL 2 minutes")
        ],
        how="inner"
    )
    .select(
        F.col("i.impression_id"),
        F.col("c.click_id"),
        F.col("i.user_id"),
        F.col("i.ad_id"),
        F.col("i.campaign"),
        F.col("c.device"),
        F.col("i.impression_time_ts"),
        F.col("c.click_time_ts")
    )
)

query_join = (
    joined_df.writeStream
    .foreachBatch(debug_batch)
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/ckpt/stream_stream_join")
    .trigger(processingTime="10 seconds")
    .start()
)

query_join.awaitTermination(120)