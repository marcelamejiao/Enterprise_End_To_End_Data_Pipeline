"""Spark Structured Streaming consumer for Pipeline B.

Reads purchase/payment events from Kafka topic `order_events`, validates
them, drops duplicates (by event_id, within a watermark window) and late
events beyond the watermark, then writes the deduplicated stream to Delta
Lake (default) or, with --sink snowflake, appends to
RETAIL_DWH.BRONZE.RAW_ORDER_EVENTS via the Snowflake Spark connector.

Usage:
    spark-submit streaming/spark_consumer.py --sink delta --output-path ./streaming/output

Checkpointing (--checkpoint-path) is mandatory for recovery: on restart, Spark
resumes from the last committed Kafka offset and the dedup state store,
rather than reprocessing or dropping events.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
    ]
)


def build_spark(app_name: str = "retail-order-events") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = raw.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")
    return parsed


def dedup_and_validate(events_df):
    """Drop malformed rows, then dedup on event_id within a 10-minute
    watermark so late-arriving duplicates past that window are dropped
    rather than held in state forever."""
    valid = events_df.filter(
        col("event_id").isNotNull()
        & col("order_id").isNotNull()
        & col("event_ts").isNotNull()
    )
    watermarked = valid.withWatermark("event_ts", "10 minutes")
    deduped = watermarked.dropDuplicates(["event_id"])
    return deduped


def write_stream(df, sink: str, output_path: str, checkpoint_path: str):
    writer = df.writeStream.option("checkpointLocation", checkpoint_path)

    if sink == "delta":
        return (
            writer.format("delta")
            .outputMode("append")
            .option("path", output_path)
            .trigger(processingTime="30 seconds")
            .start()
        )
    elif sink == "snowflake":
        # Requires spark-snowflake connector + SNOWFLAKE_* options configured
        # via environment or a separate options dict; kept minimal here.
        import os

        sf_options = {
            "sfURL": os.environ["SNOWFLAKE_URL"],
            "sfUser": os.environ["SNOWFLAKE_USER"],
            "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
            "sfDatabase": "RETAIL_DWH",
            "sfSchema": "BRONZE",
            "sfWarehouse": "TRANSFORM_WH",
            "dbtable": "RAW_ORDER_EVENTS",
        }

        def write_batch(batch_df, batch_id):
            (
                batch_df.write.format("net.snowflake.spark.snowflake")
                .options(**sf_options)
                .mode("append")
                .save()
            )

        return writer.foreachBatch(write_batch).trigger(processingTime="30 seconds").start()
    else:
        raise ValueError(f"Unknown sink: {sink}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="order_events")
    parser.add_argument("--sink", choices=["delta", "snowflake"], default="delta")
    parser.add_argument("--output-path", default="streaming/output/order_events")
    parser.add_argument("--checkpoint-path", default="streaming/checkpoints/order_events")
    args = parser.parse_args()

    spark = build_spark()
    raw_events = read_kafka_stream(spark, args.bootstrap_servers, args.topic)
    clean_events = dedup_and_validate(raw_events)
    query = write_stream(clean_events, args.sink, args.output_path, args.checkpoint_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
