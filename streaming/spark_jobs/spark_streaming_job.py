import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from pyspark.sql.functions import from_json, col, window, sum as spark_sum, count as spark_count

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "orders_clean"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/warehouse"
POSTGRES_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER", "de_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "de_password"),
    "driver": "org.postgresql.Driver",
}

# must match the JSON shape sent by producer.py
order_schema = (
    StructType()
    .add("order_id", LongType())
    .add("customer_name", StringType())
    .add("product_name", StringType())
    .add("category", StringType())
    .add("unit_price", DoubleType())
    .add("quantity", LongType())
    .add("country", StringType())
    .add("event_time", DoubleType())
    .add("total_amount", DoubleType())
)


def write_batch_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write
        .jdbc(
            url=POSTGRES_URL,
            table="live_category_revenue",
            mode="append",
            properties=POSTGRES_PROPERTIES,
        )
    )
    print(f"[spark] Batch {batch_id}: wrote {batch_df.count()} aggregated rows to Postgres")


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaOrdersStreamingToPostgres")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    orders = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), order_schema).alias("data"))
        .select("data.*")
    )

    valid_orders = orders.filter((col("unit_price") > 0) & (col("quantity") > 0))

    # 1-minute tumbling windows: revenue per category in non-overlapping time buckets
    aggregated = (
        valid_orders
        .withColumn("event_timestamp", (col("event_time")).cast("timestamp"))
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("category"),
        )
        .agg(
            spark_sum("total_amount").alias("total_revenue"),
            spark_count("order_id").alias("order_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("category"),
            col("total_revenue"),
            col("order_count"),
        )
    )

    query = (
        aggregated.writeStream
        .outputMode("update")
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", "/tmp/spark_checkpoints/orders_stream")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
