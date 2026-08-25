import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from pyspark.sql.functions import from_json, col, window

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "orders_clean"

POSTGRES_USER = os.getenv("POSTGRES_USER", "de_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "de_password")
POSTGRES_URL = f"jdbc:postgresql://postgres:5432/warehouse"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# must match the JSON sent by producer.py
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


def write_to_postgres(batch_df, batch_id):
    """Spark calls this every 30 seconds with the latest aggregated rows."""
    if batch_df.rdd.isEmpty():
        return

    batch_df.write.jdbc(
        url=POSTGRES_URL,
        table="live_category_revenue",
        mode="append",
        properties=POSTGRES_PROPERTIES,
    )
    print(f"[spark] Batch {batch_id}: wrote {batch_df.count()} rows to Postgres")


def read_orders_from_kafka(spark):
    """Connect to Kafka and parse JSON messages into typed columns."""
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )

    # Kafka gives us raw bytes; convert to string, then parse using our schema
    json_strings = kafka_stream.selectExpr("CAST(value AS STRING) AS json_str")
    parsed = json_strings.select(from_json(col("json_str"), order_schema).alias("order"))
    orders = parsed.select("order.*")
    return orders


def aggregate_revenue(orders):
    """Group valid orders into 1-minute buckets and sum revenue per category."""
    # safety net: drop bad records even though the producer already cleaned them
    valid_orders = orders.filter((col("unit_price") > 0) & (col("quantity") > 0))

    # convert unix timestamp so we can group by time windows
    with_timestamp = valid_orders.withColumn(
        "event_timestamp",
        col("event_time").cast("timestamp"),
    )

    # 1-minute tumbling window = non-overlapping time buckets (12:00-12:01, 12:01-12:02...)
    windowed = with_timestamp.groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("category"),
    )

    aggregated = windowed.agg(
        {"total_amount": "sum", "order_id": "count"}
    ).withColumnRenamed("sum(total_amount)", "total_revenue")
    aggregated = aggregated.withColumnRenamed("count(order_id)", "order_count")

    # flatten the window struct into start/end columns for Postgres
    result = aggregated.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("total_revenue"),
        col("order_count"),
    )
    return result


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaOrdersStreamingToPostgres")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    orders = read_orders_from_kafka(spark)
    revenue_per_minute = aggregate_revenue(orders)

    query = (
        revenue_per_minute.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", "/tmp/spark_checkpoints/orders_stream")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
