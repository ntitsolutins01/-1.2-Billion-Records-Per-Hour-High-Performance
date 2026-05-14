from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, count as spark_count, col, to_json, struct, count, from_json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType
)
# Correto — usar o sum do pyspark.sql.functions
from pyspark.sql import functions as F

KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = "financial_transactions"
AGGREGATES_TOPIC = "transaction_aggregates"
ANOMALIES_TOPIC = "transaction_anomalies"
CHECKPOINT_DIR = "/mnt/spark-checkpoints"
STATES_DIR       = "/mnt/spark-state"          # ✅ corrigido: sem 's'
ANOMALY_THRESHOLD = 10000.0                     # valor acima disso = anomalia

spark = (SparkSession.builder
         .appName("FinancialTransactionProcessor")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
         .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
         .config("spark.sql.streaming.stateStore.stateStoreDir", STATES_DIR)
         .config("spark.sql.shuffle.partitions", 20)
         ).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

transaction_schema = StructType([
    StructField("transactionId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("merchantId", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transactionTime", LongType(), True),
    StructField("transactionType", StringType(), True),
    StructField("location", StringType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("isInternational", StringType(), True),
    StructField("currency", StringType(), True),
])

kafka_stream = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                ).load()

transaction_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", transaction_schema).alias("data")) \
    .select("data.*")

transaction_df = transaction_df.withColumn("transactionTimestamp",
                                           (col("transactionTime") / 1000).cast("timestamp"))

aggregated_df = transaction_df.groupBy("merchantId").agg(
    F.sum("amount").alias("totalAmount"),
    count("*").alias("transactionCount")
)

aggregation_query = aggregated_df \
    .withColumn("key", col("merchantId").cast("string")) \
    .withColumn("value", to_json(struct(
    col("merchantId"),
    col("totalAmount"),
    col("transactionCount")
))).selectExpr("key", "value") \
            .writeStream \
            .format("kafka") \
            .outputMode("update") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("topic", AGGREGATES_TOPIC) \
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates") \
    .start()  # ✅ sem awaitTermination aqui!

# ─── Aguarda ambas as queries ─────────────────────────────────────────────────
spark.streams.awaitAnyTermination()  # ✅ bloqueia esperando qualquer query terminar
