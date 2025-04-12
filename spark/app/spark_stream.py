from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# Define schema of incoming Kafka messages
schema = StructType() \
    .add("event_time", StringType()) \
    .add("user_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("page", StringType()) \
    .add("ip", StringType())

# Create SparkSession with Kafka support
spark = SparkSession.builder \
    .appName("KafkaToMongo") \
    .master("spark://spark-master:7077") \
    .config("spark.jars", ",".join([
        "/opt/spark-app/spark-sql-kafka-0-10_2.12-3.4.1.jar",
        "/opt/spark-app/kafka-clients-3.4.0.jar",
        "/opt/spark-app/mongo-spark-connector_2.12-10.1.1.jar"
    ])) \
    .getOrCreate()

# Read stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "event_tracking") \
    .option("startingOffsets", "latest") \
    .load()

# Decode value from bytes to string, then parse JSON
df_json = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# For now, just print to console
query = df_json.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
