from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Initialize Spark Session with Kafka
spark = SparkSession.builder \
    .appName("KafkaStreamReader") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema of Kafka messages
schema = StructType() \
    .add("user", StringType()) \
    .add("action", StringType())

# Read stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-stream") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka message value is binary, convert to string
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Show output to console
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
