from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import sys

# Initialize Spark Session
spark = SparkSession.builder.appName("Module04-DivvyTrips").getOrCreate()

# Get file path dynamically from command-line argument
# sys.argv is a list in Python that stores command-line arguments
if len(sys.argv) != 2:
    print("Usage: spark-submit module-04.py <path-to-csv>")
    sys.exit(1)

file_path = sys.argv[1]

# Read CSV file with inferred schema
df_infer = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
print("Schema inferred from CSV:")
df_infer.printSchema()
print(f"Record count: {df_infer.count()}")

# StructType schema
schema_struct = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", IntegerType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])

df_struct = spark.read.option("header", True).schema(schema_struct).csv(file_path)
print("\nStructType schema:")
df_struct.printSchema()
print(f"Record count: {df_struct.count()}")

# DDL schema
schema_ddl = """
    trip_id INT,
    starttime STRING,
    stoptime STRING,
    bikeid INT,
    tripduration INT,
    from_station_id INT,
    from_station_name STRING,
    to_station_id INT,
    to_station_name STRING,
    usertype STRING,
    gender STRING,
    birthyear STRING
"""

df_ddl = spark.read.option("header", True).schema(schema_ddl).csv(file_path)
print("\nDDL schema:")
df_ddl.printSchema()
print(f"Record count: {df_ddl.count()}")

# Stop Spark Session
spark.stop()