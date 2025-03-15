from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# Initialize Spark Session
# --conf spark.sql.catalogImplementation=hive
spark = (
    SparkSession.builder.appName("Module06-Flights")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)

# Suppress INFO logs, only show warnings and errors
spark.sparkContext.setLogLevel("WARN")

# read file from sf-fire-calls.csv
if len(sys.argv) != 2:
    print("Usage: spark-submit module-06.py <path-to-csv>")
    sys.exit(1)

file_path = sys.argv[1]

flight_schema = StructType(
    [
        StructField("date", StringType(), True),
        StructField("delay", IntegerType(), True),
        StructField("distance", IntegerType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
    ]
)


flight_df = spark.read.csv(file_path, header=True, schema=flight_schema)


# Assignment Details - Part I
# As an exercise, convert the date column into a readable format and find the days or months when these delays were most common.
# Were the delays related to winter months or holidays?

# Make date column readable.
flight_tf_df = flight_df.withColumn(
    "formatted_date",
    concat_ws(
        " ",
        concat_ws(
            "/", substring(col("date"), 1, 2), substring(col("date"), 3, 2)
        ),  # MM/dd
        concat_ws(
            ":", substring(col("date"), 5, 2), substring(col("date"), 7, 2)
        ),  # HH:mm
    ),
)

# Create a table name us_delay_flights_tbl
flight_tf_df.createOrReplaceTempView("us_delay_flights_tbl")

# Query
spark.sql(
    """SELECT formatted_date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC"""
).show(10)

# use the CASE clause in SQL. In the fol‐ lowing example, we want to label all US flights, regardless of origin and destination,
#  with an indication of the delays they experienced: Very Long Delays (> 6 hours),
#  Long Delays (2–6 hours), etc. We’ll add these human-readable labels in a new column called Flight_Delays:
spark.sql(
    """SELECT delay, origin, destination,
        CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
            WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
            WHEN delay > 0 and delay < 60  THEN  'Tolerable Delays'
            WHEN delay = 0 THEN 'No Delays'
            ELSE 'Early'
        END AS Flight_Delays
        FROM us_delay_flights_tbl
        ORDER BY origin, delay DESC"""
).show(10)

# Assignment Details - Part II
# Create a temp view of flights from Chicago (ORD) between 03/01 and 03/15
march_flights = spark.sql(
    """
    SELECT formatted_date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE origin = 'ORD' AND CONCAT(SUBSTRING(formatted_date, 1, 2),SUBSTRING(formatted_date, 4, 5)) BETWEEN '0301' AND '0315'
"""
)

# Create a temporary view for these March flights
march_flights.createOrReplaceTempView("march_chicago_flights")

# Show the first 5 records of the temp view
spark.sql("SELECT * FROM march_chicago_flights LIMIT 5").show()

# Use Spark catalog to list the columns of us_delay_flights_tbl
print(spark.catalog.listColumns("us_delay_flights_tbl"))

# Assignment Details - Part III
flight_df = flight_df.withColumn("date", to_date(to_timestamp(col("date"), "MMddHHmm")))
flight_df.printSchema()

# JSON
flight_df.write.mode("overwrite").json("./departuredelays/json")
# JSON with lz4 compression
flight_df.write.mode("overwrite").option("compression", "lz4").json("./departuredelays/json_lz4")
# Parquet
flight_df.write.mode("overwrite").parquet("./departuredelays/parquet")

# Assignment Details - Part IV
ord_df = spark.read.parquet("./departuredelays/parquet")
ord_departure_delays = ord_df.filter(ord_df.origin == "ORD")
ord_departure_delays.show(10)
ord_departure_delays.write.mode("overwrite").parquet("./ordDeparturedelays/parquet")
