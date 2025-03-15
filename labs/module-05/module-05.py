from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

# Initialize Spark Session
spark = SparkSession.builder.appName("Module05-SFFireCalls").getOrCreate()

# read file from sf-fire-calls.csv
if len(sys.argv) != 2:
    print("Usage: spark-submit module-05.py <path-to-csv>")
    sys.exit(1)

file_path = sys.argv[1]

fire_df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
# 1.What were all the different types of fire calls in 2018?
# List all types
fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().show()
# The number of distinct type
fire_df.select("CallType").where(col("CallType").isNotNull()).agg(
    countDistinct("callType").alias("DistinctCallTypes")
).show()

# 2.What months within the year 2018 saw the highest number of fire calls?
fire_ts_df = (
    fire_df.withColumn("Call_date", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("Watch_date", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn(
        "AvailableDt", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop("AvailableDtTm")
)

fire_ts_df.filter(year("Call_date") == 2018).groupBy(month("Call_date")).count().orderBy("count", ascending=False).show(10)

# 3.Which neighborhood in San Francisco generated the most fire calls in 2018?
fire_ts_df.filter(year("Call_date") == 2018).groupBy("Neighborhood").count().orderBy("count", ascending=False).show(10)

# 4.Which neighborhoods had the worst response times to fire calls in 2018?
fire_ts_df.filter(year("Call_date") == 2018).groupBy("Neighborhood").agg(sum("Delay").alias("TotalDelay")).orderBy("TotalDelay", ascending=False).show(10)

# 5.Which week in the year in 2018 had the most fire calls?
fire_ts_df.filter(year("Call_date") == 2018).groupBy(weekofyear("Call_date")).count().orderBy("count", ascending=False).show(10)

# 6.Is there a correlation between neighborhood, zip code, and number of fire calls?
fire_ts_df.groupBy("Neighborhood","ZipCode").count().orderBy("count", ascending=False).show()

# 7.How can we use Parquet files or SQL tables to store this data and read it back?
fire_ts_df.write.format("parquet").mode("overwrite").saveAsTable("FireCalls")
spark.sql("SELECT * FROM FireCalls").show(10)