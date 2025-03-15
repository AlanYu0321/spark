from pyspark.sql import SparkSession
from pyspark import SparkConf

# spark-submit --packages mysql:mysql-connector-java:8.0.33 module-07.py

conf = SparkConf()
conf.set("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")

spark = SparkSession.builder.appName("Module07").config(conf=conf).getOrCreate()

# Suppress INFO logs, only show warnings and errors
spark.sparkContext.setLogLevel("WARN")

# MySQL Connection Properties
mysql_url = "jdbc:mysql://localhost:3306/employees"
mysql_properties = {
    "user": "worker",
    "password": "cluster",
    "driver": "com.mysql.cj.jdbc.Driver",
}

# Read MySQL Tables into DataFrames
employees_df = spark.read.jdbc(
    url=mysql_url, table="employees", properties=mysql_properties
)
salaries_df = spark.read.jdbc(
    url=mysql_url, table="salaries", properties=mysql_properties
)

emp_salaries_df = employees_df.alias("e").join(
    salaries_df, salaries_df.emp_no == employees_df.emp_no
).select(
    "e.emp_no",
    "birth_date",
    "first_name",
    "last_name",
    "gender",
    "hire_date",
    "salary",
    "from_date",
    "to_date"
)

# Save the joined DataFrame to Parquet
parquet_path = "./emp_salaries.parquet"
emp_salaries_df.write.mode("overwrite").parquet(parquet_path)

# Read Parquet back into PySpark DataFrame
emp_salaries_parquet_df = spark.read.parquet(parquet_path)

# Write the Parquet data back to MySQL with specified numPartitions
# Adjust based on dataset size and parallelism needs num_partitions = 10 
# ** unpacks the dictionary
emp_salaries_parquet_df.repartition(10).write.format("jdbc").options(
    url=mysql_url, dbtable="emp_salaries", **mysql_properties
).mode("overwrite").save()
