import sys
from pyspark.sql import SparkSession

table_name = sys.argv[1]

spark = SparkSession.builder \
                    .appName(f"select_{table_name}") \
                    .enableHiveSupport() \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
                    .getOrCreate()

df = spark.sql(f"SELECT * FROM {table_name}")

