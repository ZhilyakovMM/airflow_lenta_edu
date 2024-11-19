import sys
from pyspark.sql import SparkSession

schema_name = sys.argv[1]
table_name = sys.argv[2]
user = sys.argv[3]

spark = SparkSession.builder \
                    .appName(f"CTAS_{schema_name}_{table_name}_{user}") \
                    .enableHiveSupport() \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
                    .getOrCreate()

df = spark.sql(f"SELECT * FROM {schema_name}.{table_name} LIMIT 100;")
df.write.format("iceberg").saveAsTable(f"{schema_name}.{table_name}__{user}")

