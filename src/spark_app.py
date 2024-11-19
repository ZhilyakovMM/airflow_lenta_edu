import sys
from pyspark.sql import SparkSession

schema_table_name = sys.argv[1]
user = sys.argv[2]

spark = SparkSession.builder \
                    .appName(f"CTAS_{schema_table_name}") \
                    .enableHiveSupport() \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
                    .getOrCreate()

df = spark.sql(f"SELECT * FROM {schema_table_name} LIMIT 100;")
df.write.format("iceberg").saveAsTable(f"spark__{schema_table_name}__user")

