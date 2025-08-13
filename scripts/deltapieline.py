from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Spark Session with Delta Support
builder = SparkSession.builder \
    .appName("DeltaBasic") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Paths
delta_table_path = "data/delta_table"

# First batch - write as Delta
batch1 = spark.read.csv("file:///home/moksh/Desktop/delta_practice/data/Batch1.csv", header=True, inferSchema=True)
batch1.write.format("delta").mode("overwrite").save(delta_table_path)

# Second batch - merge incrementally
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, delta_table_path)
batch2 = spark.read.csv("file:///home/moksh/Desktop/delta_practice/data/Batch2.csv", header=True, inferSchema=True)

deltaTable.alias("old").merge(
    batch2.alias("new"),
    "old.id = new.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Read final table
final_df = spark.read.format("delta").load(delta_table_path)
final_df.show()

spark.stop()
