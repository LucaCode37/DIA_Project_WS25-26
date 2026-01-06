from pyspark.sql import SparkSession

spark= (
    SparkSession.builder
    .appName("DIA Spark Test")
    .getOrCreate()
)

stations_path = "/opt/spark-data/station_data.json"
df = spark.read.option("multiLine", "true").json(stations_path)

print(df.columns)
df.printSchema()

spark.stop()