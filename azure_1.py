# Databricks notebook source
dbutils.fs.mount(
    source = "wasbs://raw@practicemanoj01.blob.core.windows.net",
    mount_point = "/mnt/raw4",
    extra_configs= {"fs.azure.account.key.practicemanoj01.blob.core.windows.net":"RscFmdx+SBrlZxOihA8gJWR+SeotpulZh3AwOftJx3+5Y23/+r7ZrlwboI+rmDpdRP0+yGaP4P0Q+AStDmLdrA=="})

# COMMAND ----------

dbutils.fs.ls("/mnt/raw3")

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema = StructType([
    StructField("nflId", IntegerType(), nullable=True),
    StructField("height", StringType(), nullable=True),
    StructField("weight", IntegerType(), nullable=True),
    StructField("date_of_birth", DateType(), nullable=True),
    StructField("college", StringType(), nullable=True),
    StructField("position", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True)
])

df = spark.read.format("csv").schema(schema).options(header='True').load("dbfs:/mnt/raw2/`players`.txt")

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("olympics_players")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from olympics_players limit 5

# COMMAND ----------

college_data = df.groupBy("college").count().orderBy("count", ascending=False)
college_data.show()
