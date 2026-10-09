# Databricks notebook source
# MAGIC %sql
# MAGIC LIST '/Volumes/samples/databricks/datasets/'

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/songs/data-001/"))

# COMMAND ----------

df = (
    spark.read
    .format("csv")
    .option("sep", "\t")
    .option("header", "false")
    .load("/databricks-datasets/songs/data-001/part-*")
)

display(df.limit(10))