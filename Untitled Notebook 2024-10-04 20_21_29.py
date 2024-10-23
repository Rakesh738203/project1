# Databricks notebook source
# MAGIC %sql
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dev;
# MAGIC SHOW DATABASES;

# COMMAND ----------

df=spark.read.format("csv").load("abfss://unitycontainer@unitycatalogpratice.dfs.core.windows.net/Book1.csv")
display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://unitycontainer@unitycatalogpratice.dfs.core.windows.net/delta_table")
