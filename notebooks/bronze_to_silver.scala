// Databricks notebook source
var df_bronze = spark.read.format("delta").load("dbfs:/mnt/dados/bronze/dataset_imoveis/")
display(df_bronze)

// COMMAND ----------

var df_with_coluns = df_bronze.select("anuncio.*", "anuncio.endereco.*")
display(df_with_coluns)

// COMMAND ----------

var df_silver = df_with_coluns.drop("caracteristicas", "endereco")

// COMMAND ----------

display(df_silver)

// COMMAND ----------

df_silver.write.format("delta").mode("overwrite").save("dbfs:/mnt/dados/silver/dataset_imoveis/")

// COMMAND ----------


