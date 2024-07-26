// Databricks notebook source
// MAGIC %python
// MAGIC local_inbound = '/mnt/dados/inbound/'
// MAGIC dbutils.fs.ls(local_inbound)

// COMMAND ----------

// MAGIC %md
// MAGIC #lendo os dados na camada inbound

// COMMAND ----------

val dados = spark.read.json("dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json")

// COMMAND ----------

display(dados)

// COMMAND ----------

val dados_anuncio = dados.drop("imagens", "usuario")
display(dados_anuncio)

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

var df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC # salvando na camada bronze

// COMMAND ----------

df_bronze.write.format("delta").mode(SaveMode.Overwrite).save("dbfs:/mnt/dados/bronze/dataset_imoveis")

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/dados/bronze/dataset_imoveis"))
