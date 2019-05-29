// Databricks notebook source
val userName = dbutils.secrets.get(scope = "training-scope", key = "adbcclientsecret")

print(userName)

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.iomegastoragev3.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.iomegastoragev3.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.iomegastoragev3.dfs.core.windows.net", "f7ac59dd-d35e-437b-b74a-b3f300c7e0fa")
spark.conf.set("fs.azure.account.oauth2.client.secret.iomegastoragev3.dfs.core.windows.net", userName)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.iomegastoragev3.dfs.core.windows.net", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://data@iomegastoragev3.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

// MAGIC %sh 
// MAGIC 
// MAGIC wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

// COMMAND ----------

dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://data@iomegastoragev3.dfs.core.windows.net/")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS radio_sample_data;
// MAGIC CREATE TABLE radio_sample_data
// MAGIC USING json
// MAGIC OPTIONS (
// MAGIC  path  "abfss://data@iomegastoragev3.dfs.core.windows.net/small_radio_json.json"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC Select * from radio_sample_data