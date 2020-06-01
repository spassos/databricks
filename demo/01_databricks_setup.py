# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Demo - 01 Databricks Setup do ambiente

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Databricks command-line interface [CLI]
# MAGIC 
# MAGIC > https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html  
# MAGIC > https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html
# MAGIC 
# MAGIC 1. installing databricks-cli
# MAGIC > pip install databricks-cli  
# MAGIC 
# MAGIC 2. create a secret and scope
# MAGIC 
# MAGIC > databricks secrets create-scope --scope bs-az-databricks --initial-manage-principal users   
# MAGIC 
# MAGIC > databricks secrets put --scope bs-az-databricks --key key-bs-az-databricks 
# MAGIC 
# MAGIC > databricks secrets create-scope --scope sqldb-az-databricks --initial-manage-principal users 
# MAGIC 
# MAGIC > databricks secrets put --scope sqldb-az-databricks --key username-az-databricks 
# MAGIC 
# MAGIC > databricks secrets put --scope sqldb-az-databricks --key password-az-databricks 
# MAGIC 
# MAGIC > databricks secrets put --scope sqldb-az-databricks --key host-az-databricks 
# MAGIC 
# MAGIC > databricks secrets list-scopes 
# MAGIC <br>
# MAGIC 
# MAGIC > this will add a layer of security where credentials will be kept save in a key vault (key-value) pair storage
# MAGIC <br>
# MAGIC 
# MAGIC > if you want, set up using Azure Cloud Shell.  
# MAGIC > [Databricks CLI & Azure Cloud Shell](https://docs.microsoft.com/pt-pt/azure/azure-databricks/databricks-cli-from-azure-cloud-shell)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 1: Instalar o Databricks CLI e criar os secrets de token e senhas.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Passo 2: Definir variáveis do Data Lake.

# COMMAND ----------

storage_account_name = 'sntdatalake2'
container_name_stg = 'bs-stage'
container_name_prd = 'bs-production'
file_type = 'csv'
file_location_stg = "wasbs://"+container_name_stg+"@"+storage_account_name+".blob.core.windows.net/" 
file_location_prd = "wasbs://"+container_name_prd+"@"+storage_account_name+".blob.core.windows.net/" 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 3: Montar Storage [bs-stage] e visualizar o arquivo montado via [dbutils](https://docs.databricks.com/dev-tools/databricks-utils.html)

# COMMAND ----------

dbutils.fs.mount(
  source = file_location_stg,
  mount_point = "/mnt/"+container_name_stg,
  extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":dbutils.secrets.get(scope="bs-az-databricks", key="key-bs-az-databricks")})

# COMMAND ----------

display(dbutils.fs.ls("/mnt/"+container_name_stg))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 4: Montar Storage [bs-production]

# COMMAND ----------

dbutils.fs.mount(
  source = file_location_prd,
  mount_point = "/mnt/"+container_name_prd,
  extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":dbutils.secrets.get(scope="bs-az-databricks", key="key-bs-az-databricks")})

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 5: Ler o arquivo .CSV e carregar no Dataframe
# MAGIC 
# MAGIC Agora iremos carregar o nosso arquivo CSV para o DataFrame e utilizar a *option* header e inferSchema
# MAGIC 
# MAGIC <https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html>

# COMMAND ----------

df = spark.read.format(file_type).option("header", "true").option("inferSchema", "true").load("/mnt/"+container_name_stg)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 6: Visualizar os dados
# MAGIC Vamos vizualizar os dados para saber se foram carregados corretamente no DataFrame.
# MAGIC 
# MAGIC Será utilizado o dataset com os dados do coronavírus no pessoal da [brasil.io](https://brasil.io/dataset/covid19/caso/) que reunem os dados das Secretarias Estaduais de Saúde. 

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 7: Salvar o DataFrame no formato Parquet
# MAGIC 
# MAGIC ![parquet](https://upload.wikimedia.org/wikipedia/en/0/09/Apache_Parquet_Logo.svg)
# MAGIC 
# MAGIC O [Apache Parquet](https://parquet.apache.org/) é um formato de armazenamento de dados gratuito e de código aberto orientado a colunas do ecossistema [Apache Hadoop](https://hadoop.apache.org/). É semelhante aos outros formatos de arquivo de armazenamento colunar disponíveis no Hadoop, nomeadamente [RCFile](https://cwiki.apache.org/confluence/display/Hive/RCFile) e [ORC](https://orc.apache.org/). É compatível com a maioria das estruturas de processamento de dados no ambiente Hadoop. Ele fornece esquemas eficientes de compactação e codificação de dados com desempenho aprimorado para lidar com dados complexos em massa.

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/bs-production/pq_data/covid_19.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 8: Listar os arquivos Parquet criados.

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/bs-production/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 9: (Opcional) Desmontar os pontos de montagem

# COMMAND ----------

dbutils.fs.unmount("/mnt/bs-stage")
dbutils.fs.unmount("/mnt/bs-production")