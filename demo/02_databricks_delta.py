# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Demo 02 - Databricks Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![fluxo_delta](https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png)
# MAGIC 
# MAGIC O Delta Lake é uma camada de armazenamento de software livre que traz confiabilidade para os _Data lake_. O Delta Lake fornece transações ACID, tratamento de metadados escalonáveis e unifica o processamento de dados de lote e streaming. O Delta Lake é executado sobre o data Lake existente e é totalmente compatível com as APIs de Apache Spark.
# MAGIC Especificamente, o Delta Lake oferece:
# MAGIC 
# MAGIC * Transações ACID no Spark: níveis de isolamento serializáveis asseguram que os leitores nunca vejam dados inconsistentes.
# MAGIC * Manipulação de metadados escalonável: aproveita a capacidade de processamento distribuído do Spark para lidar com todos os metadados para tabelas de escala de petabytes com bilhões de arquivos com facilidade.
# MAGIC * Unificação de streaming e lote: uma tabela no Delta Lake é uma tabela de lote, bem como uma origem e um coletor de streaming. A ingestão de dados de streaming, o aterramento histórico de lote, as consultas interativas acabam funcionando imediatamente.
# MAGIC * Imposição de esquema: manipula automaticamente as variações de esquema para evitar a inserção de registros inválidos durante a ingestão.
# MAGIC * Viagem de tempo: o controle de versão de dados permite reversões, trilhas de auditoria de histórico completo e experimentos de aprendizado de máquina reproduzíveis.
# MAGIC * Upserts e Delete: dá suporte a operações de mesclagem, atualização e exclusão para habilitar casos de uso complexos, como Change-Data-Capture, operações SCD (slowly-Changing-Dimension), streaming Upserts e assim por diante.
# MAGIC * O Delta Lake no Azure Databricks permite que você configure o Delta Lake com base em seus padrões de carga de trabalho e forneça layouts e índices otimizados para consultas interativas rápidas. Para obter informações sobre o Delta Lake em Azure Databricks, consulte otimizações.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 1: Criar e usar database

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- creating a database for logical organization
# MAGIC CREATE DATABASE IF NOT EXISTS CovidBrasil;
# MAGIC USE CovidBrasil

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 2: Listar o arquivo .parquet

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/bs-production/pq_data/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 3.1: Ler o arquivo Parquet, adicionar no DataFrame e criar uma tabela temporária em Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // load data into a scala dataframe
# MAGIC val ds_covid = spark.read.parquet("dbfs:/mnt/bs-production/pq_data/covid_19.parquet/")
# MAGIC 
# MAGIC // register as a temporary view
# MAGIC ds_covid.createOrReplaceTempView("sc_ds_covid")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 3.2: Ler o arquivo Parquet, adicionar no DataFrame e criar uma tabela temporária em Python

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC # load data into a python dataframe
# MAGIC ds_covid = spark.read.parquet("dbfs:/mnt/bs-production/pq_data/covid_19.parquet/")
# MAGIC 
# MAGIC # register as a temporary view [sql]
# MAGIC ds_covid.createOrReplaceTempView("py_ds_covid")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 3.3: Ler o arquivo Parquet, adicionar no DataFrame e criar uma tabela temporária em SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- remove if not exists
# MAGIC DROP VIEW IF EXISTS ds_covid;
# MAGIC 
# MAGIC -- create temporary view
# MAGIC CREATE TEMPORARY VIEW ds_covid
# MAGIC USING org.apache.spark.sql.parquet
# MAGIC OPTIONS ( path "dbfs:/mnt/bs-production/pq_data/covid_19.parquet/" );
# MAGIC 
# MAGIC -- select data
# MAGIC SELECT * FROM ds_covid

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 4: Visualiza o Dataframe e mostra informações no nivel de Storage 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC ds_covid.cache()
# MAGIC display(ds_covid)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passo 5: Contabiliza as linhas do DataFrame 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # importing library to aggregate data
# MAGIC from pyspark.sql.functions import count
# MAGIC 
# MAGIC # select and count rows
# MAGIC ds_covid.select(count("*")).take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 6: Mostra o [plano de execução](https://docs.databricks.com/spark/latest/spark-sql/language-manual/explain.html#) da consulta.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- write a dataframe/dataset or sql
# MAGIC -- if command valid, convertst into a logical plan
# MAGIC -- spark transforms the logical plan into a physical plan
# MAGIC -- spark executes this plan [rdd] in a cluster
# MAGIC EXPLAIN EXTENDED SELECT * FROM ds_covid

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 7: Printa o Metadado do DataFrame

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC # print schema
# MAGIC # you can explicitly infer a schema
# MAGIC # better to get more precision
# MAGIC # if needed
# MAGIC ds_covid.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 8: Cria uma tabela bronze Delta Lake

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # write dataframe [ds_covid] into data lake [azure blob storage]
# MAGIC # selecing a location for the proper design pattern
# MAGIC ds_covid.write.mode("overwrite").format("delta").save("dbfs:/mnt/bs-production/delta/bronze_covid/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 9: Lê uma tabela bronze Delta

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # read delta table [querying from data lakehouse]
# MAGIC bronze_covid = spark.read.format("delta").load("dbfs:/mnt/bs-production/delta/bronze_covid/")
# MAGIC 
# MAGIC display(bronze_covid)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 10: Criação de função para classificar a letalidade

# COMMAND ----------

# creating function in python to classify letality
def cvd_letality(letality_score):
  if letality_score >= 0.10:
    return "muito alto"
  if letality_score > 0.05 and letality_score < 0.10:
    return "alto"
  if letality_score > 0.02 and letality_score < 0.05:
    return "médio"
  else:
    return "baixo"

# registering udf function to spark's core
spark.udf.register("cvd_letality", cvd_letality) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 11: Cria uma tabela temporária bronze

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE TEMPORARY VIEW bronze_covid
# MAGIC USING org.apache.spark.sql.parquet
# MAGIC OPTIONS ( path "dbfs:/mnt/bs-production/delta/bronze_covid/" );

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 12: Utilização da função `cvd_letality` na query

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- acessing function written in python into sql engine
# MAGIC -- reading from disk [parquet file]
# MAGIC SELECT cvd_letality(death_rate), COUNT(*) as qtd
# MAGIC FROM bronze_covid
# MAGIC GROUP BY cvd_letality(death_rate);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 13: montando estrutura silver com a nova função Python x SQL

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # applying transformations
# MAGIC # saving the transforms into another dataframe
# MAGIC silver_covid = spark.sql("""
# MAGIC   SELECT DISTINCT 
# MAGIC   state, 
# MAGIC   city,
# MAGIC   confirmed,
# MAGIC   deaths,
# MAGIC   date,
# MAGIC   is_last,
# MAGIC   cvd_letality(death_rate) AS letality
# MAGIC   FROM bronze_covid
# MAGIC """)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 14: visualizando dados da tabela silver.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # read dataframe
# MAGIC # note the new transform [importance]
# MAGIC display(silver_covid)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 15: Criando tabela silver Delta Lake

# COMMAND ----------

# delta offers the following write modes
# append
# overwrite
silver_covid.write.format("delta").mode("overwrite").save("dbfs:/mnt/bs-production/delta/silver_covid/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 16: Ler tabela silver do Data Lakehouse

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # reading table from data lakehouse
# MAGIC # data lake - azure blob storage
# MAGIC # delta lake - data format
# MAGIC # delta lakehouse - design pattern for etl 
# MAGIC silver_covid = spark.read.format("delta").load("dbfs:/mnt/bs-production/delta/silver_covid/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 17: Contabilizando registros na tabela silver

# COMMAND ----------

#146690
silver_covid.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 18: Listar tabelas do Database

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE covidbrasil;
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 19: Criar Tabela Gold removendo registro de sumarizando (city = null)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC -- remove if exists
# MAGIC DROP TABLE IF EXISTS gold_covid;
# MAGIC 
# MAGIC -- create gold dataset
# MAGIC CREATE TABLE gold_covid
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT sc.state, 
# MAGIC        sc.city,
# MAGIC        sc.confirmed,
# MAGIC        sc.deaths,
# MAGIC        sc.date,
# MAGIC        sc.is_last,
# MAGIC        sc.letality
# MAGIC FROM delta.`/mnt/bs-production/delta/silver_covid/` AS sc
# MAGIC where city is not null

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Passo 20: Montando query com os dados tratados (Gold Data)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- 1.01 seconds for xxxxx rows with order by
# MAGIC -- delta table performance without partitioning
# MAGIC SELECT state, date, sum(confirmed) as confirmados, sum(deaths) as mortes
# MAGIC FROM gold_covid
# MAGIC GROUP BY state, date
# MAGIC ORDER BY state, date asc