# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Demo 3 - Databricks x Azure SQL Database

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![azure_sql](https://powerbicdn.azureedge.net/cvt-761fdc929349cb3aac5b2129c9a2274e0cd3570a64e3e3bc3eeda56a452871f2/pictures/shared/integrations/2x/azure-sql-database@2x.png)
# MAGIC 
# MAGIC O Azure SQL é uma família de produtos de banco de dados gerenciados, seguros e inteligentes do SQL Server.
# MAGIC 
# MAGIC * Banco de Dados SQL do Azure: ofereça suporte a aplicativos em nuvem modernos em um serviço de banco de dados gerenciado e inteligente, que inclui computação sem servidor.
# MAGIC * Instância gerenciada do SQL do Azure: modernize seus aplicativos existentes do SQL Server em escala com uma instância inteligente como serviço totalmente gerenciada, com quase 100% de paridade de recursos com o mecanismo de banco de dados do SQL Server. Melhor para a maioria das migrações para a nuvem.
# MAGIC * SQL Server nas VMs do Azure: levante e mova suas cargas de trabalho do SQL Server com facilidade e mantenha 100% de compatibilidade com o SQL Server e acesso no nível do sistema operacional.
# MAGIC *  SQL do Azure baseia-se no mecanismo familiar do SQL Server, para que você possa migrar aplicativos com facilidade e continuar usando as ferramentas, idiomas e recursos com os quais está familiarizado. Suas habilidades e experiência são transferidas para a nuvem, para que você possa fazer ainda mais com o que já possui.
# MAGIC 
# MAGIC <https://azure.microsoft.com/pt-br/services/sql-database/#features>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > *download conector [maven] = https://search.maven.org/search?q=a:azure-sqldb-spark [azure-sqldb-spark-1.0.2.jar]*

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 1: Usar o database e listar tabelas

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE covidbrasil;
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 2: Criar Dataframe e visualizar dados da Tabela Gold

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // reading delta table into a dataframe
# MAGIC val df_gold_covid = spark.table("gold_covid")
# MAGIC 
# MAGIC // display data
# MAGIC display(df_gold_covid)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 3: Query para rodar no Azure SQL DB

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- drop table if exists
# MAGIC DROP TABLE gold_covid
# MAGIC 
# MAGIC -- create table for load
# MAGIC -- azure sql database
# MAGIC CREATE TABLE gold_covid
# MAGIC (
# MAGIC   uf CHAR(2),
# MAGIC   city VARCHAR(200),
# MAGIC   confirmed BIGINT,
# MAGIC   deaths BIGINT,
# MAGIC   date DATETIME,
# MAGIC   is_last VARCHAR(10),
# MAGIC   letality VARCHAR(20)
# MAGIC   
# MAGIC );
# MAGIC 
# MAGIC -- create columstore index for olap queries
# MAGIC CREATE CLUSTERED COLUMNSTORE INDEX cci_gold_covid ON gold_covid
# MAGIC 
# MAGIC -- query data
# MAGIC SELECT * FROM gold_covid  
# MAGIC SELECT COUNT(*) FROM gold_covid

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 4: Definir Classe JDBC

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // validates sql server driver installation
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 5: Definir Conexão com o AzureSQL DB

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // specify hostname and database name
# MAGIC val jdbcHostname = dbutils.secrets.get(scope="sqldb-az-databricks", key="host-az-databricks") // "sntdbserver.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "sntdatabase"
# MAGIC 
# MAGIC // building url to push data
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC 
# MAGIC // init properties
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC // user and password
# MAGIC // best practices to use a key vault
# MAGIC connectionProperties.put("user", dbutils.secrets.get(scope="sqldb-az-databricks", key="username-az-databricks"))
# MAGIC connectionProperties.put("password", dbutils.secrets.get(scope="sqldb-az-databricks", key="password-az-databricks"))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 6: Iniciando processo de conexão com o Azure SQL DB

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // try and init db connectivity
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 7: Procedimento de Carga do Delta Lake para o Azure SQL DB via Bulk Copy

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // import libraries
# MAGIC // bulk copy operation 
# MAGIC // 136.000 ~ 0.20 seconds
# MAGIC 
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC val url = dbutils.secrets.get(scope="sqldb-az-databricks", key="host-az-databricks")
# MAGIC val user = dbutils.secrets.get(scope="sqldb-az-databricks", key="username-az-databricks")
# MAGIC val password = dbutils.secrets.get(scope="sqldb-az-databricks", key="password-az-databricks")
# MAGIC // config values
# MAGIC // set up batch size and table lock
# MAGIC val config = Config(Map(
# MAGIC   "url"               -> url,
# MAGIC   "databaseName"      -> "sntdatabase",
# MAGIC   "user"              -> user,
# MAGIC   "password"          -> password, 
# MAGIC   "dbTable"           -> "gold_covid", 
# MAGIC   "bulkCopyBatchSize" -> "2500",
# MAGIC   "bulkCopyTableLock" -> "true",
# MAGIC   "bulkCopyTimeout"   -> "800"
# MAGIC ))
# MAGIC 
# MAGIC // init proccess
# MAGIC df_gold_covid.bulkCopyToSqlDB(config)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Passo 8: Visualizar dados no Azure SQL DB

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // read table from azure sql database
# MAGIC val read_df_gold_covid = spark.read.jdbc(jdbcUrl, "gold_covid", connectionProperties)
# MAGIC 
# MAGIC display(read_df_gold_covid)