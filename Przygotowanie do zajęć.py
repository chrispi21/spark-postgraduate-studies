# Databricks notebook source
# DBTITLE 1,Ładowanie danych
# MAGIC %md Za pomocą tego notebook'a skopiujemy pliki na DBFS (Databricks File System), a następnie utworzymy tabele, które będą nam niezbędne do wykonania późniejszych zadań.

# COMMAND ----------

# MAGIC %md UWAGA: Kombinacja klawiszy *Shift+Enter* powoduje uruchomienie komówki i przejście do następnej

# COMMAND ----------

# MAGIC %md Poniższe komórka powinna zakończyć się sukcesem

# COMMAND ----------

# DBTITLE 1,Skrypt pobierający dane i tworzący tabele
# MAGIC %python 
# MAGIC import urllib.request
# MAGIC 
# MAGIC def drop_dbfs_dirs():
# MAGIC   for dir in ["dbfs:/FileStore/tables", "dbfs:/FileStore/temp"]:
# MAGIC     dbutils.fs.rm(dir, True)
# MAGIC     
# MAGIC def create_dbfs_temp(dbfs_temp_loc):
# MAGIC   dbutils.fs.mkdirs(dbfs_temp_loc)
# MAGIC 
# MAGIC def download_to_dbfs(source_url, local_temp_loc, dbfs_temp_loc):
# MAGIC   urllib.request.urlretrieve(source_url,local_temp_loc)
# MAGIC   dbutils.fs.mv(f"file:{local_temp_loc}",dbfs_temp_loc)
# MAGIC 
# MAGIC def drop_table_if_exists(table):
# MAGIC   spark.sql(f"drop table if exists {table}")
# MAGIC   
# MAGIC def clean_dest_dir(table):
# MAGIC   dbutils.fs.rm(f"dbfs:/user/hive/warehouse/{table}", True)
# MAGIC   
# MAGIC def create_table(table, dbfs_temp):
# MAGIC   spark.read.parquet(f"{dbfs_temp}/*").write.saveAsTable(table)
# MAGIC   
# MAGIC def validate(table, expected_cnt):
# MAGIC   assert spark.table(table).count() == expected_cnt
# MAGIC   
# MAGIC config = {'uam_categories': 
# MAGIC                     {'url': 'https://drive.google.com/u/1/uc?id=1WLPzOczLl0ATgk2qyRpLUdrG6duDhkL2&export=download',
# MAGIC                      'local_temp': '/tmp/uam_categories.snappy.parquet',
# MAGIC                      'dbfs_temp': 'dbfs:/FileStore/temp/uam_categories',
# MAGIC                      'expected_row_cnt': 174
# MAGIC                     },
# MAGIC           'uam_orders':
# MAGIC                     {'url': 'https://drive.google.com/u/1/uc?id=1zt-YtImHn_48pgNWNQ-xZQSbcoxbrdHe&export=download',
# MAGIC                      'local_temp': '/tmp/uam_orders.snappy.parquet',
# MAGIC                      'dbfs_temp': 'dbfs:/FileStore/temp/uam_orders',
# MAGIC                      'expected_row_cnt': 100
# MAGIC                     },
# MAGIC           'uam_offers':
# MAGIC                     {'url': 'https://drive.google.com/u/1/uc?id=1tMBkjT_RQC4hPl0hCj9reNUjeM4wiIci&export=download',
# MAGIC                      'local_temp': '/tmp/uam_offers.snappy.parquet',
# MAGIC                      'dbfs_temp': 'dbfs:/FileStore/temp/uam_offers',
# MAGIC                      'expected_row_cnt': 188
# MAGIC                     } 
# MAGIC                    }
# MAGIC 
# MAGIC 
# MAGIC drop_dbfs_dirs()
# MAGIC 
# MAGIC for t, param in config.items():
# MAGIC   create_dbfs_temp(param['dbfs_temp'])
# MAGIC   download_to_dbfs(param['url'], param['local_temp'], param['dbfs_temp'])
# MAGIC   drop_table_if_exists(t)
# MAGIC   clean_dest_dir(t)
# MAGIC   create_table(t, param['dbfs_temp'])
# MAGIC   validate(t, param['expected_row_cnt'])
# MAGIC                     

# COMMAND ----------

# DBTITLE 1,Dodatkowa weryfikacja
for table in spark.catalog.listTables():
  print("{tbl} zawiera {cnt} rekordów".format(tbl=table.name, cnt=spark.table(table.name).count()))

# COMMAND ----------

# DBTITLE 1,Powyższa komórka powinna zwrócić następujący wynik:
# MAGIC %md 
# MAGIC uam_categories zawiera 174 rekordów
# MAGIC 
# MAGIC uam_offers zawiera 188 rekordów
# MAGIC 
# MAGIC uam_orders zawiera 100 rekordów
