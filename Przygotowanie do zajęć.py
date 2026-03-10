# Databricks notebook source
# DBTITLE 1,Ładowanie danych
# MAGIC %md Za pomocą tego notebook'a utworzymy tabele, które będą nam niezbędne do wykonania późniejszych zadań.

# COMMAND ----------

# MAGIC %md UWAGA: Kombinacja klawiszy *Shift+Enter* powoduje uruchomienie komówki i przejście do następnej

# COMMAND ----------

# MAGIC %md Poniższe komórka powinna zakończyć się sukcesem

# COMMAND ----------

import shutil
import urllib.request


catalog = "workspace"
schema = "default"
volume = "uam"

config = [
    {   
        'table': 'uam_categories',
        'url': 'https://drive.google.com/u/1/uc?id=1WLPzOczLl0ATgk2qyRpLUdrG6duDhkL2&export=download',
        'expected_row_cnt': 174
    },
    {
        'table': 'uam_orders',
        'url': 'https://drive.google.com/u/1/uc?id=1zt-YtImHn_48pgNWNQ-xZQSbcoxbrdHe&export=download',
        'expected_row_cnt': 100
    },
    {
        'table': 'uam_offers',
        'url': 'https://drive.google.com/u/1/uc?id=1tMBkjT_RQC4hPl0hCj9reNUjeM4wiIci&export=download',
        'expected_row_cnt': 188
    },
]


def recreate_volume():
    spark.sql(f"DROP VOLUME IF EXISTS {catalog}.{schema}.{volume}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")


def download(source_url, local_temp_loc):
    urllib.request.urlretrieve(source_url,local_temp_loc)


def copy_to_volume(local_temp_loc, volume_loc):
    shutil.copy(local_temp_loc, volume_loc)


def drop_table_if_exists(table):
    spark.sql(f"drop table if exists {catalog}.{schema}.{table}")


def create_table(table, volume_loc):
    spark.read.parquet(volume_loc).write.saveAsTable(table)


def validate(table, expected_cnt):
    assert spark.table(table).count() == expected_cnt


def load_data():
    recreate_volume()
    for cfg in config:
        table = cfg['table']
        expected_row_cnt = cfg['expected_row_cnt']
        url = cfg['url']
        local_temp = f'/tmp/{table}.snappy.parquet'
        volume_temp = f'/Volumes/{catalog}/{schema}/{volume}/{table}.snappy.parquet'

        download(url, local_temp)
        copy_to_volume(local_temp, volume_temp)
        drop_table_if_exists(table)
        create_table(table, volume_temp)
        validate(table, expected_row_cnt)


load_data()
