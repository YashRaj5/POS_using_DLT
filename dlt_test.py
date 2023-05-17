# Databricks notebook source
# DBTITLE 1,Importing Libraries
import pyspark.sql.functions as f
from pyspark.sql.types import *
import time

# COMMAND ----------

config = {'iot_device_connection_string': 'HostName=db-iot-hub.azure-devices.net;DeviceId=sim_data;SharedAccessKey=0vtIv9NC1SbvE1fRPCNIdT3wJ5U08IFlOoS/lm5Gtt0=',
 'event_hub_compatible_endpoint': 'Endpoint=sb://ihsuprodpnres003dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=0+WQRcCx5rdskmGhCGxlNSMsLoEL9b8NiJyfpFouKcQ=;EntityPath=iothub-ehub-db-iot-hub-24993086-0b83783c15',
 'eh_namespace': 'ihsuprodpnres003dednamespace',
 'eh_kafka_topic': 'db-iot-hub',
 'eh_listen_key_name': 'ehListenihsuprodpnres003dednamespaceAccessKey',
 'eh_bootstrap_servers': 'ihsuprodpnres003dednamespace.servicebus.windows.net:9093',
 'eh_sasl': 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="Endpoint=sb://ihsuprodpnres003dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=0+WQRcCx5rdskmGhCGxlNSMsLoEL9b8NiJyfpFouKcQ=";',
 'storage_account_name': 'dbstr4pos',
 'storage_container_name': 'pos',
 'storage_account_access_key': 'DAL49gMkMur+B9cVcmTfGzAL/HtOPOOC3pOhEGwkJc9NS1wxYtIFnPEaDxNEd3kntYdqyR/U/fZP+AStCa3sfg==',
 'storage_connection_string': 'DefaultEndpointsProtocol=https;AccountName=dbstr4pos;AccountKey=DAL49gMkMur+B9cVcmTfGzAL/HtOPOOC3pOhEGwkJc9NS1wxYtIFnPEaDxNEd3kntYdqyR/U/fZP+AStCa3sfg==;EndpointSuffix=core.windows.net',
 'dbfs_mount_name': '/mnt/pos',
 'inventory_change_store001_filename': '/mnt/pos/generator/inventory_change_store001.txt',
 'inventory_change_online_filename': '/mnt/pos/generator/inventory_change_online.txt',
 'inventory_snapshot_store001_filename': '/mnt/pos/generator/inventory_snapshot_store001.txt',
 'inventory_snapshot_online_filename': '/mnt/pos/generator/inventory_snapshot_online.txt',
 'stores_filename': '/mnt/pos/static_data/store.txt',
 'items_filename': '/mnt/pos/static_data/item.txt',
 'change_types_filename': '/mnt/pos/static_data/inventory_change_type.txt',
 'inventory_snapshot_path': '/mnt/pos/inventory_snapshots/',
 'dlt_pipeline': '/mnt/pos/dlt_pipeline',
 'database': 'pos_dlt'}

# COMMAND ----------

# DBTITLE 1,Stores
store_schema = StructType([
  StructField('store_id', IntegerType()),
  StructField('name', StringType())
  ])

def store():
  df = (
      spark
      .read
      .csv(
        config['stores_filename'], 
        header=True, 
        schema=store_schema
        )
      )
  return df

display( store() )

# COMMAND ----------

# DBTITLE 1,Items
item_schema = StructType([
  StructField('item_id', IntegerType()),
  StructField('name', StringType()),
  StructField('supplier_id', IntegerType()),
  StructField('safety_stock_quantity', IntegerType())
  ])
 
def item():
  return (
    spark
      .read
      .csv(
        config['items_filename'], 
        header=True, 
        schema=item_schema
        )
  )

display(item())

# COMMAND ----------

change_type_schema = StructType([
  StructField('change_type_id', IntegerType()),
  StructField('change_type', StringType())
  ])
 
def inventory_change_type():
  return (
    spark
      .read
      .csv(
        config['change_types_filename'],
        header=True,
        schema=change_type_schema
        )
  )

display(inventory_change_type())

# COMMAND ----------

config['change_types_filename']

# COMMAND ----------

# MAGIC %sql
# MAGIC USE pos_dlt;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inventory_current
# MAGIC ORDER BY date_time DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(date_time, 'yyyy-MM-dd HH:mm:ss') as date from inventory_current;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inventory_change
# MAGIC where trans_id = 'FA2AB8F3-432C-44DD-9644-813AC80C927D';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inventory_change_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inventory_snapshot;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from item;
# MAGIC -- select count(distinct(item_id)) from item;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from latest_inventory_snapshot
# MAGIC where store_id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_inventory_change;
# MAGIC -- desc raw_inventory_change;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from store;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inventory_current_transformed;

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date('2021-01-02T00:00:15.000+0000')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp('2020-04-30 12:25:13.45');
