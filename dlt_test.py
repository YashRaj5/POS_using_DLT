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


