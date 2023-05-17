# Databricks notebook source
# MAGIC %md ## Overview
# MAGIC
# MAGIC The Point-of-Sale Streaming with Delta Live Tables (DLT) solution accelerator demonstrates how Delta Live Tables may be used to construct a near real-time lakehouse architecture calculating current inventories for various products across multiple store locations. Instead for moving directly from raw ingested data to the calculated inventory, the solution separates the logic into two stages.</p>
# MAGIC
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/pos_dlt_pipeline_UPDATED.png' width=800>
# MAGIC
# MAGIC In the first stage (denoted in the figure as *Bronze-to-Silver ETL*), ingested data is transformed for greater accessibility.  Actions taken against the data in this stage, *e.g.* the decomposing of nested arrays, the deduplication of records, *etc.*, are not intended to apply any business-driven interpretations to the data.  The tables written to in this phase represent the *Silver* layer of our lakehouse [medallion architecture](https://databricks.com/glossary/medallion-architecture). 
# MAGIC
# MAGIC
# MAGIC In the second stage (denoted in the figure as *Silver-to-Gold ETL*, the Silver tables are used to derive our business-aligned output, calculated current-state inventory.  These data are written to a table representing the *Gold* layer of our architecture. 
# MAGIC
# MAGIC
# MAGIC Across this two-staged workflow, [Delta Live Tables (DLT)](https://databricks.com/product/delta-live-tables) is used as an orchestration and monitoring utility.

# COMMAND ----------

# DBTITLE 1,Initialize Config Settings
if 'config' not in locals():
    config = {}

# COMMAND ----------

# MAGIC %md
# MAGIC # Setting up Azure Envrionment

# COMMAND ----------

# MAGIC %md
# MAGIC * First we have to setup Azure IOT Hub

# COMMAND ----------

# DBTITLE 1,Config Settings for Azure IOT Hub (db-iot-hub)
# config['iot_device_connection_string'] = 'HostName=db-iot-hub.azure-devices.net;DeviceId=sim_data;SharedAccessKey=0vtIv9NC1SbvE1fRPCNIdT3wJ5U08IFlOoS/lm5Gtt0=' # replace with your own credential here temporarily or set up a secret scope with your credential
# # config['iot_device_connection_string'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_iot_hub_conn_string") 
 
# config['event_hub_compatible_endpoint'] = 'Endpoint=sb://ihsuprodpnres003dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=0+WQRcCx5rdskmGhCGxlNSMsLoEL9b8NiJyfpFouKcQ=;EntityPath=iothub-ehub-db-iot-hub-24993086-0b83783c15' # replace with your own credential here temporarily or set up a secret scope with your credential
# # config['event_hub_compatible_endpoint'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_iot_hub_endpoint") 
 
# # helper function to convert strings above into dictionaries
# def split_connstring(connstring):
#   conn_dict = {}
#   for kv in connstring.split(';'):
#     k,v = kv.split('=',1)
#     conn_dict[k]=v
#   return conn_dict
  
# # split conn strings
# iothub_conn = split_connstring(config['iot_device_connection_string'])
# eventhub_conn = split_connstring(config['event_hub_compatible_endpoint'])
 
# # configuring kafka endpoint settings
# config['eh_namespace'] = eventhub_conn['Endpoint'].split('.')[0].split('://')[1] 
# config['eh_kafka_topic'] = iothub_conn['HostName'].split('.')[0]
# config['eh_listen_key_name'] = 'ehListen{0}AccessKey'.format(config['eh_namespace'])
# config['eh_bootstrap_servers'] = '{0}.servicebus.windows.net:9093'.format(config['eh_namespace'])
# config['eh_sasl'] = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint={0};SharedAccessKeyName={1};SharedAccessKey={2}\";'.format(eventhub_conn['Endpoint'], eventhub_conn['SharedAccessKeyName'], eventhub_conn['SharedAccessKey'])

# COMMAND ----------

# DBTITLE 1,Config Settings for Azure IOT Hub (POS-IoTHub)
config['iot_device_connection_string'] = 'HostName=POS-IoTHub.azure-devices.net;DeviceId=pos-device;SharedAccessKey=eSi94QwYvu9NkgpNRnvBHEa+IO+DKIoInfcSZ2haDl4=' # replace with your own credential here temporarily or set up a secret scope with your credential
# config['iot_device_connection_string'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_iot_hub_conn_string") 
 
config['event_hub_compatible_endpoint'] = 'Endpoint=sb://iothub-ns-pos-iothub-25027004-a21f1e7938.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=S9KUUnVTSDPEJS8dwHM5GWrMt/hy2CBnhWOzhm1QXvM=;EntityPath=pos-iothub' # replace with your own credential here temporarily or set up a secret scope with your credential
# config['event_hub_compatible_endpoint'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_iot_hub_endpoint") 
 
# helper function to convert strings above into dictionaries
def split_connstring(connstring):
  conn_dict = {}
  for kv in connstring.split(';'):
    k,v = kv.split('=',1)
    conn_dict[k]=v
  return conn_dict
  
# split conn strings
iothub_conn = split_connstring(config['iot_device_connection_string'])
eventhub_conn = split_connstring(config['event_hub_compatible_endpoint'])
 
# configuring kafka endpoint settings
config['eh_namespace'] = eventhub_conn['Endpoint'].split('.')[0].split('://')[1] 
config['eh_kafka_topic'] = iothub_conn['HostName'].split('.')[0]
config['eh_listen_key_name'] = 'ehListen{0}AccessKey'.format(config['eh_namespace'])
config['eh_bootstrap_servers'] = '{0}.servicebus.windows.net:9093'.format(config['eh_namespace'])
config['eh_sasl'] = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint={0};SharedAccessKeyName={1};SharedAccessKey={2}\";'.format(eventhub_conn['Endpoint'], eventhub_conn['SharedAccessKeyName'], eventhub_conn['SharedAccessKey'])

# COMMAND ----------

# MAGIC %md
# MAGIC * Configuring Azure Blob storage

# COMMAND ----------

# DBTITLE 1,Config Settings for Azure Storage Account
config['storage_account_name'] = 'dbstr4pos' # replace with your own credential here temporarily or set up a secret scope with your credential
# config['storage_account_name'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_storage_account_name") 
 
config['storage_container_name'] = 'pos'
 
config['storage_account_access_key'] = 'DAL49gMkMur+B9cVcmTfGzAL/HtOPOOC3pOhEGwkJc9NS1wxYtIFnPEaDxNEd3kntYdqyR/U/fZP+AStCa3sfg==' # replace with your own credential here temporarily or set up a secret scope with your credential
# config['storage_account_access_key'] = dbutils.secrets.get("solution-accelerator-cicd","rcg_pos_storage_account_key") 
 
config['storage_connection_string'] = 'DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};EndpointSuffix=core.windows.net'.format(config['storage_account_name'], config['storage_account_access_key'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting the Azure Storage to Databricks

# COMMAND ----------

# DBTITLE 1,Config Settings for DPFS Mount Point
config['dbfs_mount_name'] = f'/mnt/pos' 

# COMMAND ----------

# DBTITLE 1,Create DBFS Mount Point
conf_key_name = "fs.azure.account.key.{0}.blob.core.windows.net".format(config['storage_account_name'])
conf_key_value = config['storage_account_access_key']
 
# determine if not already mounted
for m in dbutils.fs.mounts():
  mount_exists = (m.mountPoint==config['dbfs_mount_name'])
  if mount_exists: break
 
# create mount if not exists
if not mount_exists:
  
  print('creating mount point {0}'.format(config['dbfs_mount_name']))
  
  # create mount
  dbutils.fs.mount(
    source = "wasbs://{0}@{1}.blob.core.windows.net".format(
      config['storage_container_name'], 
      config['storage_account_name']
      ),
    mount_point = config['dbfs_mount_name'],
    extra_configs = {conf_key_name:conf_key_value}
    )

# COMMAND ----------

# DBTITLE 1,Config Settings for Data Files
# change event data files
config['inventory_change_store001_filename'] = config['dbfs_mount_name'] + '/generator/inventory_change_store001.txt'
config['inventory_change_online_filename'] = config['dbfs_mount_name'] + '/generator/inventory_change_online.txt'
 
# snapshot data files
config['inventory_snapshot_store001_filename'] = config['dbfs_mount_name'] + '/generator/inventory_snapshot_store001.txt'
config['inventory_snapshot_online_filename'] = config['dbfs_mount_name'] + '/generator/inventory_snapshot_online.txt'
 
# static data files
config['stores_filename'] = config['dbfs_mount_name'] + '/static_data/store.txt'
config['items_filename'] = config['dbfs_mount_name'] + '/static_data/item.txt'
config['change_types_filename'] = config['dbfs_mount_name'] + '/static_data/inventory_change_type.txt'

# COMMAND ----------

# MAGIC %md
# MAGIC In this last step, we will provide the paths to a few items our accelerator will need to access. First amongst these is the location of the inventory snapshot files our simulated stores will deposit into our streaming infrastructure. This path should be dedicated to this one purpose and not shared with other file types:

# COMMAND ----------

# DBTITLE 1,Config Settings for Checkpoint Files
config['inventory_snapshot_path'] = config['dbfs_mount_name'] + '/inventory_snapshots/'

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we will configure the default storage location for our DLT objects and metadata:

# COMMAND ----------

# DBTITLE 1,Config Settings for DLT Data
config['dlt_pipeline'] = config['dbfs_mount_name'] + '/dlt_pipeline'

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, we will set the name of the database within which persisted data objects will be housed:

# COMMAND ----------

# DBTITLE 1,Identify Database for Data Objects and initialize it
database_name = f'pos_dlt'
config['database'] = database_name

# COMMAND ----------

config
