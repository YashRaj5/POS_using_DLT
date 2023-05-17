# Databricks notebook source
# MAGIC %md
# MAGIC The purpose of this notebook is to process inventory change event and snapshot data being transmitted into the Azure infrastructure from the various (simulated) point-of-sale systems in this demonstration. As they are received, these data are landed into various Delta tables, enabling persistence and downstream stream processing.
# MAGIC
# MAGIC This notebook should be scheduled to run while the '02_data_generation' notebook (which generates the simulated event data) runs on a separate cluster. It also depends on the demo environment having been configured per the instructions in the '00_environment_setup' notebook.

# COMMAND ----------

# DBTITLE 1,Importing Required Libraries
import pyspark.sql.functions as f
from pyspark.sql.types import *

from delta.tables import *
import dlt # library for using delta live tables

import time

# COMMAND ----------

# MAGIC %md
# MAGIC Notebooks scheduled for the purpose of running DLT workflows must be self-contained, i.e. they cannot reference other notebooks. Given this requirement, we'll set a few configuration settings here. 

# COMMAND ----------

# DBTITLE 1,Initialize Configuration (db-iot-hub)
# config = {'iot_device_connection_string': 'HostName=db-iot-hub.azure-devices.net;DeviceId=sim_data;SharedAccessKey=0vtIv9NC1SbvE1fRPCNIdT3wJ5U08IFlOoS/lm5Gtt0=',
#  'event_hub_compatible_endpoint': 'Endpoint=sb://ihsuprodpnres003dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=0+WQRcCx5rdskmGhCGxlNSMsLoEL9b8NiJyfpFouKcQ=;EntityPath=iothub-ehub-db-iot-hub-24993086-0b83783c15',
#  'eh_namespace': 'ihsuprodpnres003dednamespace',
#  'eh_kafka_topic': 'db-iot-hub',
#  'eh_listen_key_name': 'ehListenihsuprodpnres003dednamespaceAccessKey',
#  'eh_bootstrap_servers': 'ihsuprodpnres003dednamespace.servicebus.windows.net:9093',
#  'eh_sasl': 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="Endpoint=sb://ihsuprodpnres003dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=0+WQRcCx5rdskmGhCGxlNSMsLoEL9b8NiJyfpFouKcQ=";',
#  'storage_account_name': 'dbstr4pos',
#  'storage_container_name': 'pos',
#  'storage_account_access_key': 'DAL49gMkMur+B9cVcmTfGzAL/HtOPOOC3pOhEGwkJc9NS1wxYtIFnPEaDxNEd3kntYdqyR/U/fZP+AStCa3sfg==',
#  'storage_connection_string': 'DefaultEndpointsProtocol=https;AccountName=dbstr4pos;AccountKey=DAL49gMkMur+B9cVcmTfGzAL/HtOPOOC3pOhEGwkJc9NS1wxYtIFnPEaDxNEd3kntYdqyR/U/fZP+AStCa3sfg==;EndpointSuffix=core.windows.net',
#  'dbfs_mount_name': '/mnt/pos',
#  'inventory_change_store001_filename': '/mnt/pos/generator/inventory_change_store001.txt',
#  'inventory_change_online_filename': '/mnt/pos/generator/inventory_change_online.txt',
#  'inventory_snapshot_store001_filename': '/mnt/pos/generator/inventory_snapshot_store001.txt',
#  'inventory_snapshot_online_filename': '/mnt/pos/generator/inventory_snapshot_online.txt',
#  'stores_filename': '/mnt/pos/static_data/store.txt',
#  'items_filename': '/mnt/pos/static_data/item.txt',
#  'change_types_filename': '/mnt/pos/static_data/inventory_change_type.txt',
#  'inventory_snapshot_path': '/mnt/pos/inventory_snapshots/',
#  'dlt_pipeline': '/mnt/pos/dlt_pipeline',
#  'database': 'pos_dlt'}

# COMMAND ----------

# DBTITLE 1,Initialize Configuration (POS-IoTHub)
config = {'iot_device_connection_string': 'HostName=POS-IoTHub.azure-devices.net;DeviceId=pos-device;SharedAccessKey=eSi94QwYvu9NkgpNRnvBHEa+IO+DKIoInfcSZ2haDl4=',
 'event_hub_compatible_endpoint': 'Endpoint=sb://iothub-ns-pos-iothub-25027004-a21f1e7938.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=S9KUUnVTSDPEJS8dwHM5GWrMt/hy2CBnhWOzhm1QXvM=;EntityPath=pos-iothub',
 'eh_namespace': 'iothub-ns-pos-iothub-25027004-a21f1e7938',
 'eh_kafka_topic': 'POS-IoTHub',
 'eh_listen_key_name': 'ehListeniothub-ns-pos-iothub-25027004-a21f1e7938AccessKey',
 'eh_bootstrap_servers': 'iothub-ns-pos-iothub-25027004-a21f1e7938.servicebus.windows.net:9093',
 'eh_sasl': 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="Endpoint=sb://iothub-ns-pos-iothub-25027004-a21f1e7938.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=S9KUUnVTSDPEJS8dwHM5GWrMt/hy2CBnhWOzhm1QXvM=";',
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

# DBTITLE 1,Setting up the POS Database Environment
# MAGIC %md
# MAGIC The typical first step in setting up a streaming architecture is to create a database to house our tables. This needs to be done in advance of running the DLT jobs.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS pos_dlt;

# COMMAND ----------

# DBTITLE 1,Load the static reference data
# MAGIC %md
# MAGIC While we've given attention in this and other notebooks to the fact that we are receiving streaming event data and periodic snapshots, we also have reference data we need to access. These data are relatively stable so that we might update them just once daily.
# MAGIC
# MAGIC To define a Delta Live Tables object, we define a dataframe as we would normally in Spark and have that dataframe returned by a function. A @dlt.table decorator on the function identifies it as defining a DLT object and provides additional metadata such as a description (comment) and any other metadata we might find useful for the management of our DLT workflows.
# MAGIC
# MAGIC
# MAGIC The name element associated with the @dlt.table decorator identifies the name of the table to be created by the DLT engine. Had we not used this optional element, the function name would have been used as the name of the resulting table.
# MAGIC
# MAGIC The spark_conf element associated with the DLT table defines the frequency with which the table will be run. At the passing of each interval, the DTL engine will examine the file pointed to by this dataframe to see if there have been any changes. If there have been, the table will be rewritten using the data processing logic associated with the dataframe definition:

# COMMAND ----------

# DBTITLE 1,Stores
# defining the schema for incoming file
store_schema = StructType([
  StructField('store_id', IntegerType()),
  StructField('name', StringType())
  ])

# defining the dlt table
@dlt.table(
  name='store', # name of the table to create
  comment = 'data associated with individual store locations', # description
  table_properties = {'quality': 'silver'}, # various table properties
  spark_conf = {'pipelines.trigger.interval':'24 hours'} # various spark configurations
  )
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

# COMMAND ----------

# DBTITLE 1,Items
item_schema = StructType([
  StructField('item_id', IntegerType()),
  StructField('name', StringType()),
  StructField('supplier_id', IntegerType()),
  StructField('safety_stock_quantity', IntegerType())
  ])
 
@dlt.table(
  name = 'item',
  comment = 'data associated with individual items',
  table_properties={'quality':'silver'},
  spark_conf={'pipelines.trigger.interval':'24 hours'}
)
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

# COMMAND ----------

# MAGIC %md
# MAGIC And lastly, we can write a DLT table definition for our inventory change type data.

# COMMAND ----------

# DBTITLE 1,Inventory Change Types
change_type_schema = StructType([
  StructField('change_type_id', IntegerType()),
  StructField('change_type', StringType())
  ])
 
@dlt.table(
  name = 'inventory_change_type',
  comment = 'data mapping change type id values to descriptive strings',
  table_properties={'quality':'silver'},
  spark_conf={'pipelines.trigger.interval':'24 hours'}
)
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream Inventory Change Events
# MAGIC Let's now tackle our inventory change event data. These data consist of a JSON document transmitted by a store to summarize an event with inventory relevance. These events may represent sales, restocks, or reported loss, damage or theft (shrinkage). A fourth event type, bopis, indicates a sales transaction that takes place in the online store but which is fulfilled by a physical store. All these events are transmitted as part of a consolidated stream:
# MAGIC <img src = "https://brysmiwasb.blob.core.windows.net/demos/images/pos_event_change_streaming_etl_UPDATED.png">
# MAGIC
# MAGIC Just as before, we write a function to return a Spark dataframe and decorate that function with the appropriate elements. The dataframe is defined using patterns used with Spark Structured Streaming. Because the dataframe is streaming data from the Kafka endpoint of the Azure IOT Hub, we configure the connection using Kafka properties. As a Kafka data source, the structure of the dataframe read from the IOT Hub is pre-defined (so that there's no need to specify a schema). The maxOffsetsPerTrigger configuration setting limits the number of messages read from the IOT Hub within a given cycle so that we don't overwhelm the resources provisioned for stream processing:

# COMMAND ----------

# DBTITLE 1,Reading Event Stream
@dlt.table(
  name = 'raw_inventory_change',
  comment= 'data representing raw (untransformed) inventory-relevant events originating from the POS',
  table_properties={'quality':'bronze'}
  )
def raw_inventory_change():
  return (
    spark
      .readStream
      .format('kafka')
      .option('subscribe', config['eh_kafka_topic'])
      .option('kafka.bootstrap.servers', config['eh_bootstrap_servers'])
      .option('kafka.sasl.mechanism', 'PLAIN')
      .option('kafka.security.protocol', 'SASL_SSL')
      .option('kafka.sasl.jaas.config', config['eh_sasl'])
      .option('kafka.request.timeout.ms', '60000')
      .option('kafka.session.timeout.ms', '60000')
      .option('failOnDataLoss', 'false')
      .option('startingOffsets', 'latest')
      .option('maxOffsetsPerTrigger', '100') # read 100 messages at a time
      .load()
  )

# COMMAND ----------

# MAGIC %md
# MAGIC The schema of the data being read through the Kafka connector is pre-defined as follows:
# MAGIC
# MAGIC
# MAGIC {Column :	Type, key :	binary, value :	binary, topic :	string, partition :	int, offset :	long, timestamp :	timestamp, timestampType :	int, headers : array}
# MAGIC
# MAGIC
# MAGIC The value field represents the payload sent from the simulated POS. To access this data, we need to cast and transform the data leveraging advance knowledge of its structure. In our scenario, this data is delivered as JSON with a well-defined schema. We can convert this data and extract elements from the value field using standard dataframe method calls as one would employ with Structured Streaming. Please note that because the table builds on the raw_inventory_change DLT object defined in the last cell, we use the dlt.read_stream() method to access its data. This instructs the DLT engine to treat the DLT object, i.e. inventory_change, as part of the same streaming pipeline as the referenced object:

# COMMAND ----------

# DBTITLE 1,Convert Transaction to Structure Field & Extract Data Elements
# schema of value field
value_schema = StructType([
  StructField('trans_id', StringType()),
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('change_type_id', IntegerType()),
  StructField('items', ArrayType(
    StructType([
      StructField('item_id', IntegerType()), 
      StructField('quantity', IntegerType())
      ])
    ))
  ])
 
# define inventory change data
@dlt.table(
  name = 'inventory_change',
  comment = 'data representing item-level inventory changes originating from the POS',
  table_properties = {'quality':'silver'}
)
def inventory_change():
  df = (
    dlt
      .read_stream('raw_inventory_change')
      .withColumn('body', f.expr('cast(value as string)')) # convert payload to string
      .withColumn('event', f.from_json('body', value_schema)) # parse json string in payload
      .select( # extract data from payload json
        f.col('event').alias('event'),
        f.col('event.trans_id').alias('trans_id'),
        f.col('event.store_id').alias('store_id'), 
        f.col('event.date_time').alias('date_time'), 
        f.col('event.change_type_id').alias('change_type_id'), 
        f.explode_outer('event.items').alias('item')     # explode items so that there is now one item per record
        )
      .withColumn('item_id', f.col('item.item_id'))
      .withColumn('quantity', f.col('item.quantity'))
      .drop('item')
      .withWatermark('date_time', '1 hour') # ignore any data more than 1 hour old flowing into deduplication
      .dropDuplicates(['trans_id','item_id'])  # drop duplicates 
    )
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream Inventory Snapshots
# MAGIC Periodically, we receive counts of items in inventory at a given store location. Such inventory snapshots are frequently used by retailers to update their understanding of which products are actually on-hand given concerns about the reliability of calculated inventory quantities. We may wish to preserve both a full history of inventory snapshots received and the latest counts for each product in each store location. To meet this need, two separate tables are built from this one data source as it arrives in our environment.
# MAGIC <img src = "https://brysmiwasb.blob.core.windows.net/demos/images/pos_snapshot_auto_loader_etl_UPDATED.png">
# MAGIC These inventory snapshot data arrive in this environment as CSV files on a slightly irregular basis. But as soon as they land, we will want to process them, making them available to support more accurate estimates of current inventory. To enable this, we will take advantage of the Databricks [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) feature which listens for incoming files to a storage path and processes the data for any newly arriving files as a stream. Again, we define a function to return a Spark Structured Streaming dataframe and decorate that function to register it with the DLT engine:

# COMMAND ----------

# DBTITLE 1,Access Incoming Snapshots
inventory_snapshot_schema = StructType([
  StructField('id', IntegerType()),
  StructField('item_id', IntegerType()),
  StructField('employee_id', IntegerType()),
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('quantity', IntegerType())
  ])
 
@dlt.table(
  name='inventory_snapshot',
  comment='data representing periodic counts of items in inventory',
  table_properties={'quality':'silver'}
  )
def inventory_snapshot():
  return (
    spark
      .readStream
      .format('cloudFiles')  # auto loader
      .option('cloudFiles.format', 'csv')
      .option('cloudFiles.includeExistingFiles', 'true') 
      .option('header', 'true')
      .schema(inventory_snapshot_schema)
      .load(config['inventory_snapshot_path'])
      .drop('id')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC The inventory_snapshot table will contain details about every inventory count taken within a given store location. For the purposes of calculating things like current inventory, we only need the latest count of an item in a given location. We can create a DLT table containing this subset of data in a relatively simple manner using the apply_changes() method.
# MAGIC
# MAGIC The apply_changes() method is part of Delta Live Table's change data capture functionality. While we could accomplish this update with a merge, this is such a common pattern the DLT builds in mechanics to handle it in a more succinct manner. With a source data stream and target table identified, keys for matching records are specified. When there's a match, the row is updated based on the latest sequence_by value:

# COMMAND ----------

# DBTITLE 1,Define Table for latest Snapshot Data
# create dlt table to hold latest inventory snapshot (if it doesn't exist)
dlt.create_target_table('latest_inventory_snapshot')
 
# merge incoming snapshot data with latest
dlt.apply_changes( # merge
  target = 'latest_inventory_snapshot',
  source = 'inventory_snapshot',
  keys = ['store_id','item_id'], # match source to target records on these keys
  sequence_by = 'date_time' # determine latest value by comparing date_time field
  )

# COMMAND ----------

# MAGIC %md
# MAGIC If you are familiar with the earlier release of this accelerator which is based on structured streaming, you may remember that to accomplish the work in the last cell, we implemented forEachBatch() logic. In that step, we not only defined a merge operation but also inserted dummy records into the inventory_change table to work around a problem with streaming joins that occurs as we move to construct the current inventory dataset. With our DLT implementation of current inventory (in POS 04), we don't have this same limitation. This allows us to simplify our logic here in the Bronze-to-Silver ETL.
