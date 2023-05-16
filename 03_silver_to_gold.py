# Databricks notebook source
# MAGIC %md
# MAGIC The purpose of this notebook is to calculate the current-state inventory of products in various store locations leveraging data arriving in near real-time from point-of-sale systems. Those data are exposed as Delta Live Table (DLT) objects in the previous notebook.
# MAGIC
# MAGIC This notebook should be scheduled to run while the *01_data_generation* notebook (which generates the simulated event data) runs on a separate cluster. It also depends on the demo environment having been configured per the instructions in the *00_environment_setup* notebook

# COMMAND ----------

# MAGIC %md
# MAGIC # Calculating Current Inventory
# MAGIC In the last notebook, we defined Delta Live Table (DLT) objects to capture inventory change event data as well as periodic snapshot data flowing from stores into our lakehouse environment. Leveraging these data along with various reference data elements, we might issue a query as follows to calculate the current state of product inventory:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT 
# MAGIC --   a.store_id,
# MAGIC --   a.item_id,
# MAGIC --   FIRST(a.quantity) as snapshot_quantity,  
# MAGIC --   COALESCE(SUM(b.quantity),0) as change_quantity,
# MAGIC --   FIRST(a.quantity) + COALESCE(SUM(b.quantity),0) as current_inventory,
# MAGIC --   GREATEST(FIRST(a.date_time), MAX(b.date_time)) as date_time
# MAGIC -- FROM pos.latest_inventory_snapshot a  -- access latest snapshot
# MAGIC -- LEFT OUTER JOIN ( -- calculate inventory change with bopis corrections
# MAGIC --   SELECT
# MAGIC --     x.store_id,
# MAGIC --     x.item_id,
# MAGIC --     x.date_time,
# MAGIC --     x.quantity
# MAGIC --   FROM pos.inventory_change x
# MAGIC --   INNER JOIN pos.store y
# MAGIC --     ON x.store_id=y.store_id
# MAGIC --   INNER JOIN pos.inventory_change_type z
# MAGIC --     ON x.change_type_id=z.change_type_id
# MAGIC --   WHERE NOT(y.name='online' AND z.change_type='bopis') -- exclude bopis records from online store
# MAGIC --   ) b
# MAGIC --   ON 
# MAGIC --     a.store_id=b.store_id AND 
# MAGIC --     a.item_id=b.item_id AND 
# MAGIC --     a.date_time<=b.date_time
# MAGIC -- GROUP BY
# MAGIC --   a.store_id,
# MAGIC --   a.item_id
# MAGIC -- ORDER BY 
# MAGIC --   date_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC The query is moderately complex as it joins inventory change data to the latest inventory counts taken for a given product at a store location to calculate a current state. The joining of these two datasets is facilitated by a match on store_id and item_id where inventory change data flowing from the POS occurs on or after an inventory count for that product was performed. Because it is possible no POS events have been recorded since an item was counted, this join is implemented as an outer join.
# MAGIC
# MAGIC For an experienced Data Engineer reasonably comfortable with SQL, this is how a solution for current-state inventory calculations is intuitively defined. And this maps nicely to how we define the solution with Delta Live Tables (DLT):

# COMMAND ----------

# DBTITLE 1,Define Current Inventory Table (SQL)
# MAGIC %sql
# MAGIC SET pipelines.trigger.interval = 5 minute;
# MAGIC  
# MAGIC CREATE LIVE TABLE inventory_current 
# MAGIC COMMENT 'calculate current inventory given the latest inventory snapshots and inventory-relevant events' 
# MAGIC TBLPROPERTIES (
# MAGIC   'quality'='gold'
# MAGIC   ) 
# MAGIC AS
# MAGIC   SELECT  -- calculate current inventory
# MAGIC     a.store_id,
# MAGIC     a.item_id,
# MAGIC     FIRST(a.quantity) as snapshot_quantity,
# MAGIC     COALESCE(SUM(b.quantity), 0) as change_quantity,
# MAGIC     FIRST(a.quantity) + COALESCE(SUM(b.quantity), 0) as current_inventory,
# MAGIC     GREATEST(FIRST(a.date_time), MAX(b.date_time)) as date_time
# MAGIC   FROM LIVE.latest_inventory_snapshot a -- access latest snapshot
# MAGIC   LEFT OUTER JOIN ( -- calculate inventory change with bopis corrections
# MAGIC     SELECT
# MAGIC       x.store_id,
# MAGIC       x.item_id,
# MAGIC       x.date_time,
# MAGIC       x.quantity
# MAGIC     FROM LIVE.inventory_change x
# MAGIC       INNER JOIN LIVE.store y ON x.store_id = y.store_id
# MAGIC       INNER JOIN LIVE.inventory_change_type z ON x.change_type_id = z.change_type_id
# MAGIC     WHERE NOT( y.name = 'online' AND z.change_type = 'bopis') -- exclude bopis records from online store
# MAGIC     ) b 
# MAGIC     ON  
# MAGIC       a.store_id = b.store_id AND
# MAGIC       a.item_id = b.item_id AND
# MAGIC       a.date_time <= b.date_time
# MAGIC   GROUP BY
# MAGIC     a.store_id,
# MAGIC     a.item_id
# MAGIC   ORDER BY 
# MAGIC     date_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC same things as above in python

# COMMAND ----------

# @dlt.table(
#   name='inventory_current_python',
#   comment='current inventory count for a product in a store location',
#   table_properties={'quality':'gold'},
#   spark_conf={'pipelines.trigger.interval': '5 minutes'}
#   )
#  def inventory_current_python():
  
#    # calculate inventory change with bopis corrections
#    inventory_change_df = (
#       dlt
#        .read('inventory_change').alias('x')
#        .join(
#          dlt.read('store').alias('y'), 
#          on='store_id'
#          )
#        .join(
#          dlt.read('inventory_change_type').alias('z'), 
#          on='change_type_id'
#          )
#        .filter(f.expr("NOT(y.name='online' AND z.change_type='bopis')"))
#        .select('store_id','item_id','date_time','quantity')
#        )
   
#    # calculate current inventory
#    inventory_current_df = (
#       dlt
#          .read('latest_inventory_snapshot').alias('a')
#          .join(
#            inventory_change_df.alias('b'), 
#            on=f.expr('''
#              a.store_id=b.store_id AND 
#              a.item_id=b.item_id AND 
#              a.date_time<=b.date_time
#              '''), 
#            how='leftouter'
#            )
#          .groupBy('a.store_id','a.item_id')
#            .agg(
#                first('a.quantity').alias('snapshot_quantity'),
#                sum('b.quantity').alias('change_quantity'),
#                first('a.date_time').alias('snapshot_datetime'),
#                max('b.date_time').alias('change_datetime')
#                )
#          .withColumn('change_quantity', f.coalesce('change_quantity', f.lit(0)))
#          .withColumn('current_quantity', f.expr('snapshot_quantity + change_quantity'))
#          .withColumn('date_time', f.expr('GREATEST(snapshot_datetime, change_datetime)'))
#          .drop('snapshot_datetime','change_datetime')
#          .orderBy('current_quantity')
#          )
   
#    return inventory_current_df

# COMMAND ----------

# MAGIC %md
# MAGIC It's important to note that the current inventory table is implemented using a 5-minute recalculation. While DLT supports near real-time streaming, the business objectives associated with the calculation of near current state inventories do not require up to the second precision. Instead, 5-, 10- and 15-minute latencies are often preferred to give the data some stability and to reduce the computational requirements associated with keeping current. From a business perspective, responses to diminished inventories are often triggered when values fall below a threshold that's well-above the point of full depletion (as lead times for restocking may be measured in hours, days or even weeks). With that in mind, the 5-minute interval used here exceeds the requirements of many retailers.
