-- Databricks notebook source
CREATE LIVE TABLE inventory_current_transformed
COMMENT 'Changing some formats for making visualisation easy on Power BI' 
TBLPROPERTIES (
  'quality'='gold'
  )
  AS
  SELECT
    --date_format(a.date_time, 'yyyy-MM-dd HH:mm:ss') as date_time,
    to_date(a.date_time) date,
    CASE a.store_id WHEN 1 THEN 'Store_01'
                    WHEN 0 THEN 'Online_01'
    ELSE 'Something_new'
    END store_id,
    a.item_id,
    b.change_type,
    a.current_inventory,
    a.change_quantity,
    a.snapshot_quantity,
    a.date_time
  FROM LIVE.inventory_current a
  LEFT JOIN (
    SELECT 
    y.change_type, x.item_id
    FROM LIVE.inventory_change x
    LEFT JOIN LIVE.inventory_change_type y
    ON x.change_type_id = y.change_type_id
  ) b
  ON a.item_id = b.item_id
