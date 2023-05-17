# Databricks notebook source
# MAGIC %md
# MAGIC To run the workflow defined in the notebook 02 and notebook 03 notebooks, we need to schedule them as a DLT pipeline. Using the Jobs UI within the Databricks workspace, we can click on the Delta Live Tables tab and click Create Pipeline.
# MAGIC
# MAGIC In the Create Pipeline dialog, we select notebook 02 and then click Add Notebook Library to select POS 04, indicating these two notebooks should be orchestrated as one workflow.
# MAGIC
# MAGIC Because we are using the apply_changes() functionality in notebook 02, we need to click Add Configuration and set a key and value of pipelines.applyChangesPreviewEnabled and true, respectively. (This requirement will likely change at a future date.)
# MAGIC
# MAGIC Under Target, we specify the name of the database within which DLT objects created in these workflows should reside. If we accept the defaults associated with this demo, that database is pos_dlt.
# MAGIC
# MAGIC Under Storage Location, we specify the storage location where object data and metadata will be placed. Again, our POS 01 notebook expects this to be /mnt/pos/dlt_pipeline.
# MAGIC
# MAGIC Under Pipeline Mode, we specify how the cluster that runs our job will be managed. If we select Triggered, the cluster shuts down with each cycle. As several of our DLT objects are configured to run continuously, we should select Continuous mode. In our DLT object definitions, we leveraged some throttling techniques to ensure our workflows do not become overwhelmed with data. Still, there will be some variability in terms of data moving through our pipelines so we might specify a minimum and maximum number of workers within a reasonable range based on our expectations for the data. Once deployed, we might monitor resource utilization to determine if this range should be adjusted.
# MAGIC
# MAGIC When the job initially runs, it will run in Development mode as indicated at the top of the UI. In Development mode, any errors will cause the job to be stopped so that they may be corrected. By clicking Production, the job is moved into a state where jobs are restarted upon error.
# MAGIC
# MAGIC To stop a job, click the Stop button at the top of the UI. If you do not explicitly stop this job, it will run indefinitely.
