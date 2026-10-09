# Databricks notebook source
# MAGIC %md
# MAGIC ### Create catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS demo.sdp;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create volume for incoming events

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS demo.sdp.events;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create volume for pipeline metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS demo.sdp.pipeline_metadata;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create directories

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/demo/sdp/pipeline_metadata/bronze/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the volumes

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/demo/sdp/events"))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/demo/sdp/pipeline_metadata/bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Put your JSON source data into the volume

# COMMAND ----------

json_data = """
[
  {
    "event_id": 100001,
    "user_id": 501,
    "event_type": "login",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 09:15:23",
    "source_ip": "192.168.1.101"
  },
  {
    "event_id": 100002,
    "user_id": 502,
    "event_type": "purchase",
    "event_value": 2499.99,
    "event_timestamp": "2026-10-08 09:17:45",
    "source_ip": "10.24.16.52"
  },
  {
    "event_id": 100003,
    "user_id": 503,
    "event_type": "page_view",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 09:18:12",
    "source_ip": "172.16.4.21"
  },
  {
    "event_id": 100004,
    "user_id": 501,
    "event_type": "logout",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 09:32:07",
    "source_ip": "192.168.1.101"
  },
  {
    "event_id": 100005,
    "user_id": 504,
    "event_type": "purchase",
    "event_value": 799.5,
    "event_timestamp": "2026-10-08 09:35:41",
    "source_ip": "10.24.18.73"
  },
  {
    "event_id": 100006,
    "user_id": 505,
    "event_type": "add_to_cart",
    "event_value": 1299.0,
    "event_timestamp": "2026-10-08 09:41:18",
    "source_ip": "172.16.8.44"
  },
  {
    "event_id": 100007,
    "user_id": 502,
    "event_type": "page_view",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 09:45:33",
    "source_ip": "10.24.16.52"
  },
  {
    "event_id": 100008,
    "user_id": 506,
    "event_type": "login",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 09:52:09",
    "source_ip": "192.168.2.115"
  },
  {
    "event_id": 100009,
    "user_id": 503,
    "event_type": "purchase",
    "event_value": 1599.99,
    "event_timestamp": "2026-10-08 10:01:27",
    "source_ip": "172.16.4.21"
  },
  {
    "event_id": 100010,
    "user_id": 507,
    "event_type": "search",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 10:05:54",
    "source_ip": "10.10.20.31"
  },
  {
    "event_id": 100011,
    "user_id": 508,
    "event_type": "purchase",
    "event_value": 4999.0,
    "event_timestamp": "2026-10-08 10:12:16",
    "source_ip": "192.168.3.87"
  },
  {
    "event_id": 100012,
    "user_id": 505,
    "event_type": "remove_from_cart",
    "event_value": 1299.0,
    "event_timestamp": "2026-10-08 10:19:42",
    "source_ip": "172.16.8.44"
  },
  {
    "event_id": 100013,
    "user_id": 509,
    "event_type": "login",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 10:25:03",
    "source_ip": "10.11.12.45"
  },
  {
    "event_id": 100014,
    "user_id": 510,
    "event_type": "page_view",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 10:31:29",
    "source_ip": "192.168.4.91"
  },
  {
    "event_id": 100015,
    "user_id": 504,
    "event_type": "purchase",
    "event_value": 349.75,
    "event_timestamp": "2026-10-08 10:37:58",
    "source_ip": "10.24.18.73"
  },
  {
    "event_id": 100016,
    "user_id": 501,
    "event_type": "purchase",
    "event_value": 899.99,
    "event_timestamp": "2026-10-08 10:44:21",
    "source_ip": "192.168.1.101"
  },
  {
    "event_id": 100017,
    "user_id": 506,
    "event_type": "logout",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 10:49:36",
    "source_ip": "192.168.2.115"
  },
  {
    "event_id": 100018,
    "user_id": 511,
    "event_type": "search",
    "event_value": 1.0,
    "event_timestamp": "2026-10-08 10:53:17",
    "source_ip": "172.20.10.12"
  },
  {
    "event_id": 100019,
    "user_id": 502,
    "event_type": "add_to_cart",
    "event_value": 799.0,
    "event_timestamp": "2026-10-08 11:02:45",
    "source_ip": "10.24.16.52"
  },
  {
    "event_id": 100020,
    "user_id": 512,
    "event_type": "purchase",
    "event_value": 2199.99,
    "event_timestamp": "2026-10-08 11:08:31",
    "source_ip": "192.168.5.63"
  }
]
"""

dbutils.fs.put(
    "/Volumes/demo/sdp/events/events.json",
    json_data,
    overwrite=True
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Verfiy the file

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/demo/sdp/events/"))