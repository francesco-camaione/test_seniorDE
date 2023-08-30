import os
import sys

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from lib.utils import remove_matching_values
from app.data_processing import polling_events_info, closest_polling_events, closest_conn_status
from pyspark import StorageLevel


# create spark session

spark = SparkSession.builder \
    .config('spark.driver.host', '192.168.1.62')\
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "2g") \
    .getOrCreate()

# drop orders where device_id is null
orders_df = spark.read \
    .csv("./test_dataset/orders.csv", header=True, inferSchema=True) \
    .withColumnRenamed("_c0", "id") \
    .dropna("any", None, "device_id")

polling_df = spark.read \
    .csv("./test_dataset/polling.csv", header=True, inferSchema=True) \
    .withColumnRenamed("_c0", "id") \
    .withColumnRenamed("device_id", "polling_device_id")

conn_status_df = spark.read \
    .csv("./test_dataset/connectivity_status.csv", header=True, inferSchema=True) \
    .withColumnRenamed("_c0", "id") \
    .withColumnRenamed("device_id", "cs_device_id")

# 1 .join data and get order dispatched to device id
# 2. drop the orders that do not have pollingCT
d = orders_df \
    .join(polling_df, on=orders_df["device_id"] == polling_df["polling_device_id"]) \
    .join(conn_status_df, on=orders_df["device_id"] == conn_status_df["cs_device_id"]) \
    .drop("id", "polling_device_id", "cs_device_id") \
    .dropna(how="any", subset="creation_time")

# 3. compute the differences between creation times
df = d.withColumn("pollingCT_orderCT_difference",
                  unix_timestamp("creation_time") - unix_timestamp("order_creation_time")
                  ) \
    .withColumn("conn_statusCT_orderCT_difference",
                unix_timestamp("conn_status_creation_time") - unix_timestamp("order_creation_time")
                )

# rows will be serialized by default
df.persist(storageLevel=StorageLevel.MEMORY_ONLY)

time_periods = [-180, 180, -3600]

# *********FIRST TASK DF***********
d_1 = polling_events_info(df, time_periods)

# # *********SECOND TASK DF***********
d_2 = closest_polling_events(df).withColumnRenamed("order_id", "order_id_2")

# *********THIRD TASK DF***********
d_3 = closest_conn_status(df).withColumnRenamed("order_id", "order_id_3")

df.unpersist()

d_2_columns_to_remove = ["order_creation_time", "device_id", "negative_pollingCT_orderCT_difference",
                         "positive_pollingCT_orderCT_difference"]
selected_d2_columns = remove_matching_values(d_2.columns, d_2_columns_to_remove)

d_3_columns_to_remove = ["order_creation_time", "device_id", "negative_conn_statusCT_orderCT_difference"]
selected_d3_columns = remove_matching_values(d_3.columns, d_3_columns_to_remove)

output_df = d_1 \
    .join(d_2.select(selected_d2_columns), on=d_1["order_id"] == d_2["order_id_2"]) \
    .join(d_3.select(selected_d3_columns), on=d_1["order_id"] == d_3["order_id_3"]) \
    .drop("order_id_2", "order_id_3")

output_df.show(10)
# output_df.explain()

# output_path = "../test_dataset/output_dataset"
# output_df.write.option("header", "true").csv(output_path, mode="overwrite")

# HOW TO PRODUCE FINAL SINGLE FORMATTED CSV DATASET
# final single formatted csv can be produced using the following linux commands in the shell:
# 1. move to the output_dataset folder
# 2. use the cat command to copy the content of partitions files into the target file:
# cat ./*.csv >> single_output_dataset.csv
