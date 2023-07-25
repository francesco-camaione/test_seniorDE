from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, first

from app.data_processing import polling_events_info, closest_polling_events, closest_conn_status

# create spark session
spark = SparkSession.builder.getOrCreate()

# drop orders where device_id is null
orders_df = spark.read \
    .csv("./glovo_test_dataset/orders.csv", header=True, inferSchema=True) \
    .withColumnRenamed("_c0", "id") \
    .dropna("any", None, "device_id")

polling_df = spark.read \
    .csv("./glovo_test_dataset/polling.csv", header=True, inferSchema=True) \
    .withColumnRenamed("_c0", "id") \
    .withColumnRenamed("device_id", "polling_device_id")

conn_status_df = spark.read \
    .csv("./glovo_test_dataset/connectivity_status.csv", header=True, inferSchema=True) \
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
                    unix_timestamp("creation_time", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_creation_time")
                    ) \
    .withColumn("conn_statusCT_orderCT_difference",
                unix_timestamp("conn_status_creation_time") - unix_timestamp("order_creation_time")
                )

time_periods = [-180, 180, -3600]

# *********FIRST TASK***********
# d_1 = polling_events_info(df, time_periods)
# d_1.show(2)
#
# d_1.groupBy("order_id").agg(
#     first("device_id").alias("device_id"),
#     # count of polling events
#     first("total_poll_events_-180s").alias("poll_events_3m_beforeOCT"),
#     first("total_poll_events_180s").alias("poll_events_3m_afterOCT"),
#     first("total_poll_events_-3600s").alias("poll_events_1h_beforeOCT"),
#     # count of polling status codes
#     first("count_typeof_status_c_-180s").alias("typesOf_status_codes_3m_beforeOCT"),
#     first("count_typeof_status_c_180s").alias("typesOf_status_codes_3m_afterOCT"),
#     first("count_typeof_status_c_-3600s").alias("typesOf_status_codes_1h_beforeOCT"),
#     # count of types of error codes
#     first("count_typeof_error_c_-180s").alias("typesOf_error_codes_3m_beforeOCT"),
#     first("count_typeof_error_c_180s").alias("typesOf_error_codes_3m_afterOCT"),
#     first("count_typeof_error_c_-3600s").alias("typesOf_error_codes_1h_beforeOCT"),
#     # count of ok responses
#     first("ok_responses_-180s").alias("ok_responses_3m_beforeOCT"),
#     first("ok_responses_180s").alias("ok_responses_3m_afterOCT"),
#     first("ok_responses_-3600s").alias("ok_responses_1h_beforeOCT"),
# ).show(10)
#
# # *********SECOND TASK***********
# d_2 = closest_polling_events(df)
# d_2.show(11)

# *********THIRD TASK***********
d_3 = closest_conn_status(df)
d_3.show(12)
