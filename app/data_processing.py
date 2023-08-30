from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, when, sum, first, min, from_unixtime, unix_timestamp, \
    to_timestamp, collect_set, concat_ws, lit
from pyspark.sql.window import Window
from typing import List


def old_polling_events_info(d: DataFrame, periods: List[int]) -> DataFrame:
    """
    Goal: For each order dispatched to a device compute :
        ● The total count of all polling events
        ● The count of each type of polling status_code
        ● The count of each type of polling error_code and the count of responses without error
        codes.
        ...across the following periods of time:
        ● Three minutes before the order creation time
        ● Three minutes after the order creation time
        ● One hour before the order creation time

    Solution plan: For every time periods, filter the df to include only requested time periods.
    Then, for each unique order compute the requested output.
        """
    new_df = d
    for s in periods:
        if s < 0:
            _df = d.filter(
                (d["pollingCT_orderCT_difference"] >= s) & (d["pollingCT_orderCT_difference"] <= 0)) \
                .groupBy(col("order_id")) \
                .agg(
                # total count of polling events for every time period
                count("status_code").alias(f"total_poll_events_{s}s"),
                # count of each type of polling status_code for every time period
                countDistinct(col("status_code")).alias(f"count_typeof_status_c_{s}s"),
                # count of each type of polling error code for every time period
                countDistinct(col("error_code")).alias(f"count_typeof_error_c_{s}s"),
                # count of responses without error codes for every time period
                sum(when(col("status_code") == 200, 1).otherwise(0)).alias(f"ok_responses_{s}s")
            )

        else:
            _df = d.filter(
                (d["pollingCT_orderCT_difference"] <= s) & (d["pollingCT_orderCT_difference"] >= 0)) \
                .groupBy(col("order_id")) \
                .agg(
                # total count of polling events for every time period
                count("status_code").alias(f"total_poll_events_{s}s"),
                # count of each type of polling status_code for every time period
                countDistinct(col("status_code")).alias(f"count_typeof_status_c_{s}s"),
                # count of each type of polling error code for every time period
                countDistinct(col("error_code")).alias(f"count_typeof_error_c_{s}s"),
                # count of responses without error codes for every time period
                sum(when(col("status_code") == 200, 1).otherwise(0)).alias(f"ok_responses_{s}s")
            )
        new_df = new_df.join(_df, on="order_id")

    return new_df


def polling_events_info(d: DataFrame, periods: List[int]) -> DataFrame:
    """
    Goal: For each order dispatched to a device compute :
        ● The total count of all polling events
        ● The count of each type of polling status_code
        ● The count of each type of polling error_code and the count of responses without error
        codes.
        ...across the following periods of time:
        ● Three minutes before the order creation time
        ● Three minutes after the order creation time
        ● One hour before the order creation time

    Solution plan: For every time periods, compute and add the necessary columns using window functions and
    filtering the df to include only requested time periods.
        """
    window_spec = Window().partitionBy("order_id")
    new_df = d

    for s in periods:
        if s < 0:
            time_condition = (new_df["pollingCT_orderCT_difference"] >= s) & (
                    new_df["pollingCT_orderCT_difference"] <= 0)
            suffix = f"_{s}s"
        else:
            time_condition = (new_df["pollingCT_orderCT_difference"] <= s) & (
                    new_df["pollingCT_orderCT_difference"] >= 0)
            suffix = f"_{s}s"

        new_df = new_df.withColumn(
            f"total_poll_events{suffix}",
            sum(when(time_condition, 1).otherwise(0)).over(window_spec)
        ).withColumn(
            f"count_typeof_status_c{suffix}",
            concat_ws(", ", collect_set(when(time_condition, col("status_code")).otherwise(lit(""))).over(window_spec))
        ).withColumn(
            f"count_typeof_error_c{suffix}",
            concat_ws(", ", collect_set(when(time_condition, col("error_code")).otherwise(0)).over(window_spec))
        ).withColumn(
            f"ok_responses{suffix}",
            sum(when(time_condition & (col("status_code") == 200), 1).otherwise(0)).over(window_spec)
        )

    result_df = new_df.groupBy("order_id").agg(
        *[first(column).alias(f"{column}") for column in new_df.columns if column != "order_id"]
    )
    return result_df


def closest_polling_events(d: DataFrame) -> DataFrame:
    """Across an unbounded period of time, we would like to know:
        The time of the polling event immediately preceding, and immediately following the order creation time.

        Solution: Get the difference between polling_CT and orderCT and use them to get the min() for: negative
        and positive values. The min negative value will be the closest one preceding the order_CT.
        The min positive value will be the closest one following the order_CT.
        Then add those values to orderCT to compute the datetime.
    """
    df = d.withColumn("p_pollingCT_orderCT_difference", when(
        col("pollingCT_orderCT_difference") > 0, col("pollingCT_orderCT_difference"))
                      ) \
        .withColumn("order_creation_time", unix_timestamp("order_creation_time")) \
        .withColumnRenamed("p_pollingCT_orderCT_difference", "positive_pollingCT_orderCT_difference")

    result_df = df.groupBy("order_id").agg(
        first("order_creation_time").alias("order_creation_time"),
        first("device_id").alias("device_id"),
        min("pollingCT_orderCT_difference").alias("negative_pollingCT_orderCT_difference"),
        min("positive_pollingCT_orderCT_difference").alias("positive_pollingCT_orderCT_difference")
    ) \
        .withColumn("immed_preceding_polling_event_CT",
                    from_unixtime(col("negative_pollingCT_orderCT_difference") + col("order_creation_time"))
                    ) \
        .withColumn("immed_following_polling_event_CT",
                    from_unixtime(col("positive_pollingCT_orderCT_difference") + col("order_creation_time"))
                    ) \
        .withColumn("order_creation_time", to_timestamp("order_creation_time"))
    return result_df


def closest_conn_status(d: DataFrame) -> DataFrame:
    """Across an unbounded period of time, we would like to know:
        The most recent connectivity status (“ONLINE” or “OFFLINE”) before an order, and at what time the order changed
        to this status. This can be across any period of time before the order creation time.
        Not all devices have a connectivity status.

        Solution:
        Find the min negative difference between conn status and order creation time. Access its date time and
        status. Null values should have been removed in the df passed as parameter.
    """
    new_df = d.filter(col("conn_statusCT_orderCT_difference") < 0) \
        .withColumn("order_creation_time", unix_timestamp("order_creation_time"))

    df = new_df.groupBy("order_id").agg(
        first("order_creation_time").alias("order_creation_time"),
        first("device_id").alias("device_id"),
        min("conn_statusCT_orderCT_difference").alias("negative_conn_statusCT_orderCT_difference"),
        first("status").alias("most_recent_conn_status")
    ).withColumn(
        "most_recent_conn_status_time",
        from_unixtime(col("negative_conn_statusCT_orderCT_difference") + col("order_creation_time"))
    ).withColumn("order_creation_time", to_timestamp("order_creation_time"))

    return df
