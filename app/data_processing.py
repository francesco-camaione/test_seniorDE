from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, when, sum
from typing import List


def polling_events_info(d: DataFrame, periods: List[int]) -> DataFrame:
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
