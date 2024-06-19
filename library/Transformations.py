from pyspark.sql.functions import *


def join_order_customer(orders_df, customers_df):
    return orders_df.join(customers_df, 'customer_id')

def count_state(joined_df):
    return joined_df.where(joined_df.order_status == 'CLOSED') \
                    .groupBy('state') \
                    .agg(count('order_status').alias('closed_statuses')) \
                    .sort(desc('closed_statuses'))