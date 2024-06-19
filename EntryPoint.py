import sys
from library import Transformations, DataReader, UtilityFunctions
from pyspark.sql.functions import *


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('Please specify the environment')
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating spark session")
          
    spark = UtilityFunctions.get_spark_session(job_run_env)

    print("Created spark session")

    orders_df = DataReader.read_orders(spark, job_run_env)

    orders_df.show(5)

    customers_df = DataReader.read_customers(spark, job_run_env)

    customers_df.show(5)

    joined_df = Transformations.join_order_customer(orders_df, customers_df)

    joined_df.show(5)

    count_df = Transformations.count_state(joined_df)

    count_df.show()
    
    print("Application Ended")