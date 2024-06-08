import streamlit as st
from datetime import *

# PySpark modules =>
import pyspark
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import lit, desc, col, size, array_contains\
, isnan, udf, hour, array_min, array_max, countDistinct

def create_page_PUR(df):
    # We create 4 helper function for 'timestamp' column as described above then convert them to UDF =>

    # Convert a timestamp column into Datetime.Datetime, to be used for .withColumn function later =>
    def to_time(timestamp_list):
        return [datetime.fromtimestamp(t) - timedelta(hours=7) for t in timestamp_list]
    
    # Register 'to_time' function into UDF pyspark framework =>
    udf_to_time = udf(to_time, ArrayType(elementType=TimestampType()))

    # Support function to get the duration (in minutes) of a list of datetime values, to be used for withColumn function later =>
    def get_duration(datetime_list):
        time_dif = max(datetime_list) - min(datetime_list)
        return time_dif.seconds/60
    
    # Register the support function 'get_duration' as a user defined function into pyspark framework
    udf_get_duration = udf(get_duration, FloatType())

    # Support function to get the workout start time of the datetime list, to be used for withColumn function later
    def get_start_time(datetime_list):
        return min(datetime_list)
    
    # Register the support function 'get_start_time' as a user defined function into pyspark framework
    udf_get_start_time = udf(get_start_time, TimestampType())

    # Support function to get list of intervals within a workout =>
    def get_interval(datetime_list):
        if len(datetime_list) == 1:
            return [0]
        
        else:
            interval_list = []

            for i in range(0, len(datetime_list)-1):
                interval = (datetime_list[i+1] - datetime_list[i]).seconds
                interval_list.append(interval)

            return interval_list
        
    # Register the support function 'get_interval' as a user defined function into pyspark framework =>
    udf_get_interval = udf(get_interval, ArrayType(elementType=IntegerType()))

    # Create new 'date_time' column to convert from timestamp into python's datetime format for later usage =>
    df = df.withColumn('date_time', 
        udf_to_time('timestamp'))
    
    # Create 'workout_start_time' column to get the start time of each workout/row:
    df = df.withColumn('workout_start_time', hour(udf_get_start_time('date_time')))

    # Create duration column from the date_time column just created, using the udf function udf_get_duration defined above =>
    df = df.withColumn('duration', udf_get_duration('date_time'))

    # Create interval column from the date_time column, using the udf function udf_get_interval defined above =>
    df = df.withColumn('interval', udf_get_interval('date_time'))

    st.subheader(":green[Statistics of the new duration column in minutes : ]")
    st.table(df.select('duration').toPandas().describe().T)