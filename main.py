import os
import sys
import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib
from mpl_toolkits.mplot3d import Axes3D
import math
from datetime import *
import statistics as stats

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

MAX_MEMORY = '15G'
# Initialize a spark session =>
conf = pyspark.SparkConf().setMaster("local[*]") \
        .set('spark.executor.heartbeatInterval', 10000) \
        .set('spark.network.timeout', 10000) \
        .set("spark.core.connection.ack.wait.timeout", "3600") \
        .set("spark.executor.memory", MAX_MEMORY) \
        .set("spark.driver.memory", MAX_MEMORY) \
        .set("spark.driver.bindAddress", "127.0.0.1")

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Exploratory-Data") \
        .config(conf=conf) \
        .getOrCreate()
    
    return spark

spark = init_spark()
data = './endomondoHR.json'

# Load the main data set into pyspark data frame =>
df = spark.read.json(data, mode="DROPMALFORMED")
print('Data frame type: ' + str(type(df)))

# Overview of Dataset =>

#print('Data overview')
#df.printSchema()

#print('Columns overview')
#print(pd.DataFrame(df.dtypes, columns = ['Column Name','Data type']))

#print('Data frame describe (string and numeric columns only):')
#df.describe().toPandas()

#print(f'There are total {df.count()} row, Let print first 2 data rows:')
#df.limit(2).toPandas()

################################################################################

