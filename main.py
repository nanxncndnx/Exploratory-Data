import os
import sys
from dotenv import load_dotenv
import pandas as pd
from pandas import DataFrame
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
import math
from datetime import *
import statistics as stats

load_dotenv()
Json = os.getenv('Json_File')
import streamlit as st 
from streamlit_option_menu import option_menu
import altair as alt

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

from DetectMissingValues import DMV
from PysparkLazyEvaluation import PLE
from PysparkUdfRegistering import PUR
from UnstackPysparkDataframe import UPD
from RDD import RDD

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
data = Json

# Load the main data set into pyspark data frame =>
df = spark.read.json(data, mode="DROPMALFORMED")
#print('Data frame type: ' + str(type(df)))

# Overview of Dataset =>
#print('Data overview')
#df.printSchema()

#print('Columns overview')
#print(pd.DataFrame(df.dtypes, columns = ['Column Name','Data type']))

#print('Data frame describe (string and numeric columns only):')
#df.describe().toPandas()

#print(f'There are total {df.count()} row, Let print first 2 data rows:')
#df.limit(2).toPandas()

alt.themes.enable("dark")

#as you see this is our DashBoard =>
with st.sidebar:
    selected = option_menu("Features", ['Docs', 'Detect Missing Values', 'Pyspark Lazy Evaluation', 'Unstack Pyspark Dataframe', 'Pyspark UDF Registering', 'RDD'], 
        icons=['file-text', 'bar-chart-fill', 'bar-chart-fill', 'bar-chart-fill', 'bar-chart-fill', 'bar-chart-fill'], default_index=0,
            styles={
    "container": {"padding": "important", "background-color": "#0F161E", "border-radius" : "10px"},
    "icon": {"color": "white", "font-size": "20px"}, 
    "nav-link": {"font-size": "20px", "text-align": "center", "margin":"15px", "--hover-color": "#9BE8DA", "border-radius" : "10px"},
    "nav-link-selected": {"background-color": "#4F776F"},
    }
)

if selected == "Docs":
    st.subheader(":green[Hello] From Docs")

elif selected == "Detect Missing Values":
    DMV.create_page_DMV(df)

elif selected == "Pyspark Lazy Evaluation":
    PLE.create_page_PLE(df)

elif selected == "Unstack Pyspark Dataframe":
    UPD.create_page_UDP(df)

elif selected == "Pyspark UDF Registering":
    PUR.create_page_PUR(df)

elif selected == "RDD":
    RDD.create_page(df) 