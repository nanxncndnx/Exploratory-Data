import sys
import pandas as pd
import numpy as np
import streamlit as st

# setting path
#sys.path.append('../')
#from main import df

# PySpark modules =>
from pyspark.sql import functions
from pyspark.sql.functions import lit, desc, col, size, array_contains\
, isnan, udf, hour, array_min, array_max, countDistinct

# Detect missing values and abnormal zeroes =>
# For string columns, we check for None and null
# For numeric columns, we check for zeroes and NaN
# For array type columns, we check if the array contain zeroes or NaN

def create_page(df):
    st.subheader(':green[Columns overview]')
    columns_overview = pd.DataFrame(df.dtypes, columns = ['Column Name','Data type'])
    st.table(columns_overview)

    string_columns = ['gender', 'sport', 'url']
    numeric_columns = ['id','userId']
    array_columns = ['altitude', 'heart_rate', 'latitude', 'longitude', 'speed', 'timestamp']

    missing_values = {}

    for index, column in enumerate(df.columns):
        if column in string_columns:    # check string columns with None and Null values
            missing_count = df.filter(col(column).eqNullSafe(None) | col(column).isNull()).count()
            missing_values.update({column: missing_count})
            missing_count = df.filter(col(column).eqNullSafe(None) | col(column).isNull()).count()
            missing_values.update({column:missing_count})

        if column in numeric_columns:  # check zeroes, None, NaN
            missing_count = df.where(col(column).isin([0,None,np.nan])).count()
            missing_values.update({column:missing_count})

        if column in array_columns:  # check zeros and NaN
            missing_count = df.filter(array_contains(df[column], 0) | array_contains(df[column], np.nan)).count()
            missing_values.update({column:missing_count})

    missing_df = pd.DataFrame.from_dict([missing_values])
    st.subheader(':green[Missing Values]')
    st.table(missing_df)
    st.write()

    # We create new column to count the number of timestamps recorded per row/workout, named as 'PerWorkoutRecordCount' column =>
    df = df.withColumn('PerWorkoutRecordCount', size(col('timestamp')))
    def user_activity_workout_summarize(df):
        #lets counting users id and their activity => 
        user_count = format(df.select('userId').distinct().count(), ',d')
        workout_count = format(df.select('id').distinct().count(), ',d')
        activity_count = str(df.select('sport').distinct().count())

        # calculating total of the workout record
        sum_temp = df.agg(functions.sum('PerWorkoutRecordCount')).toPandas()
        total_records_count = format(sum_temp['sum(PerWorkoutRecordCount)'][0],',d')

        columns=['Users count', 'Activity types count','Workouts count', 'Total records count']
        data = [[user_count], [activity_count], [workout_count], [total_records_count]]

        # create data frame and dict
        sum_dict = {column: data[i] for i, column in enumerate(columns)}
        sum_df = pd.DataFrame.from_dict(sum_dict)[columns]

        # Gender segregation and counting their activity
        gender_user_count = df.select('gender','userId').distinct().groupBy('gender').count().toPandas()
        gender_activities_count = df.groupBy('gender').count().toPandas()
        gender_user_activity_count = gender_user_count.join(
            gender_activities_count.set_index('gender'), on='gender'
            , how='inner', lsuffix='_gu'
        )
        gender_user_activity_count.columns = ['Gender', '# of users', 'Activities (workouts) count']
    
        return sum_df, gender_user_activity_count
    
    sum_dfs= user_activity_workout_summarize(df) 
    st.write('Overall Data Set Summary On Users, :green[WORKOUTS] and :green[NUMBER] of :green[RECORDS] : ')
    st.table(sum_dfs[0])
    st.table(sum_dfs[1])