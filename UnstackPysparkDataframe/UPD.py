import sys
import streamlit as st

import pandas as pd
import numpy as np

def create_page_UDP(df):
    activities_by_gender = df.groupBy('sport', 'gender').count().toPandas() 

    activities_by_gender_df = activities_by_gender.pivot_table(
        index="sport", columns="gender", values='count', fill_value=0) \
        .reset_index().rename_axis(None, axis=1)

    activities_by_gender_df['total'] = activities_by_gender_df['male'] \
        + activities_by_gender_df['female'] \
        + activities_by_gender_df['unknown']
    
    activities_by_gender_df['percentage'] = activities_by_gender_df['total'] \
        / sum(activities_by_gender_df['total']) * 100
    
    top_activities_by_gender_df = activities_by_gender_df.sort_values(by='percentage', ascending=False)
    st.subheader(":green[Top Activities By Gender : ]")
    st.table(top_activities_by_gender_df.head(5))

    
    min_number_of_sports = 1
    sport_df = df \
        .select(df.userId, df.gender, df.sport) \
        .distinct() \
        .groupBy(df.userId, df.gender) \
        .count()
    
    user_more_sports_df = sport_df \
        .filter(sport_df["count"] > min_number_of_sports) \
        .orderBy("count", ascending = False) \
        .toPandas()  
    user_more_sports_df.rename(columns = {'count':'Sports count'}, inplace = True)

    st.write("")
    st.write(":green[how many people participated in more than 1 sport ? ]")
    st.write("")
    st.table(user_more_sports_df.describe().astype(int).T)

    # Filter df with at least 10 records (as we are assumming if any user_id with less then 10 record would not be meaningful)
    qualified_df = df \
        .select(df.sport, df.userId, df.gender) \
        .groupBy(df.sport, df.userId, df.gender) \
        .count()
    
    qualified_df = qualified_df.filter(qualified_df["count"] >= 10).orderBy("count", ascending = False)
    qualified_pd_df = qualified_df.select("userId", "gender").distinct().groupBy(qualified_df.gender).count().toPandas()

    qualified_pd_df.rename(columns={'count': 'Users count'}, inplace=True)
    
    st.write("")
    st.subheader(":green[Number of users having more than 10 workouts : ]")
    st.table(qualified_pd_df)