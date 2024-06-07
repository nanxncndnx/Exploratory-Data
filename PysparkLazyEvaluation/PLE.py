import streamlit as st
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib

def create_page(df):
    ranked_sport_users_df = df.select(df.sport, df.userId) \
        .distinct() \
        .groupBy(df.sport) \
        .count() \
        .orderBy("count", ascending=False)
    
    # Lets Extracting Top 5 workout types =>
    highest_sport_users_df = ranked_sport_users_df.limit(5).toPandas()

    # Rename column name (count) To Users count =>
    highest_sport_users_df.rename(columns = {'count':'Users count'}, inplace = True)

    # Caculate the total users =>
    total_sports_users = ranked_sport_users_df.groupBy().sum().collect()[0][0]
    st.table(ranked_sport_users_df.collect()[:5])
     
    # Compute the percentage of top 5 Workout type / Total Users
    highest_sport_users_df_renamed = highest_sport_users_df
    highest_sport_users_df_renamed['percentage'] = highest_sport_users_df['Users count'] / total_sports_users * 100

    # We assign the rest of users belong to another specific group that we call 'others'
    others = {
          'sport': 'others'
        , 'Users count': total_sports_users - sum(highest_sport_users_df_renamed['Users count'])
        , 'percentage': 100 - sum(highest_sport_users_df_renamed['percentage'])
    }

    highest_sport_users_df_renamed = pd.concat([highest_sport_users_df_renamed, pd.DataFrame([others])], ignore_index=True)

    st.subheader(":green[Top 5 sports that have the most users participated]", divider="green")
    st.table(highest_sport_users_df_renamed)

    # Let quick overview activities by gender
    activities_by_gender = df.groupBy('sport', 'gender').count().toPandas()
    st.write("")
    st.write(":green[Activities By Gender]") 
    st.table(activities_by_gender[:5])