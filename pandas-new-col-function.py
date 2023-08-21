# modules
import mysql.connector
import pandas as pd
import os

conn = mysql.connector.connect(read_default_file='/Users/conradkyiamah/.my.cnf')
cur_path = os.getcwd()
file = 'movie_watchability.csv'
file_path = os.path.join(cur_path, 'data_files', file)
print(file_path)

query = " select year " \
        ", title, genre, avg_vote " \
        ", case " \
        " when avg_vote < 3 then 'bad' " \
        " when avg_vote < 6 then 'okay' " \
        " when avg_vote >= 6 then 'good' " \
        " end as moving_rating " \
        ", duration " \
        "from `oscarval_sql_course`.`imdb_movies` " \
        "where year between 2005 and 2006"

# create a duration label function
def movie_duration(d):
        if d < 60:
                return 'short movie'
        elif d < 90:
                return 'avg length movie'
        elif d < 5000:
                return 'really long movie'
        else:
                return 'no data'

df  = pd.read_sql(query, conn)
df['watchability'] = df['duration'].apply(movie_duration)

yr_2005 = df['year'] == 2005

#print(df['year'].unique())
#print(yr_2005)
#print(df[yr_2005].head())
#print(df[~yr_2005].head())

#df.to_csv(file_path)
#df.to_csv(file_path, index=false)
df.to_csv(file_path, index=False)

conn.close()