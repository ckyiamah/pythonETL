# modules
import mysql.connector
import pandas as pd
import os

conn = mysql.connector.connect(read_default_file='/Users/conradkyiamah/.my.cnf')
cur_path = os.getcwd()
file = 'city_housing.csv'
file_path = os.path.join(cur_path, 'data_files', file)
print(file_path)

query = """ select * from `oscarval_sql_course`.`city_house_prices` """

# data transformation steps
df  = pd.read_sql(query, conn)

df.set_index('Date', inplace=True)
df = df.stack().reset_index()

df.columns = ['date', 'city_and_state', 'price']
df['state'] = df['city_and_state'].split("_").df['city_and_state'].get(0)
df['city'] = df['city_and_state'].split("_").df['city_and_state'].get(1)
#print(df.dtypes)
df.to_csv(file_path, index=False)
