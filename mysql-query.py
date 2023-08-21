# modules
import mysql.connector
from mysql.connector import errorcode

conn = mysql.connector.connect(read_default_file='/Users/conradkyiamah/.my.cnf')
cursor = conn.cursor()
query = " select year " \
        ", title, genre " \
        "from `oscarval_sql_course`.`imdb_movies` " \
        "limit 5 "

cursor.execute(query)

for(year, title, genre) in cursor:
        print(year, title, genre)
conn.close()


