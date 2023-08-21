# modules
import mysql.connector
from mysql.connector import errorcode

try:
    conn = mysql.connector.connect(read_default_file='/Users/conradkyiamah/.my.cnf')
    print('Connection Successful')
    conn.close()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Check Credentials')
    else:
        print('err')

