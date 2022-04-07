from sqlalchemy import create_engine
import os 
import math
import json,urllib,pyodbc
import pandas as pd

def conection():
    with open('DBconfig.json') as conn:

        ConnetionDB = 'Connectionlocal'

        config = json.loads(conn.read())

        database_name = config[ConnetionDB]["DB_NAME"]

        database_user = config[ConnetionDB]["DB_USER"]

        database_password = config[ConnetionDB]["DB_PASSWORD"]

        database_server = config[ConnetionDB]["SERVER"]

        dbms_driver = config[ConnetionDB]["DRIVER"]

        ConnectionString = "DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}".format(dbms_driver, database_server, database_name, database_user, database_password)


        quoted = urllib.parse.quote_plus(ConnectionString)

        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    return engine
    

path = 'C:\\Users\\path_files'
  
contenido = os.listdir(path) 
  
file_csv = [ file for file in contenido if os.path.isfile(os.path.join(path, file)) and file.endswith('.csv') ]
files =  [* map(pd.read_csv,file_csv)]

df_num_of_cols = len(files[0].columns)
    
chunknum = math.floor(2100/df_num_of_cols)

conn= conection()

for file in files:
    
    file.to_sql("name_table",con= conn,index=False,schema="dbo",if_exists='append', chunksize=chunknum,method='multi')

