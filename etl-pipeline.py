from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

driver = "{SQL Server Native Client 11.0}"
server = "haq-PC"
database = "AdventureWorksDW2019;"

def extract():
    try:
        src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS' + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)
        src_cursor = src_conn.cursor()
        src_cursor.execute(""" select t.name as table_name from sys.tables t where t.name in ('DinProduct, 'DinProductSubcategory') """)
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
    except Exception as e:
        print("Data error: " + str(e))
    finally:
        src_conn.close()

def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/AdventureWorks')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print("Data import successful")
    except Exception as e:
        print("Data error " + str(e))

try:
    extract()
except Exception as e:
    print("Error while extracting data " + str(e))
    
