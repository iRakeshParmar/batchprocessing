import datetime
import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from Pandasentities import StockMain, t_StockDetail_dtype ,t_StockMain_dtype,BatchProcessStats
import urllib
import pyodbc
import multiprocessing as mp
import numpy as np


def save_stats(loadstarttime,loadendtime,totalloadtime):
    params = urllib.parse.quote_plus("[Your DB Connection String]")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True , echo=False)
    with Session(engine) as session:
        try:
            BatchProcessStatsObj = BatchProcessStats('PYTHON, PANDAS, if_exists=append, fast_executemany = True, multiprocessing = Yes ',loadstarttime,loadendtime,totalloadtime)
            session.add(BatchProcessStatsObj)
            session.commit()     
            endtime = datetime.datetime.now()
        except Exception as ex:
            session.rollback()
            print("An exception occurred", ex)

def truncate_table():
    params = urllib.parse.quote_plus("[Your DB Connection String]")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True , echo=False)
    with Session(engine) as session:
        try:
            starttime = datetime.datetime.now()
            session.execute(text("truncate table Stock_Detail"))
            session.commit()
            endtime = datetime.datetime.now()
            totaltime = endtime - starttime
            print('Time taken for truncate table : ' ,  totaltime)
        except Exception as ex:
            session.execute(text("truncate table Stock_Detail"))
            session.commit()
            print("An exception occurred", ex)
def insert_data(StockMainList):
    params = urllib.parse.quote_plus("[Your DB Connection String]")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True , echo=False)
    with Session(engine) as session:
        try:
            for data in StockMainList:
                StockMainObj = StockMain(data)
                Detaildf = pd.read_csv(r'[Your file dir path]' + StockMainObj.Symbol + '.csv')
                Detaildf.round(decimals = 8)
                Detaildf.fillna(0)
                Detaildf['Symbol'] = StockMainObj.Symbol
                #print(StockMainObj.Symbol)
                Detaildf.to_sql(name='Stock_Detail',con= engine, if_exists='append',chunksize=1000 ,index=False,dtype=t_StockDetail_dtype)
            session.commit()
        except Exception as ex:
            session.execute(text("truncate table StockDetail"))
            session.commit()
            print("An exception occurred", ex)
if __name__ == '__main__':
    truncate_table()
    loadstarttime = datetime.datetime.now()
    Maindf = pd.read_csv(r'[Your file dir path]\symbols_valid_meta.csv')
    Maindf.sort_values(by=['Symbol'], ascending=False,inplace=True)
    StockMainList = Maindf.values.tolist()
    num_processes = mp.cpu_count() - 1
    df_split = np.array_split(StockMainList, num_processes)
    pool = mp.Pool(processes = (num_processes))
    pool.map(insert_data, df_split)
    pool.close()
    pool.join()
    print('Load Start Time : ' ,  loadstarttime)
    loadendtime = datetime.datetime.now()
    print('Load End Time : ' ,  loadendtime)
    totalloadtime = loadendtime - loadstarttime
    print('Time taken for Load  files : ' ,  totalloadtime)
    save_stats(loadstarttime,loadendtime,totalloadtime)  