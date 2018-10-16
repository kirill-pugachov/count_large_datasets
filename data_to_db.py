# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 15:11:35 2018

@author: tom
"""


import time
import os
import sqlalchemy as sql_al
import pandas as pd


TABLE_NAME = 'in_out_operation'
DB = 'input_output_in_usd'
HOST = '@localhost:3306'
LOGIN = 'root:'
PSWD = '1'

RES_INOUT_DATA = 'C:\\Users\\tom\\usd_input_output_operation\\Result\\In_out_short_in_USD'

def get_engine():
    engine = sql_al.create_engine('mysql+pymysql://' + LOGIN + PSWD + HOST + '/' + DB)
    return engine


def get_file_list(path_to_data):
    file_list = os.listdir(path_to_data)
    return file_list


def csv_to_df(path_to_file):
    df = pd.read_csv(
            path_to_file,
            header=0,
            sep=',',
            encoding="utf8",
            error_bad_lines=False,
            low_memory=False,
            skipinitialspace=True
            )
    return df


if __name__ == '__main__':
    eng = get_engine()
    files = get_file_list(RES_INOUT_DATA)
    
    for file in files:
        file_name = RES_INOUT_DATA + '\\' + file
        temp_df = csv_to_df(file_name)
        temp_df.to_sql(name=TABLE_NAME, con=eng, if_exists='append', index=False, index_label=None)
        print(file, 'Записан в базу')
        time.sleep(60)
        