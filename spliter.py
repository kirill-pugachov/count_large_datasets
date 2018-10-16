# -*- coding: utf-8 -*-
"""
Created on Fri Sep 21 13:57:12 2018

@author: tom
"""
import os
import pandas as pd
import numpy as np


LARGE_FILE_PATH = 'C:\\Users\\tom\\usd_input_output_operation\\Result\\Cash_back'
SMOLE_FILES_PATH = 'C:\\Users\\tom\\usd_input_output_operation\\Result\\Cash_back\\Cashback_by_parts'
PARTS_SIZE = 200000000
LARGE_FILE_NAME = 'Cashback_Deals_USD_september_2018.csv'


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


def get_files_size(LARGE_FILE_PATH, file_list):
    res_dict = dict()
    for file in file_list:
        res_dict[file] = os.path.getsize(LARGE_FILE_PATH + '\\' + file)
    return max(res_dict.values())


file_list = os.listdir(LARGE_FILE_PATH)
file_list = [name for name in os.listdir(LARGE_FILE_PATH) if 'Cashback_Deals_USD' in name]

##split by rows
#filename = 1
#for LARGE_FILE_NAME in file_list:
#    csvfile = open(LARGE_FILE_PATH + '\\' + LARGE_FILE_NAME, 'r').readlines()
#    for i in range(len(csvfile)):
#        if i % 1000000 == 0:
#            open(SMOLE_FILES_PATH +'\\'+ str(filename) + '.csv', 'w+').writelines(csvfile[i:i+1000000])
#            filename += 1


parts_number = get_files_size(LARGE_FILE_PATH, file_list) // PARTS_SIZE
        
#parts_number = 12


for file in file_list:
    if file == LARGE_FILE_NAME:
        path_to_file = LARGE_FILE_PATH + '\\' + file
        df = csv_to_df(path_to_file)
        frame_list = np.array_split(df, parts_number) 
        size_control = 0
        index = 0
        for frame in frame_list:
            frame.to_csv(SMOLE_FILES_PATH + '\\' +'short_in_out_USD_' + str(index) + '_' + file, index=False)
            size_control += frame.shape[0]
            index += 1
            
        if size_control == df.shape[0]:
            print(file, ' processing finished')
            print(size_control, df.shape[0], 'sizes is Ok')
        else:
            print(file, ' processing finished')
            print('Alarm! ' * 3)
            print(size_control, df.shape[0], 'sizes is wrong') 