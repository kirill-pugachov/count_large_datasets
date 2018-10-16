# -*- coding: utf-8 -*-
"""
Created on Mon Aug 27 09:57:42 2018

@author: tom
"""


import csv
import pandas as pd
import numpy as np
#import shelve as shv
import time
import datetime
import os


CURRENCY = 'USD'
PATH_TO_DIRECTORY_DEALS = 'C:\\Users\\tom\\usd_input_output_operation\\Data\\Deals'
PATH_TO_DIRECTORY_INOUT = 'C:\\Users\\tom\\usd_input_output_operation\\Data\\In_out'
PATH_TO_DIRECTORY_RESULT = 'C:\\Users\\tom\\usd_input_output_operation\\Result\\Deals_in_USD\\Deals_in_USD_short'
PARTS_SIZE = 200000000
RESULT_FILE_NAME = 'currancy_to_USD_rates.csv'


outcome_list = [
        'dt',
        'trade_id',
        'trade_type',
        'user_sell',
        'user_buy',
        'pair_name',
        'quantity_in_USD',
        'amount_in_USD',
        'profit_buy_amount_in_USD',
        'profit_buy_user_amount_in_USD',
        'profit_sell_amount_in_USD',
        'profit_sell_user_amount_in_USD'
        ]


def csv_to_df(path_to_file, path_to_dir):
    df = pd.read_csv(path_to_dir + '\\' + path_to_file,  header=0, sep=',')
    return df


def active_pair_list(df):
    res = df.pair_name.unique().tolist()
    return res


def pair_price_frame(deals_df, pair):
    temp = deals_df[deals_df['pair_name'] == pair]
    temp = temp.groupby([temp['date'].dt.hour, 'day'])['price'].mean().unstack()
    return temp


def get_price_dict(pair_price):
    temp = dict()
    for col in pair_price.columns.tolist():
        for ind in pair_price.index.tolist():
            s = col + '-' + str(ind)
            key = int(time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d-%H").timetuple()))
#            print(s, key, pair_price[col][ind])
            if pair_price[col][ind] != 0.0:
                if key in temp.keys():
                    temp[key] = pair_price[col][ind]
                else:
                    temp[key] = pair_price[col][ind]
    return temp


#def curr_pair_price_dict(PATH_TO_DEALS):
#    result_dict = dict()
#    deals_df = csv_to_df(PATH_TO_DEALS)
#    deals_df['date'] = pd.to_datetime(deals_df['dt'], dayfirst=False, yearfirst=False)
#    pair_list = active_pair_list(deals_df)
#    for pair in pair_list:
#        pair_price = pair_price_frame(deals_df, pair)
#        pair_price.fillna(0, inplace=True)
##        pair_price.dropna(how='any')
#        pair_price_dict = get_price_dict(pair_price)
#        if pair in result_dict.keys():
#            result_dict[pair].update(pair_price_dict)
#        else:
#            result_dict[pair] = pair_price_dict
#    return result_dict

    
def curr_pair_price_dict(PATH_TO_DEALS, PATH_TO_DIR):
    result_dict = dict()
    deals_df = csv_to_df(PATH_TO_DEALS, PATH_TO_DIR)
    deals_df['date'] = pd.to_datetime(deals_df['dt'], dayfirst=False, yearfirst=False)
    pair_list = active_pair_list(deals_df)
    for pair in pair_list:
        pair_price = pair_price_frame(deals_df, pair)
        pair_price.fillna(0, inplace=True)
#        pair_price.dropna(how='any')
        pair_price_dict = get_price_dict(pair_price)
        if pair in result_dict.keys():
            result_dict[pair].update(pair_price_dict)
        else:
            result_dict[pair] = pair_price_dict
    return result_dict


def rev_key(key_str):
    '''
    Разворот валютной пары
    DOGE_USD
    на
    USD_DOGE
    '''
    return (key_str.split('_')[1] +'_'+ key_str.split('_')[0])


def reverse_curs_create(pair_price_from_deals):
    '''
    Пересчет курса для развернутой пары
    '''
    temp_dict = dict()
    for key in pair_price_from_deals.keys():
        temp_dict_0 = dict()
        for key_0 in pair_price_from_deals[key].keys():
            temp_dict_0[key_0] = (1/pair_price_from_deals[key][key_0])
        temp_dict[rev_key(key)] = temp_dict_0
    return temp_dict    


def available_curr_deals(pair_price_from_deals):
    '''
    Валюты присутствующие в Сделках
    '''
    temp_set = set()
    for pair in list(pair_price_from_deals.keys()):
        temp_set.add(pair.split('_')[0])
        temp_set.add(pair.split('_')[1])
    return list(temp_set)


def check_curr_presense_in_deals(curr_to_count, curr_deals):
    '''
    Проверяем, чтобы все валюты в вводе-выводе были представлены в Сделках
    Если какой-то валюты нет, добавляем в список отсутствующих валют
    Возвращаем список, если пустой все валюты из ввода-вывода есть в Сделках
    Если список не пустой - значит есть валюты отсутсвующие в Сделках
    '''
    no_curr_in_deals = list()
    for curr in curr_to_count:
        if curr not in curr_deals:
            no_curr_in_deals.append(curr_to_count)
    return no_curr_in_deals


#def get_pair_to_count(curr_to_count, CURRENCY):
#    '''
#    Определяем валютные пары, которых нет в справочнике курсов в Сделках
#    '''
#    res = list()
#    for curr in curr_to_count:
#        if curr != CURRENCY:
#            temp = pair_price_from_deals.get(curr + '_' + CURRENCY, 'no pair in dict')
#            if temp == 'no pair in dict':
#                print(curr + '_' + CURRENCY, temp)
#                res.append(curr + '_' + CURRENCY)
#                
#    if len(res) == 0:
#        print('\n')
#        print('All pairs are present')
#        print('\n', '\n')
#        
#    return res


def build_pair(curr, pair_price_from_deals, curr_deals):
    temp = list()
    for c_d in curr_deals:
        if pair_price_from_deals.get(curr + '_' + c_d, 'No pair') != 'No pair':
            temp.append(curr + '_' + c_d)
    return temp


def build_pair_path(curr_pair_to_count_rate, pair_price_from_deals, curr_deals):
    '''
    Строим валютные связки (пары валют вида ['DOGE_BTC', 'BTC_USD'])
    для расчета курса 'DOGE_USD' через промежуточную валюту
    '''
    res_dict = dict()
    for pair in curr_pair_to_count_rate:
        temp_pair_list = build_pair(pair.split('_')[0], pair_price_from_deals, curr_deals)
        if pair in temp_pair_list:
            pass
        else:
            for temp_pair in temp_pair_list:
                temp_0_pair_list = build_pair(temp_pair.split('_')[1], pair_price_from_deals, curr_deals)
                if (temp_pair.split('_')[1] + '_' + pair.split('_')[1]) in temp_0_pair_list:
                    if pair in res_dict.keys():
                        res_dict[pair].append([temp_pair, (temp_pair.split('_')[1] + '_' + pair.split('_')[1])])
                    else:
                        res_dict[pair] = [[temp_pair, (temp_pair.split('_')[1] + '_' + pair.split('_')[1])]]
    return res_dict


def check_pathes(path_to_count_pair_rate):
    '''
    Проверка что есть хотя бы одна валютная связка 
    для расчета курса
    '''
    temp = 0
    for pair in path_to_count_pair_rate.values():
        if len(pair) > 0:
            temp += 1
    if temp >= len(path_to_count_pair_rate):
        return 1
    else:
        return 0


def count_paire_rate(path_to_count_pair_rate, curr_pair_to_count_rate, pair_price_from_deals):
    temp_dict = dict()
    for curr_pair in curr_pair_to_count_rate:
        temp_dict_0 = dict()
        path = path_to_count_pair_rate[curr_pair][0]
        pair_0 = pair_price_from_deals[path[0]]
        pair_1 = pair_price_from_deals[path[1]]
        for key in pair_0.keys():
            if pair_0.get(key, 0) != 0 and pair_1.get(key, 0) != 0:
                temp_dict_0[key] = pair_0.get(key, 0) * pair_1.get(key, 0)
        temp_dict[curr_pair] = temp_dict_0
    return temp_dict


def str_to_timestamp(raw):
    s = raw.split(' ')[0] + ' ' + raw.split(' ')[1].split(':')[0]
    temp = int(time.mktime(datetime.datetime.strptime(s, '%Y-%m-%d %H').timetuple()))
    return temp


def curr_to_pair(raw, CURRENCY):
    if raw != CURRENCY:
        s = raw + '_' + CURRENCY
    else:
        s = raw
    return s


def get_pair_curr_price(PATH_TO_DIRECTORY_DEALS):
    '''
    Получаем словарь почасовых курсов валютных пар
    представленных в папке PATH_TO_DIRECTORY_DEALS
    файлов сделок
    '''
    file_list = os.listdir(PATH_TO_DIRECTORY_DEALS)
    res_pair_dict = dict()
    for file in file_list:
#        print(file)
    
        pair_price_from_deals = curr_pair_price_dict(file, PATH_TO_DIRECTORY_DEALS)
#        print(len(pair_price_from_deals))
#        print('\n')
        pair_price_from_deals_reversed = reverse_curs_create(pair_price_from_deals)
        pair_price_from_deals.update(pair_price_from_deals_reversed)
        del pair_price_from_deals_reversed
        
        for key in pair_price_from_deals:
            if key in res_pair_dict:
                res_pair_dict[key].update(pair_price_from_deals[key])
            else:
                res_pair_dict[key] = pair_price_from_deals[key]
    return res_pair_dict


def total_currencies_list(curr_dict):
    '''
    Получаем список всех валют
    которые присутствовали на бирже
    '''
    res = set()
    for pair in curr_dict:
        res.add(pair.split('_')[0])
        res.add(pair.split('_')[1])
    return res


def curr_pair_to_count_usd(curr_list, curr_dict):
    '''
    Определяем пары для расчета курса валюта_USD
    '''
    res = set()
    for curr in curr_list:
        if curr != CURRENCY:
            if (curr + '_' + CURRENCY) not in curr_dict:
                res.add((curr + '_' + CURRENCY))
    return res


def select_path_to_count_in_USD(pair_path):
    '''
    Выясняем через какие две валютные пары рассчитывать
    стоимость заданной валюты в USD
    Если первая встречается BTC используем связку сurr_BTC - BTC_USD
    Если ETH, точно так же через ETH
    Если пара всего одна используем то, что имеем
    '''
    res = dict()
    for key in pair_path:
        if len(pair_path[key]) == 1:
            res[key] = pair_path[key][0]
        else:
            for path in pair_path[key]:
                if 'BTC' in path[0] and 'BTC' in path[1]:
                    res[key] = path
                    break
                elif 'ETH' in path[0] and 'ETH' in path[1]:
                    res[key] = path
                    break
                else:
                    res[key] = path  
    return res


def get_pair_curr_rate_by_hours(PATH_TO_DIRECTORY_DEALS):
    pair_curr_rate_by_hour = get_pair_curr_price(PATH_TO_DIRECTORY_DEALS)
    currencies_list = total_currencies_list(pair_curr_rate_by_hour)
    pair_to_count_in_usd = curr_pair_to_count_usd(currencies_list, pair_curr_rate_by_hour)
    pathes_to_count_pair_rate = build_pair_path(pair_to_count_in_usd, pair_curr_rate_by_hour, currencies_list)
    
    if check_pathes(pathes_to_count_pair_rate):
        curr_path_to_count_in_usd = select_path_to_count_in_USD(pathes_to_count_pair_rate)
        
        for curr in curr_path_to_count_in_usd:
            temp_dict = dict()
            temp_0 = pair_curr_rate_by_hour[curr_path_to_count_in_usd[curr][0]]
            temp_1 = pair_curr_rate_by_hour[curr_path_to_count_in_usd[curr][1]]
            for timestamp in temp_0.keys():
                if timestamp in temp_1.keys():
                    temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp]
                else:
                    if timestamp > sorted(list(temp_1.keys()))[0]:
                        temp_dict[timestamp] = temp_0[timestamp] * sorted(list(temp_1.keys()))[0]
                    elif timestamp > sorted(list(temp_1.keys()))[-1]:
                        temp_dict[timestamp] = temp_0[timestamp] * sorted(list(temp_1.keys()))[-1]
                    elif timestamp < sorted(list(temp_1.keys()))[-1]:   
                        if timestamp - 3600 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp - 3600]
                        elif timestamp - 7200 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp - 7200]
                        elif timestamp - 10800 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp - 10800]
                        elif timestamp - 14400 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp - 14400]
                        elif timestamp - 18000 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp - 18000]
                        elif timestamp + 3600 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp + 3600]
                        elif timestamp + 7200 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp + 7200]
                        elif timestamp + 10800 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp + 10800]
                        elif timestamp + 14400 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp + 14400]
                        elif timestamp + 18000 in temp_1.keys():
                            temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp + 18000]                    
            pair_curr_rate_by_hour[curr] = temp_dict
    return pair_curr_rate_by_hour


def get_rate_by_pair_timestamp(pair, timestamp):
    if timestamp in pair_curr_rate_by_hours[pair]:
        temp = pair_curr_rate_by_hours[pair][timestamp]
    else:
        if timestamp > sorted(list(pair_curr_rate_by_hours[pair].keys()))[-1]:
            temp = sorted(list(pair_curr_rate_by_hours[pair].keys()))[-1]
        elif timestamp < sorted(list(pair_curr_rate_by_hours[pair].keys()))[0]:
            temp = sorted(list(pair_curr_rate_by_hours[pair].keys()))[0]
        elif timestamp < sorted(list(pair_curr_rate_by_hours[pair].keys()))[-1]:   
            if timestamp - 3600 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp - 3600]
            elif timestamp - 7200 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp - 7200]
            elif timestamp - 10800 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp - 10800]
            elif timestamp - 14400 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp - 14400]
            elif timestamp - 18000 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp - 18000]
            elif timestamp + 3600 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp + 3600]
            elif timestamp + 7200 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp + 7200]
            elif timestamp + 10800 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp + 10800]
            elif timestamp + 14400 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp + 14400]
            elif timestamp + 18000 in pair_curr_rate_by_hours[pair]:
                temp = pair_curr_rate_by_hours[pair][timestamp + 18000]
    return temp
            

def get_amount_in_USD(raw):
    '''
    Пересчитываем кол-во валюты №1 сделки
    в USD по курсу данного часа сделки
    '''
#    print(raw['curr2'])
    if raw['curr2'] != 'USD':
        pair = raw['curr2'] + '_' + 'USD'
        rate = get_rate_by_pair_timestamp(pair, raw['timestamp'])
#        rate = pair_curr_rate_by_hours[pair][raw['timestamp']]
        amount = raw['amount'] * rate
    else:
        amount = raw['amount']
    return amount
    

def get_quantity_in_USD(raw):
    '''
    Пересчитываем кол-во валюты №2 сделки
    в USD по курсу данного часа сделки
    '''
    if raw['curr1'] != 'USD':
        pair = raw['curr1'] + '_' + 'USD'
        rate = get_rate_by_pair_timestamp(pair, raw['timestamp'])
    #    rate = pair_curr_rate_by_hours[pair][raw['timestamp']]
        quantity = raw['quantity'] * rate
    else:
        quantity = raw['quantity']        
    return quantity


def get_profit_buy_amount_in_USD(raw):
    '''
    Персчитываем profit_buy_amount
    '''
    if raw['profit_buy_currency'] != 'USD':
        pair = raw['profit_buy_currency'] + '_' + 'USD'
        rate = get_rate_by_pair_timestamp(pair, raw['timestamp'])
    #    rate = pair_curr_rate_by_hours[pair][raw['timestamp']]
        amount = raw['profit_buy_amount'] * rate
    else:
        amount = raw['profit_buy_amount']    
    return amount


def get_profit_buy_user_amount_in_USD(raw):
    if raw['profit_buy_currency'] != 'USD':
        pair = raw['profit_buy_currency'] + '_' + 'USD'
        rate = get_rate_by_pair_timestamp(pair, raw['timestamp'])
    #    rate = pair_curr_rate_by_hours[pair][raw['timestamp']]
        amount = raw['profit_buy_user_amount'] * rate
    else:
        amount = raw['profit_buy_user_amount']    
    return amount
    

def get_profit_sell_amount_in_USD(raw):
    if raw['profit_sell_currency'] != 'USD':
        pair = raw['profit_sell_currency'] + '_' + 'USD'
        rate = get_rate_by_pair_timestamp(pair, raw['timestamp'])
    #    rate = pair_curr_rate_by_hours[pair][raw['timestamp']]
        amount = raw['profit_sell_amount'] * rate
    else:
        amount = raw['profit_sell_amount']         
    return amount


def get_profit_sell_user_amount_in_USD(raw):
    if raw['profit_sell_currency'] != 'USD':
        pair = raw['profit_sell_currency'] + '_' + 'USD'
        rate = get_rate_by_pair_timestamp(pair, raw['timestamp'])
    #    rate = pair_curr_rate_by_hours[pair][raw['timestamp']]
        amount = raw['profit_sell_user_amount'] * rate
    else:
        amount = raw['profit_sell_user_amount']         
    return amount


def get_max_files_size(file_list):
    res_size_list = list()
    for file in file_list:
        res_size_list.append(os.path.getsize(PATH_TO_DIRECTORY_DEALS + '\\' + file))
    return max(res_size_list)
    

if __name__ == '__main__':
    
    pair_curr_rate_by_hours = get_pair_curr_rate_by_hours(PATH_TO_DIRECTORY_DEALS)
    
    file_list = os.listdir(PATH_TO_DIRECTORY_DEALS)
    
    for file in file_list:
        df = csv_to_df(file, PATH_TO_DIRECTORY_DEALS)
        df['timestamp'] = df['dt'].apply(lambda x: str_to_timestamp(x))
        df['amount_in_USD'] = df.apply(get_amount_in_USD, axis=1)
        df['quantity_in_USD'] = df.apply(get_quantity_in_USD, axis=1)
        df['price_in_USD'] = df['amount_in_USD'] / df['quantity_in_USD']
        df['profit_buy_amount_in_USD'] = df.apply(get_profit_buy_amount_in_USD, axis=1)
        df['profit_buy_user_amount_in_USD'] = df.apply(get_profit_buy_user_amount_in_USD, axis=1)
        df['profit_sell_amount_in_USD'] = df.apply(get_profit_buy_amount_in_USD, axis=1)
        df['profit_sell_user_amount_in_USD'] = df.apply(get_profit_buy_user_amount_in_USD, axis=1)
        
        short_df = df[outcome_list]
        
        parts_number = get_max_files_size(file_list) // PARTS_SIZE
        
        frame_list = np.array_split(short_df, parts_number)
        
        size_control = 0
        index = 0
        for frame in frame_list:
            frame.to_csv(PATH_TO_DIRECTORY_RESULT + '\\' +'Deals_in_USD_short_hours_' + str(index) + '_' + file, index=False)
            size_control += frame.shape[0]
            index += 1
        
        if size_control == short_df.shape[0]:
            print(file, ' processing finished')
            print(size_control, df.shape[0], 'sizes is Ok')
        else:
            print(file, ' processing finished')
            print('Alarm! ' * 3)
            print(size_control, df.shape[0], 'sizes is wrong')            


    for pair_key in pair_curr_rate_by_hours:
        if pair_key.split('_')[1] == 'USD':
            with open(PATH_TO_DIRECTORY_RESULT + '\\' + RESULT_FILE_NAME, 'a') as file_large:
                csv_writer = csv.writer(file_large, quoting=csv.QUOTE_NONNUMERIC)
                for time_key in pair_curr_rate_by_hours[pair_key]:
                    date_human = datetime.utcfromtimestamp(time_key).strftime('%m/%d/%Y %H:%M:%S')
                    csv_writer.writerow([pair_key, date_human, pair_curr_rate_by_hours[pair_key][time_key]])
                
