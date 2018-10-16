# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 15:05:40 2018

@author: tom
"""


import pandas as pd
#import shelve as shv
import time
import datetime
import os


PATH_TO_DEALS = 'deals_may_2018.csv'
PATH_TO_IN_OUT = 'in_out_may_2018.csv'
CURRENCY = 'USD'
PATH_TO_DIRECTORY_DEALS = 'C:\\Users\\tom\\usd_input_output_operation\\Data\\Deals'
PATH_TO_DIRECTORY_INOUT = 'C:\\Users\\tom\\usd_input_output_operation\\Data\\In_out'
PATH_TO_DIRECTORY_RESULT = 'C:\\Users\\tom\\usd_input_output_operation\\Result'
NEEDED_COLUMNS = [
        'id',
        'login',
        'order_id',
        'operation_type', 
        'dt', 
        'currency', 
        'old_provider', 
        'provider', 
        'profit_in_USD',
        'pr_sum_in_USD',
        'our_sum_in_USD',
        'amount_in_USD'
        ]



#def csv_to_df(path_to_file):
#    df = pd.read_csv(path_to_file,  header=0, sep=',')
#    return df


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


def reverse_curs_create(curr_dict):
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


def available_curr_deals(curr_dict):
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


def get_pair_to_count(curr_to_count, CURRENCY):
    '''
    Определяем валютнрые пары, которых нет в справочнике курсов в Сделках
    '''
    res = list()
    for curr in curr_to_count:
        if curr != CURRENCY:
            temp = pair_price_from_deals.get(curr + '_' + CURRENCY, 'no pair in dict')
            if temp == 'no pair in dict':
                print(curr + '_' + CURRENCY, temp)
                res.append(curr + '_' + CURRENCY)
                
    if len(res) == 0:
        print('\n')
        print('All pairs are present')
        print('\n', '\n')
        
    return res


def build_pair(curr, pair_price_from_deals, curr_deals):
    temp = list()
    for c_d in curr_deals:
        if pair_price_from_deals.get(curr + '_' + c_d, 'No pair') != 'No pair':
            temp.append(curr + '_' + c_d)
    return temp


def build_pair_path(curr_pair_to_count_rate):
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
    if 'T' in raw:
        s = raw.split('T')[0] + ' ' + raw.split('T')[1].split(':')[0]
        temp = int(time.mktime(datetime.datetime.strptime(s, '%Y-%m-%d %H').timetuple()))
    else:
        s = raw.split(' ')[0] + ' ' + raw.split(' ')[1].split(':')[0]
        temp = int(time.mktime(datetime.datetime.strptime(s, '%Y-%m-%d %H').timetuple()))
    return temp


def curr_to_pair(raw, CURRENCY):
    if raw != CURRENCY:
        s = raw + '_' + CURRENCY
    else:
        s = raw
    return s


def rate_to_pair(raw):
    if raw['currency'] != CURRENCY:
        curr_dict = pair_price_from_deals[raw['pair_to_count']]
        if curr_dict.get(raw['timestamp'], 0):
            temp = curr_dict.get(raw['timestamp'], 0)
        else:
            if raw['timestamp'] > sorted(list(curr_dict.keys()))[-1]:
                temp = curr_dict.get(sorted(list(curr_dict.keys()))[-1], 0)
            elif raw['timestamp'] < sorted(list(curr_dict.keys()))[0]:
                temp = curr_dict.get(sorted(list(curr_dict.keys()))[0], 0)
            else:
                if curr_dict.get(raw['timestamp'] - 3600, 0):
                    temp = curr_dict.get(raw['timestamp'] - 3600, 0)
                elif curr_dict.get(raw['timestamp'] - 7200, 0):
                    temp = curr_dict.get(raw['timestamp'] - 7200, 0)
                elif curr_dict.get(raw['timestamp'] - 10800, 0):
                    temp = curr_dict.get(raw['timestamp'] - 10800, 0)
                elif curr_dict.get(raw['timestamp'] - 14400, 0):
                    temp = curr_dict.get(raw['timestamp'] - 14400, 0)
                elif curr_dict.get(raw['timestamp'] - 18000, 0):
                    temp = curr_dict.get(raw['timestamp'] - 18000, 0)
                elif curr_dict.get(raw['timestamp'] - 21600, 0):
                    temp = curr_dict.get(raw['timestamp'] - 21600, 0)
                elif curr_dict.get(raw['timestamp'] - 25200, 0):
                    temp = curr_dict.get(raw['timestamp'] - 25200, 0)
                elif curr_dict.get(raw['timestamp'] - 28800, 0):
                    temp = curr_dict.get(raw['timestamp'] - 28800, 0)
                elif curr_dict.get(raw['timestamp'] - 32400, 0):
                    temp = curr_dict.get(raw['timestamp'] - 32400, 0)
                elif curr_dict.get(raw['timestamp'] - 36000, 0):
                    temp = curr_dict.get(raw['timestamp'] - 36000, 0)
                elif curr_dict.get(raw['timestamp'] - 39600, 0):
                    temp = curr_dict.get(raw['timestamp'] - 39600, 0)
                elif curr_dict.get(raw['timestamp'] - 43200, 0):
                    temp = curr_dict.get(raw['timestamp'] - 43200, 0)
                elif curr_dict.get(raw['timestamp'] - 46800, 0):
                    temp = curr_dict.get(raw['timestamp'] - 46800, 0)                  
                elif curr_dict.get(raw['timestamp'] + 3600, 0):
                    temp = curr_dict.get(raw['timestamp'] + 3600, 0)
                elif curr_dict.get(raw['timestamp'] + 7200, 0):
                    temp = curr_dict.get(raw['timestamp'] + 7200, 0)
                elif curr_dict.get(raw['timestamp'] + 10800, 0):
                    temp = curr_dict.get(raw['timestamp'] + 10800, 0)
                elif curr_dict.get(raw['timestamp'] + 14400, 0):
                    temp = curr_dict.get(raw['timestamp'] + 14400, 0)
                elif curr_dict.get(raw['timestamp'] + 18000, 0):
                    temp = curr_dict.get(raw['timestamp'] + 18000, 0)
                elif curr_dict.get(raw['timestamp'] + 21600, 0):
                    temp = curr_dict.get(raw['timestamp'] + 21600, 0)
                elif curr_dict.get(raw['timestamp'] + 25200, 0):
                    temp = curr_dict.get(raw['timestamp'] + 25200, 0)
                elif curr_dict.get(raw['timestamp'] + 28800, 0):
                    temp = curr_dict.get(raw['timestamp'] + 28800, 0)
                elif curr_dict.get(raw['timestamp'] + 32400, 0):
                    temp = curr_dict.get(raw['timestamp'] + 32400, 0)
                elif curr_dict.get(raw['timestamp'] + 36000, 0):
                    temp = curr_dict.get(raw['timestamp'] + 36000, 0)
                elif curr_dict.get(raw['timestamp'] + 39600, 0):
                    temp = curr_dict.get(raw['timestamp'] + 39600, 0)
                elif curr_dict.get(raw['timestamp'] + 43200, 0):
                    temp = curr_dict.get(raw['timestamp'] + 43200, 0)
                elif curr_dict.get(raw['timestamp'] + 46800, 0):
                    temp = curr_dict.get(raw['timestamp'] + 46800, 0)                 
    else:
        temp = CURRENCY
    return temp


def count_in_USD(raw):
    if raw['currency'] != CURRENCY:
        temp = raw['amount'] * raw['curr_rate']
    else:
        temp = raw['amount']
    return temp


def coun_profit_in_USD(raw):
    if raw['currency'] != CURRENCY:
        temp = raw['profit'] * raw['curr_rate']
    else:
        temp = raw['profit']
    return temp


def coun_pr_sum_in_USD(raw):
    if raw['currency'] != CURRENCY:
        temp = raw['pr_sum'] * raw['curr_rate']
    else:
        temp = raw['pr_sum']
    return temp


def coun_our_sum_in_USD(raw):
    if raw['currency'] != CURRENCY:
        temp = raw['our_sum'] * raw['curr_rate']
    else:
        temp = raw['our_sum']
    return temp


if __name__ == '__main__':
    
    file_list = temp = zip(os.listdir(PATH_TO_DIRECTORY_DEALS), os.listdir(PATH_TO_DIRECTORY_INOUT))
    for file_pair in file_list:
        print(file_pair[0], file_pair[1])
    
#        pair_price_from_deals = curr_pair_price_dict(PATH_TO_DEALS)
        pair_price_from_deals = curr_pair_price_dict(file_pair[0], PATH_TO_DIRECTORY_DEALS)
        pair_price_from_deals_reversed = reverse_curs_create(pair_price_from_deals)
        pair_price_from_deals.update(pair_price_from_deals_reversed)
        del pair_price_from_deals_reversed
        
        in_out = csv_to_df(file_pair[1], PATH_TO_DIRECTORY_INOUT)
        
        curr_to_count = in_out.currency.unique().tolist()
        curr_deals = available_curr_deals(pair_price_from_deals)
        
        if check_curr_presense_in_deals(curr_to_count, curr_deals):
            print('ALARM')
            print('Следующих валют нет в Сделках', check_curr_presense_in_deals(curr_to_count, curr_deals))
            break
        else:
            curr_pair_to_count_rate = get_pair_to_count(curr_to_count, CURRENCY)
            path_to_count_pair_rate = build_pair_path(curr_pair_to_count_rate)
            
            if check_pathes(path_to_count_pair_rate):
                new_rates_dict = count_paire_rate(path_to_count_pair_rate, curr_pair_to_count_rate, pair_price_from_deals)
                pair_price_from_deals.update(new_rates_dict)
                in_out['timestamp'] = in_out['dt'].apply(lambda x: str_to_timestamp(x))
                in_out['pair_to_count'] = in_out['currency'].apply(lambda x: curr_to_pair(x, CURRENCY))
                in_out['curr_rate'] = in_out.apply(rate_to_pair, axis=1)
                in_out['amount_in_USD'] = in_out.apply(count_in_USD, axis=1)
                
                in_out['profit_in_USD'] = in_out.apply(coun_profit_in_USD, axis=1)
                in_out['pr_sum_in_USD'] = in_out.apply(coun_pr_sum_in_USD, axis=1)
                in_out['our_sum_in_USD'] = in_out.apply(coun_our_sum_in_USD, axis=1)
#                in_out.to_csv("USD_in_out_may_2018.csv", index=False)
                in_out.to_csv(PATH_TO_DIRECTORY_RESULT + '\\' +'USD_' + file_pair[1], index=False)
                short_in_out = in_out[NEEDED_COLUMNS]
                short_in_out.to_csv(PATH_TO_DIRECTORY_RESULT + '\\' +'USD_short_' + file_pair[1], index=False, decimal='.')
                if len(get_pair_to_count(curr_to_count, CURRENCY)) == 0:
                    pass
                    
            else:
                print('ALARM')
                print('Не хватает валютной пары для расчета курса в файлах:', file_pair[0], file_pair[1])
                break
            

#def rate_to_pair(raw):
#    if raw['currency'] != CURRENCY:
#        curr_dict = pair_price_from_deals[raw['pair_to_count']]
#        if curr_dict.get(raw['timestamp'], 0):
#            temp = curr_dict.get(raw['timestamp'], 0)
#        else:
#            if curr_dict.get(raw['timestamp'] - 3600, 0):
#                temp = curr_dict.get(raw['timestamp'] - 3600, 0)
#            elif curr_dict.get(raw['timestamp'] - 7200, 0):
#                temp = curr_dict.get(raw['timestamp'] - 7200, 0)
#            elif curr_dict.get(raw['timestamp'] - 10800, 0):
#                temp = curr_dict.get(raw['timestamp'] - 10800, 0)
#            elif curr_dict.get(raw['timestamp'] - 14400, 0):
#                temp = curr_dict.get(raw['timestamp'] - 14400, 0)
#            elif curr_dict.get(raw['timestamp'] - 18000, 0):
#                temp = curr_dict.get(raw['timestamp'] - 18000, 0)
#            elif curr_dict.get(raw['timestamp'] - 21600, 0):
#                temp = curr_dict.get(raw['timestamp'] - 21600, 0)
#            elif curr_dict.get(raw['timestamp'] - 25200, 0):
#                temp = curr_dict.get(raw['timestamp'] - 25200, 0)
#            elif curr_dict.get(raw['timestamp'] - 28800, 0):
#                temp = curr_dict.get(raw['timestamp'] - 28800, 0)
#            elif curr_dict.get(raw['timestamp'] - 32400, 0):
#                temp = curr_dict.get(raw['timestamp'] - 32400, 0)
#            elif curr_dict.get(raw['timestamp'] - 36000, 0):
#                temp = curr_dict.get(raw['timestamp'] - 36000, 0)
#            elif curr_dict.get(raw['timestamp'] - 39600, 0):
#                temp = curr_dict.get(raw['timestamp'] - 39600, 0)
#            elif curr_dict.get(raw['timestamp'] - 43200, 0):
#                temp = curr_dict.get(raw['timestamp'] - 43200, 0)
#            elif curr_dict.get(raw['timestamp'] - 46800, 0):
#                temp = curr_dict.get(raw['timestamp'] - 46800, 0)                  
#            elif curr_dict.get(raw['timestamp'] + 3600, 0):
#                temp = curr_dict.get(raw['timestamp'] + 3600, 0)
#            elif curr_dict.get(raw['timestamp'] + 7200, 0):
#                temp = curr_dict.get(raw['timestamp'] + 7200, 0)
#            elif curr_dict.get(raw['timestamp'] + 10800, 0):
#                temp = curr_dict.get(raw['timestamp'] + 10800, 0)
#            elif curr_dict.get(raw['timestamp'] + 14400, 0):
#                temp = curr_dict.get(raw['timestamp'] + 14400, 0)
#            elif curr_dict.get(raw['timestamp'] + 18000, 0):
#                temp = curr_dict.get(raw['timestamp'] + 18000, 0)
#            elif curr_dict.get(raw['timestamp'] + 21600, 0):
#                temp = curr_dict.get(raw['timestamp'] + 21600, 0)
#            elif curr_dict.get(raw['timestamp'] + 25200, 0):
#                temp = curr_dict.get(raw['timestamp'] + 25200, 0)
#            elif curr_dict.get(raw['timestamp'] + 28800, 0):
#                temp = curr_dict.get(raw['timestamp'] + 28800, 0)
#            elif curr_dict.get(raw['timestamp'] + 32400, 0):
#                temp = curr_dict.get(raw['timestamp'] + 32400, 0)
#            elif curr_dict.get(raw['timestamp'] + 36000, 0):
#                temp = curr_dict.get(raw['timestamp'] + 36000, 0)
#            elif curr_dict.get(raw['timestamp'] + 39600, 0):
#                temp = curr_dict.get(raw['timestamp'] + 39600, 0)
#            elif curr_dict.get(raw['timestamp'] + 43200, 0):
#                temp = curr_dict.get(raw['timestamp'] + 43200, 0)
#            elif curr_dict.get(raw['timestamp'] + 46800, 0):
#                temp = curr_dict.get(raw['timestamp'] + 46800, 0)                 
#    else:
#        temp = CURRENCY
#    return temp        

        

    
#    for curr in curr_to_count:
#        if curr != CURRENCY:
#            temp = pair_price_from_deals.get(curr + '_' + CURRENCY, 'no pair in dict')
#            if temp == 'no pair in dict':
#                print(curr + '_' + CURRENCY, temp)
#
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
            
        
#        temp = deals_df[deals_df['pair_name'] == pair]
#        temp = temp.groupby([temp['date'].dt.hour, 'day'])['price'].mean().unstack()        
#        temp.groupby([temp['date'].dt.hour, 'day'])['price'].mean().unstack().plot()
        