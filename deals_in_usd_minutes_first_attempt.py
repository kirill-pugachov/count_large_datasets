# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 10:44:03 2018

@author: tom
"""


import time
import datetime
import os
import csv
import pandas as pd
#import numpy as np


COMISSION = 0.002
CURRENCY = 'USD'
PATH_TO_DIRECTORY_DEALS = 'C:\\Users\\tom\\usd_input_output_operation\\Data\\Deals'
PATH_TO_DIRECTORY_RESULT = 'C:\\Users\\tom\\usd_input_output_operation\\Result\\Deals_in_USD'
PARTS_SIZE = 200000000
RESULT_FILE_NAME = 'currancy_to_USD_rates_september.csv'

outcome_list = [
    'dt',
    'trade_id',
    'trade_type',
    'user_sell',
    'user_buy',
    'pair_name',
    #    'quantity_in_USD',
    #    'amount_in_USD',
    'volume_USD',
    'price_USD',
    #    'profit_buy_amount_in_USD',
    #    'profit_buy_user_amount_in_USD',
    #    'profit_sell_amount_in_USD',
    #    'profit_sell_user_amount_in_USD',
    'profit_buy_amount_USD',
    'profit_buy_user_amount_USD',
    'profit_sell_amount_USD',
    'profit_sell_user_amount_USD']


def csv_to_df(path_to_file, path_to_dir):
    '''
    Считывает csv pandas dataframe
    '''
    df_csv = pd.read_csv(path_to_dir + '\\' + path_to_file, header=0, sep=',')
    return df_csv


def active_pair_list(df_pair):
    '''
    Получаем из фрейма входного файла
    список активных валютных пар
    т.е. валютных пар, по которым были сделки
    '''
    res = df_pair.pair_name.unique().tolist()
    return res


def get_minute_from_dt(row):
    '''
    Считаем минуту сделки во входном фрейме
    для расчета среднего в минуту
    '''
    if 'T' in row:
        minute = int(row.split('T')[1].split(':')[0]) * 60 + int(row.split('T')[1].split(':')[1])
    else:
        minute = int(row.split(' ')[1].split(':')[0]) * 60 + int(row.split(' ')[1].split(':')[1])
    return minute


def get_pair_price_dict_by_minutes(df):
    '''
    Датафрейм приходит за месяц
    Время считается по минутам
    Рассчитываем курсы по имеющимся в представленном датафрейме
    валютным парам.
    Полученный справочник курсов - возвращаем словарем
    пара-время:курс
    пара_0-время:курс
    пара_1-время:курс
    '''
    total_res = dict()
    for pair in df['pair_name'].unique():
        temp_df = df[df['pair_name'] == pair]
        temp_dict = dict()
        for day in temp_df['day'].unique():
            day_temp_df = temp_df[temp_df['day'] == day]
            temp_res = day_temp_df.groupby([day_temp_df['date'].dt.minute, 'minute'])['price'].mean().unstack()
            temp_res.fillna(0, inplace=True)
            for col in temp_res.columns:
                key = int(time.mktime(datetime.datetime.strptime(day, '%Y-%m-%d').timetuple())) + int(col) * 60
                value = temp_res[col].sum()
                if key in temp_dict:
                    temp_dict[key] = value
                else:
                    temp_dict[key] = value
        total_res[pair] = temp_dict
    return total_res


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


def total_price_dict_updater(total_price, month_price):
    '''
    Добавляем курсы за очередной месяц в глобальный словарь
    (он же справочник) валютных курсов
    '''
    for key_month in month_price:
        if key_month in total_price:
            total_price[key_month].update(month_price[key_month])
        else:
            total_price[key_month] = month_price[key_month]
    return total_price


def get_pair_price_dict(PATH_TO_DIRECTORY_DEALS):
    '''
    Получаем справочник (словарь курсов) из файлов сделок
    для прямой и обратной валютной пары с усреднением цены за минуту
    по каждой паре
    '''
    file_list = os.listdir(PATH_TO_DIRECTORY_DEALS)
#    file_list = ['deals_september_2018.csv'] #эта заглушка - ее нужно удалить
    total_pair_price = dict()
    for file in file_list:
        print(file)
        df = csv_to_df(file, PATH_TO_DIRECTORY_DEALS)
        df['date'] = pd.to_datetime(df['dt'], dayfirst=False, yearfirst=False)
        df['minute'] = df['dt'].apply(lambda x: get_minute_from_dt(x))
        month_pair_price = get_pair_price_dict_by_minutes(df)
        month_pair_price_reversed = reverse_curs_create(month_pair_price)
        month_pair_price.update(month_pair_price_reversed)
        total_pair_price = total_price_dict_updater(total_pair_price, month_pair_price)
    return total_pair_price


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
    если из валюты нет выхода в USD добавляем пару
    валюта_USD
    '''
    res = set()
    for curr in curr_list:
        if curr != CURRENCY:
            if (curr + '_' + CURRENCY) not in curr_dict:
                res.add((curr + '_' + CURRENCY))
    return res


def build_pair(curr, pair_price_from_deals, curr_deals):
    '''
    Строим валютные связки (пары валют вида ['DOGE_BTC', 'BTC_USD'])
    '''
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


def get_rate_by_pair_timestamp(pair, timestamp):
    '''
    По паре валют и времени получаем значение курса
    для этой пары
    если время больше максимального имеющегося в справочнике
    курсов валютных пар - отдаем значение курса максимального
    имеющегося времени
    если время меньше минимального имеющегося в справочнике
    курсов валютных пар - отдаем значение курса минимального
    имеющегося времени
    если время внутри имеющегося диапазона в справочнике
    курсов валютных пар, ищем значение времени первые слева и справа
    и берем среднее арифметическое для данных значение в справочнике
    курсов валютных пар
    '''
    if timestamp in pair_price_dict[pair]:
        temp = pair_price_dict[pair][timestamp]
    else:
        if timestamp > sorted(list(pair_price_dict[pair].keys()))[-1]:
            temp = pair_price_dict[pair][sorted(list(pair_price_dict[pair].keys()))[-1]]
        elif timestamp < sorted(list(pair_price_dict[pair].keys()))[0]:
            temp = pair_price_dict[pair][sorted(list(pair_price_dict[pair].keys()))[0]]
        elif timestamp < sorted(list(pair_price_dict[pair].keys()))[-1]:
            temp_left = 0
            temp_right = 0
            for left in range(0, -1000, -1):
                if timestamp + left * 60 in pair_price_dict[pair]:
                    temp_left = pair_price_dict[pair][timestamp + left * 60]
                    break
            for right in range(0, 1000):
                if timestamp + right * 60 in pair_price_dict[pair]:
                    temp_right = pair_price_dict[pair][timestamp + right * 60]
                    break
            if temp_left != 0 and temp_right != 0:
                temp = (temp_left + temp_right) / 2
            elif temp_left != 0 and temp_right == 0:
                temp = temp_left
            elif temp_left == 0 and temp_right != 0:
                temp = temp_right
            else:
                temp = 0

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
    '''
    Возвращает значение профита по пользователю
    в USD на основе курса по валютной паре состоящей из
    'profit_buy_currency'_USD из справочника курсов
    и значения профита в валюте 'profit_buy_currency'
    '''
    if raw['profit_buy_currency'] != 'USD':
        pair = raw['profit_buy_currency'] + '_' + 'USD'
        rate = get_rate_by_pair_timestamp(pair, raw['timestamp'])
    #    rate = pair_curr_rate_by_hours[pair][raw['timestamp']]
        amount = raw['profit_buy_user_amount'] * rate
    else:
        amount = raw['profit_buy_user_amount']
    return amount


def get_profit_sell_amount_in_USD(raw):
    '''
    Возвращает значение профита по пользователю
    в USD на основе курса по валютной паре состоящей из
    'profit_sell_currency'_USD из справочника курсов
    и значения профита в валюте 'profit_sell_currency'
    '''
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


def str_to_timestamp(raw):
    '''
    Возвращает по содержимому строки
    значение таймстемпа в секундах
    '''
    s = raw.split(' ')[0] + ' ' + raw.split(' ')[1].split(':')[0] + ':' + raw.split(' ')[1].split(':')[1]
    temp = int(time.mktime(datetime.datetime.strptime(s, '%Y-%m-%d %H:%M').timetuple()))
    return temp


def get_max_files_size(file_list):
    '''
    Определяет размеры файлов в директории
    Возвращает значение максимального размера
    для определения на сколько частей разбивать файлы
    для выгрузки в DOMO
    '''
    res_size_list = list()
    for file in file_list:
        res_size_list.append(os.path.getsize(PATH_TO_DIRECTORY_DEALS + '\\' + file))
    return max(res_size_list)


def get_volume_USD(raw):
    '''
    Считает объем в ЮСД
    по заданным условиям
    если нет ЮСД в curr1 или curr2
    усредняет значения по amount_in_USD
    и quantity_in_USD т.к. эти поля должны
    быть равными, но они разные из-за входных
    данных
    '''
    if raw['curr1'] == CURRENCY:
        volume = raw['quantity']
    elif raw['curr2'] == CURRENCY:
        volume = raw['amount']
    else:
        volume = (raw['amount_in_USD'] + raw['quantity_in_USD']) / 2
    return volume


def create_curr_dict(PATH_TO_DIRECTORY_DEALS):
    #составляем словарь курсов валют
    #валютная пара:{время:курс}
    pair_price_dict = get_pair_price_dict(PATH_TO_DIRECTORY_DEALS)

    #составляем список валют
    currencies_list = total_currencies_list(pair_price_dict)

    #определяем валютные пары по котором нужно рассчитать курс к USD она же CURRENCY
    pair_to_count_in_usd = curr_pair_to_count_usd(currencies_list, pair_price_dict)

    #определяем какими парами пользоваться для рассчета
    pathes_to_count_pair_rate = build_pair_path(pair_to_count_in_usd, pair_price_dict, currencies_list)

    #проводим рассчет курса
    #проверяем есть ли хотя бы одна валютная пара для расчета
    if check_pathes(pathes_to_count_pair_rate):
        #определяем по каким парам считать
        curr_path_to_count_in_usd = select_path_to_count_in_USD(pathes_to_count_pair_rate)

        for curr in curr_path_to_count_in_usd:
            temp_dict = dict()
            #курс первой пары расчета, к примеру LTC_BTC
            temp_0 = pair_price_dict[curr_path_to_count_in_usd[curr][0]]
            #курс второй пары расчета, к примеру BTC_USD
            temp_1 = pair_price_dict[curr_path_to_count_in_usd[curr][1]]
            #по временной метке первого курса находим курс второй пары расчта и считаем курс LTC_BTC * BTC_USD
            for timestamp in temp_0.keys():
                if timestamp in temp_1.keys():
                    temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp]
                else:
                    #если курс первой пары расчета меньше минимального минимального имеющегося курса второй
                    #пары по оси времени берем минимальный доступный курс второй пары
                    if timestamp < sorted(list(temp_1.keys()))[0]:
                        temp_dict[timestamp] = temp_0[timestamp] * temp_1[sorted(list(temp_1.keys()))[0]]
                    #если курс первой пары расчета больше максимального имеющегося курса второй
                    #пары по оси времени берем максимальный доступный курс второй пары
                    elif timestamp > sorted(list(temp_1.keys()))[-1]:
                        temp_dict[timestamp] = temp_0[timestamp] * temp_1[sorted(list(temp_1.keys()))[-1]]
                    else:
                    #если курс первой пары расчета между двумя значениями имеющегося курса второй
                    #пары по оси времени берем левый и правый доступный курс второй пары и считаем средний курс
                    #если не находим левый за 1000 минут берем только правый
                    #если не находим правый за 1000 минут берем только левый
                    #если не находим ни левый, ни правый значение курса приравниваем к 0
                        temp_left = 0
                        temp_right = 0
                        for left in range(0, -1000, -1):
                            if (timestamp + left * 60) in temp_1.keys():
                                temp_left = temp_1[(timestamp + left * 60)]
                                print(temp_left)
                                break
                        for right in range(0, 1000):
                            if (timestamp + right * 60) in temp_1.keys():
                                temp_right = temp_1[(timestamp + right * 60)]
                                print(temp_right)
                                break
                        print('\n', temp_0[timestamp] * ((temp_left + temp_right) / 2), temp_0[timestamp], temp_left, temp_right, '\n')
                        if temp_left != 0 and temp_right != 0:
                            temp_dict[timestamp] = temp_0[timestamp] * ((temp_left + temp_right) / 2)
                        elif temp_left != 0 and temp_right == 0:
                            temp_dict[timestamp] = temp_0[timestamp] * temp_left
                        elif temp_left == 0 and temp_right != 0:
                            temp_dict[timestamp] = temp_0[timestamp] * temp_right
                        else:
                            temp_dict[timestamp] = 0
            #добавляем в словарь валют новые валюты и курсы
            pair_price_dict[curr] = temp_dict

    return pair_price_dict

if __name__ == '__main__':

    pair_price_dict = create_curr_dict(PATH_TO_DIRECTORY_DEALS)

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
        df['volume_USD'] = df.apply(get_volume_USD, axis=1)
        df['profit_buy_amount_USD'] = df['volume_USD'] * COMISSION
        df['profit_buy_user_amount_USD'] = df['volume_USD'] * (1 - COMISSION)
        df['profit_sell_amount_USD'] = df['volume_USD'] * COMISSION
        df['profit_sell_user_amount_USD'] = df['volume_USD'] * (1 - COMISSION)
        df['price_USD'] = df['volume_USD'] / df['quantity']

        short_df = df[outcome_list]
        short_df.to_csv(PATH_TO_DIRECTORY_RESULT + '\\' +'Deals_USD_short_' + file, index=False)

    for pair_key in pair_price_dict:
#        if pair_key.split('_')[1] == 'USD':
        with open(PATH_TO_DIRECTORY_RESULT + '\\' + RESULT_FILE_NAME, 'w', newline='') as file_large:
            csv_writer = csv.writer(file_large, quoting=csv.QUOTE_NONNUMERIC)
            for time_key in pair_price_dict[pair_key]:
                date_human = datetime.datetime.utcfromtimestamp(time_key).strftime('%m/%d/%Y %H:%M:%S')
                csv_writer.writerow([pair_key, date_human, pair_price_dict[pair_key][time_key]])








#    pair_price_dict = get_pair_price_dict(PATH_TO_DIRECTORY_DEALS)
#
#    currencies_list = total_currencies_list(pair_price_dict)
#
#    pair_to_count_in_usd = curr_pair_to_count_usd(currencies_list, pair_price_dict)
#
#    pathes_to_count_pair_rate = build_pair_path(pair_to_count_in_usd, pair_price_dict, currencies_list)
#
#    if check_pathes(pathes_to_count_pair_rate):
#        curr_path_to_count_in_usd = select_path_to_count_in_USD(pathes_to_count_pair_rate)
#
#        for curr in curr_path_to_count_in_usd:
#            temp_dict = dict()
#            temp_0 = pair_price_dict[curr_path_to_count_in_usd[curr][0]]
#            temp_1 = pair_price_dict[curr_path_to_count_in_usd[curr][1]]
#            for timestamp in temp_0.keys():
#                if timestamp in temp_1.keys():
#                    temp_dict[timestamp] = temp_0[timestamp] * temp_1[timestamp]
#                else:
#                    if timestamp < sorted(list(temp_1.keys()))[0]:
#                        temp_dict[timestamp] = temp_0[timestamp] * temp_1[sorted(list(temp_1.keys()))[0]]
#                    elif timestamp > sorted(list(temp_1.keys()))[-1]:
#                        temp_dict[timestamp] = temp_0[timestamp] * temp_1[sorted(list(temp_1.keys()))[-1]]
#                    else:
#                        temp_left = 0
#                        temp_right = 0
#                        for left in range(0,-1000, -1):
#                            if (timestamp + left * 60) in temp_1.keys():
#                                temp_left = temp_1[(timestamp + left * 60)]
#                                print(temp_left)
#                                break
#                        for right in range(0,1000):
#                            if (timestamp + right * 60) in temp_1.keys():
#                                temp_right = temp_1[(timestamp + right * 60)]
#                                print(temp_right)
#                                break
#                        print('\n', temp_0[timestamp] * ((temp_left + temp_right) / 2), temp_0[timestamp], temp_left, temp_right)
#                        if temp_left != 0 and temp_right != 0:
#                            temp_dict[timestamp] = temp_0[timestamp] * ((temp_left + temp_right) / 2)
#                        elif temp_left != 0 and temp_right == 0:
#                            temp_dict[timestamp] = temp_0[timestamp] * temp_left
#                        elif temp_left == 0 and temp_right != 0:
#                            temp_dict[timestamp] = temp_0[timestamp] * temp_right
#                        else:
#                            temp_dict[timestamp] = 0
#
#            pair_price_dict[curr] = temp_dict


#        short_df = df[outcome_list]
#
#        parts_number = get_max_files_size(file_list) // PARTS_SIZE
#
#        frame_list = np.array_split(short_df, parts_number)
#
#        size_control = 0
#        index = 0
#        for frame in frame_list:
#            frame.to_csv(PATH_TO_DIRECTORY_RESULT + '\\' +'Deals_USD_short_' + str(index) + '_' + file, index=False)
#            size_control += frame.shape[0]
#            index += 1
#
#        if size_control == short_df.shape[0]:
#            print(file, ' processing finished')
#            print(size_control, df.shape[0], 'sizes is Ok')
#        else:
#            print(file, ' processing finished')
#            print('Alarm! ' * 3)
#            print(size_control, df.shape[0], 'sizes is wrong')

















#    file_list = os.listdir(PATH_TO_DIRECTORY_DEALS)
#    total_pair_price = dict()
#    for file in file_list:
#        print(file)
#        df = csv_to_df(file, PATH_TO_DIRECTORY_DEALS)
#        df['date'] = pd.to_datetime(df['dt'], dayfirst=False, yearfirst=False)
#        df['minute'] = df['dt'].apply(lambda x: get_minute_from_dt(x))
#        month_pair_price = get_pair_price_dict_by_minutes(df)
#        month_pair_price_reversed = reverse_curs_create(month_pair_price)
#        month_pair_price.update(month_pair_price_reversed)
#        total_pair_price = total_price_dict_updater(total_pair_price, month_pair_price)
        