import pandas as pd
import polars as pl
import os
import re
from tqdm import tqdm
# import read_mdb

# Источники данных:/-----------------------------------------/

# Балансные остатки материалов
balances_sap = [item for item in os.listdir('//corp.tele2.ru/NSMRFolders/MR_LOGISTIC/Новосибирск/Остатки') if
                item.startswith('2540_остатки_ОС_Kryon_')]
balances_sap = sorted(balances_sap, key=lambda x: x[28:-5] + x[25:-10] + x[22:-13])  # "правильная" сортировка по дате в имени файла (гггг.мм.дд)
balances_sap = '//corp.tele2.ru/NSMRFolders/MR_LOGISTIC/Новосибирск/Остатки/' + balances_sap[-1]
hardware_ts = 'P:/Git/Balance-fact-pandas--main/Balance-fact-pandas--main/РРЛ_Шасси_платы.xlsx'
# hardware_bs = 'Активное оборудование БС W2410.xlsx'
# hardware_bs = read_mdb.df


# Работа с датафреймами /-----------------------------------------/


# Датафрейм - остатки
pl_os = pl.read_excel(balances_sap, columns=[3,4,6,14], infer_schema_length=0)
pl_os = pl_os.filter(pl.col('Сайт').str.contains(r'(?i)NS'))
pl_os = pl_os.with_columns(pl.col('Субномер'))


# pd_os = pd.read_excel(balances_sap, engine='openpyxl', usecols=[3, 4, 6, 14]).dropna(how='all')
# pd_os = pd_os.loc[pd_os['Субномер'] == 0]
# filter_regex = '(?=(NS))'
# pd_os = pd_os[(pd_os['Сайт'].str.contains(filter_regex, flags=re.IGNORECASE, regex=True, na=False))]
# # pd_os = pd_os[(pd_os['Сайт'] == filter_regex)]
# pd_os = pd_os.set_index('Сайт')
#
# # Датафрейм - активное оборудование ТС
# pd_ac_ts = pd.read_excel(hardware_ts, engine='openpyxl', usecols=[7, 8, 9]).dropna(how='all')
# pd_ac_ts['Шасси и плата'] = pd_ac_ts['Тип платы'].fillna(pd_ac_ts['Тип шасси'])
# pd_ac_ts = pd_ac_ts.set_index('Сайт')
#
# # Датафрейм - активное оборудование БС
# # pd_ac = pd.read_excel(hardware_bs, engine='openpyxl', usecols=[3, 33]).dropna(how='all')
# # pd_ac = pd_ac[(pd_ac['SiteName'].str.contains(filter_regex, flags=re.IGNORECASE, regex = True, na=False))]
#
# pd_ac = read_mdb.df
# pd_ac = pd_ac.drop(columns='Region')
# pd_ac = pd_ac.rename(columns={"SiteName": "Сайт"})  # приводим к едионобразию индексы датафреймов
# pd_ac = pd_ac.set_index('Сайт')


# Функция для создания листов учета
def result_list_ts(x):
    '''
    Выгрузка с использованием pivot_table
    '''

    list_report = []

    def create_list(list_hw_ts, list_os_ts, list_name, list_report):
        for i in tqdm(range(len(list_os_ts))):

            temp_df_os = pd_os[(
                pd_os['Название основного средства'].str.contains(r'(' + list_os_ts[i] + ')', flags=re.IGNORECASE,
                                                                  regex=True, na=False))]
            if len(temp_df_os) == 0:
                list_report.append(f'Значение не найдено (баланс) -  {list_hw_ts[i]}')
                print(list_report[i])
                continue

            temp_df_os = pd.pivot_table(data=temp_df_os, index='Сайт', aggfunc='count')['Основное средство']
            temp_df_ac = pd_ac_ts[(
                pd_ac_ts['Шасси и плата'].str.contains('(' + list_hw_ts[i] + ')', flags=re.IGNORECASE, regex=True,
                                                       na=False))]

            if len(temp_df_ac) == 0:
                list_report.append(f'Значение не найдено (факт) =  {list_hw_ts[i]}')
                print(list_report[i])
                continue

            temp_df_ac = pd.pivot_table(data=temp_df_ac, index='Сайт', aggfunc='count')['Шасси и плата']

            result_merge = pd.merge(temp_df_ac, temp_df_os, on='Сайт', how='outer', sort=True).fillna(0)
            result_merge['Расхождение'] = result_merge['Шасси и плата'] - result_merge['Основное средство']

            with pd.ExcelWriter(r'P://result bs/Result/Result_TS.xlsx', mode='a', if_sheet_exists='replace',
                                engine='openpyxl') as writer:
                result_merge.to_excel(writer, sheet_name=list_name[i])

            list_report.append(f'Выгружено успешно - {list_hw_ts[i]}')
            print(list_report[i])

    if x == 0:
        # Список активного оборудования храниться в отдельном файле - listhw_ts.xlsx
        list_ts = pd.read_excel('P:/Git/Balance-fact-pandas--main/Balance-fact-pandas--main/listhw_ts.xlsx',
                                engine='openpyxl')
        list_hw_ts = list_ts['hw list'].tolist()
        list_os_ts = list_ts['os list'].tolist()
        list_name = list_ts['name list'].tolist()
        create_list(list_hw_ts, list_os_ts, list_name, list_report)
    if x != 0:
        list_hw_ts = x
        create_list(list_hw_ts)

    print('\n'.join(list_report))

# Функция по выгрузке БС
def result_list_bs(x):
    '''
    Выгрузка с использованием pivot_table
    '''
    # result = pd.DataFrame(columns=['Сайт','inventoryUnitType', 'Основное средство', 'Название основного средства'])
    list_report = []

    def create_list(list_hw_bs):
        for i in tqdm(range(len(list_hw_bs))):

            temp_df_os = pd_os[(
                pd_os['Название основного средства'].str.contains(r'(' + list_hw_bs[i] + ')', flags=re.IGNORECASE,
                                                                  regex=True, na=False))]

            if len(temp_df_os) == 0:
                list_report.append(f'Значение не найдено (баланс) -  {list_hw_bs[i]}')
                print(list_report[i])
                continue

            temp_df_os = pd.pivot_table(data=temp_df_os, index='Сайт', aggfunc='count')['Основное средство']

            temp_df_ac = pd_ac[(
                pd_ac['inventoryUnitType'].str.contains('(' + list_hw_bs[i] + ')', flags=re.IGNORECASE, regex=True,
                                                        na=False))]

            if len(temp_df_ac) == 0:
                list_report.append(f'Значение не найдено (факт) =  {list_hw_bs[i]}')
                print(list_report[i])
                continue

            temp_df_ac = pd.pivot_table(data=temp_df_ac, index='Сайт', aggfunc='count')

            result_merge = pd.merge(temp_df_ac, temp_df_os, on='Сайт', how='outer', sort=True).fillna(0)
            result_merge['Расхождение'] = result_merge['inventoryUnitType'] - result_merge['Основное средство']

            with pd.ExcelWriter(r'P:/result bs/Result/Result_BS.xlsx', mode='a', if_sheet_exists='replace',
                                engine='openpyxl') as writer:
                result_merge.to_excel(writer, sheet_name=list_hw_bs[i])

            list_report.append(f'Выгружено успешно - {list_hw_bs[i]}')
            print(list_report[i])

    if x == 0:
        # Список активного оборудования храниться в отдельном файле - listhw_bs.xlsx
        list_hw_bs = pd.read_excel('P:/Git/Balance-fact-pandas--main/Balance-fact-pandas--main/listhw_bs.xlsx',
                                   engine='openpyxl')
        list_hw_bs = list_hw_bs['hw list'].tolist()
        create_list(list_hw_bs)

    if x != 0:
        list_hw_bs = x
        create_list(list_hw_bs)

    print('\n'.join(list_report))

print(pl_os)