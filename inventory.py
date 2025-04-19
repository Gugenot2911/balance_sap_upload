import polars as pl
from balance import Balance
import read_mdb
import os
import re

def get_balances_path(file_name='ОС') -> str:

    '''
    :param file_name: ОС / ТМЦ кириллицей
    :return: путь к фалайм остатки (ОС), либо ТМЦ
    '''

    path = [item for item in os.listdir('//corp.tele2.ru/NSMRFolders/MR_LOGISTIC/Новосибирск/Остатки') if
            item.startswith('2540_остатки_' + file_name + '_Kryon_')]
    # "правильная" сортировка по дате в имени файла (гггг.мм.дд):
    path = sorted(path, key=lambda x: x[28:-5] + x[25:-10] + x[22:-13])
    path = '//corp.tele2.ru/NSMRFolders/MR_LOGISTIC/Новосибирск/Остатки/' + path[-1]

    return path

def counting_elements_sap():

    '''

    :return: pivot table sap position
    '''
    # Загрузка данных
    df = pl.read_excel(get_balances_path(), columns=[3, 4, 6, 13, 14]).filter(
        (pl.col('Субномер') == '0')).drop('Субномер')

    df = df.filter(pl.col('Сайт').str.contains('NS'))

    hw_list = pl.read_excel('inventory/listhw_bs.xlsx')['hw list'].to_list()
    pattern = "|".join([re.escape(item) for item in hw_list])

    # Добавляем столбец с найденным соответствием
    df = df.with_columns(
        pl.col('Название основного средства')
        .str.extract(r'(?i)(' + pattern + ')', group_index=1)
        .alias("Оборудование")
    )
    df_test = df.select(pl.col('Сайт'), pl.col('Оборудование'))

    df_test = (
        df_test.group_by(["Сайт", "Оборудование"])
        .agg(pl.len().alias("Количество"))
    )
    df_test = df_test.filter(pl.col("Оборудование").is_not_null())
    df_test.write_excel('temp/sap_'+ get_balances_path()[-15:-5] + '.xlsx')

    return df_test


# print(counting_elements_sap())


def merge_basestation():

    df_sap = pl.read_excel('temp/sap_17.04.2025.xlsx')
    df_hw = pl.read_excel('temp/hwBSS_Nokia_W2515.xlsx')

    merged = df_hw.join(
        df_sap,
        on=["Сайт", "Оборудование"],
        how="outer",
        suffix="_hw"
    )

    return merged


# print(merge_basestation())