import polars as pl
import os
from openpyxl import load_workbook
from xlsxwriter import Workbook

pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_width_chars(9999)
pl.Config.set_fmt_str_lengths(100)

files = os.listdir('Reports')


def write_report_demontage(data: list | dict, workbook: Workbook):
    ws = workbook['демонтаж']
    start_row = 6

    # Если data - словарь, преобразуем в список из одного элемента
    if isinstance(data, dict):
        data = [data]

    for item in data:
        # Ищем первую полностью пустую строку
        while ws[f'A{start_row}'].value not in (None, ""):
            print(f"Строка {start_row} занята: {ws[f'A{start_row}'].value}")
            start_row += 1

        # Заполняем данные
        ws[f"A{start_row}"].value = item.get("sap")
        ws[f"B{start_row}"].value = item.get("name")
        ws[f"C{start_row}"].value = item.get("baseStation", "не указано")  # Используем get с default
        ws[f"D{start_row}"].value = item.get("destination")
        ws[f"E{start_row}"].value = item.get("type")

        print(f'Демонтаж внесен в строку {start_row}')
        start_row += 1

    # Вставляем пустую строку после последней записи
    ws.insert_rows(start_row)
    print(f"Добавлена пустая строка {start_row}")


def write_report_montage(data: list | dict, workbook: Workbook):
    ws = workbook['монтаж']
    start_row = 9

    # Унифицируем входные данные (работаем с list)
    if isinstance(data, dict):
        data = [data]

    for item in data:
        # Пропускаем записи не типа 'montage'
        if item.get('type') != 'montage':
            continue

        # Ищем первую пустую строку
        while ws[f'A{start_row}'].value not in (None, ""):
            start_row += 1

        # Заполняем данные
        ws[f"A{start_row}"].value = item.get("name")
        ws[f"B{start_row}"].value = 'монтаж'
        ws[f"C{start_row}"].value = item.get("baseStation", "не указано")
        ws[f"D{start_row}"].value = item.get("count")
        ws[f"E{start_row}"].value = 'новое' if item.get("sap") == 'ТМЦ' else 'б/у'

        print(f'Монтаж внесен в строку {start_row}')
        start_row += 1

    # Вставляем пустую строку после последней записи
    if len(data) > 0:
        ws.insert_rows(start_row)
        print(f"Добавлена пустая строка {start_row}")


def new_report(data:str|dict, file_name = "v1_template.xlsx"):
    '''

    :param data: словарь значений для вставки в СИМ/демонтаж (POST)
    :param path: путь к файлу СИМ/демонтаж (по умолчанию открывается пустой шаблон)
    :return: сохранение в excel
    '''

    os.chdir('Reports')
    wb = load_workbook(file_name)
    write_report_montage(data=data, workbook=wb)
    write_report_demontage(data=data, workbook=wb)
    wb.save("v1_template.xlsx")


def add_items(data:dict):

    rows = []

    for item in data['items']:
        row = {
            'id': item['id'],
            'type': item['type'],
            'destination': item.get('destination', None),  # Используем get на случай отсутствия ключа
            **item['data']  # Распаковываем содержимое data в тот же словарь
        }
        rows.append(row)

    new_report(data=rows)

    # print(rows)


    # return rows

def read_report():

    combined_dem = pl.DataFrame()
    combined_mon = pl.DataFrame()

    for file in files:

        df_report_m = pl.read_excel('Reports/' + file, sheet_name='монтаж', read_options={"header_row": 6})
        df_report_m = df_report_m.filter(pl.col('БС') != 'null')
        df_report_m = df_report_m.filter(pl.col('Материал (новое/БУ)') == 'новое')
        combined_mon = pl.concat([combined_mon, df_report_m], how='vertical')

        df_report_d = pl.read_excel('Reports/' + file, sheet_name='демонтаж', read_options={"header_row": 4})
        df_report_d = df_report_d.filter(pl.col('NS___') != 'null')
        df_report_d = df_report_d.filter(pl.col('Перемещение осуществляется на склад/сайт').str.contains(r'(?i)NS'))
        combined_dem = pl.concat([combined_dem, df_report_d], how='vertical')

    return combined_dem, combined_mon

data =[{'id': '140000071873-34', 'type': 'demontage', 'destination': 'Не выбрано', 'name': 'Приемопередающий модуль FRMF 6TX800 360W', 'count': 1, 'baseStation': 'NS001588', 'sap': '140000071873'}, {'id': 'P-T221001.54.9996-639', 'type': 'montage', 'destination': 'KZ01', 'name': 'Приемопередающий модуль FRGX RFM 3 2100', 'sppElement': 'P-T221001.54.9996', 'count': 1, 'warehouse': 'KZ01', 'party': 'Z000104899', 'sap': '140000031564'}]
new_report(data=data)
# print(data[0]['type'])