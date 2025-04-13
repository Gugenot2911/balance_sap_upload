import polars as pl
import os
from openpyxl import load_workbook


pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_width_chars(9999)
pl.Config.set_fmt_str_lengths(100)

files = os.listdir('Reports')

def write_report_demontage(data:list|dict):

    #демонтаж
    wb = load_workbook("Reports/template.xlsx")
    ws = wb['демонтаж']

    start_row = 6

    for i in range(len(data)):
        if (ws[f"A{start_row}"].value is None or ws[f"A{start_row}"].value == ''):
            print(start_row)
            ws[f"A{start_row}"].value = data[i]["sap"]
            ws[f"B{start_row}"].value = data[i]["name"]

            try: #заглушка
                ws[f"C{start_row}"].value = data[i]["baseStation"]
            except:
                ws[f"C{start_row}"].value = 'не указано'

            ws[f"D{start_row}"].value = data[i]["destination"]
            ws[f"E{start_row}"].value = data[i]["type"]

            start_row += 1
        else:
            start_row += 1

    ws.insert_rows(start_row + 1)  # вставляет строку **после** текущей
    wb.save("v1_template.xlsx")

def write_report_montage(data:list|dict):

    wb = load_workbook("Reports/template.xlsx")
    ws = wb['монтаж']

    start_row_montage = 9
    for i in range(len(data)):
        if data[i]['type'] == 'montage':
            if (ws[f"A{start_row_montage}"].value is None or ws[f"A{start_row_montage}"].value == ''):
                ws[f"A{start_row_montage}"].value = data[i]["name"]
                ws[f"B{start_row_montage}"].value = 'монтаж'

                try:  # заглушка
                    ws[f"C{start_row_montage}"].value = data[i]["baseStation"]
                except:
                    ws[f"C{start_row_montage}"].value = 'не указано'

                ws[f"D{start_row_montage}"].value = data[i]["count"]

                if data[i]["sap"] == 'ТМЦ':
                    ws[f"E{start_row_montage}"].value = 'новое'
                else:
                    ws[f"E{start_row_montage}"].value = 'б/у'

                start_row_montage += 1
            else:
                start_row_montage += 1
        else:
            continue
    ws.insert_rows(start_row_montage + 1)  # вставляет строку **после** текущей
    wb.save("v1_template.xlsx")

# def new_report():

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

    write_report_demontage(data=rows)

    # print(rows)


    # return rows


def new_report(filename: str) -> pl.dataframe:
    if filename is files:
        print(f' имя отчета: {filename} уже существует')
        #открыть текущий отчет

        df_report = pl.read_excel(filename, )

    else:
        #создать новый пустой отчет (2 листа: монтаж, демонтаж)
        # schema_d = {
        #     'Наименование материалов':pl.Utf8,
        #     'Вид работ':pl.Utf8,
        #     'БС':pl.Utf8,
        #     'Кол-во (шт./м.)':pl.Int8,
        #     'Материал (новое/БУ)':pl.Utf8,
        #     'СПП-элемент':pl.Utf8,
        #     '№ MIGO':pl.Utf8
        # }
        # df_report = pl.DataFrame(schema=schema_d)



        return

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
write_report_montage(data=data)
# print(data[0]['type'])