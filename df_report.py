import polars as pl
import os
import xlsxwriter

pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_width_chars(9999)
pl.Config.set_fmt_str_lengths(100)

files = os.listdir('Reports')

def add_items(data:dict) -> pl.dataframe:
    # df_report = pl.from_dicts(data, schema=["items"])

    rows = []
    for item in data['items']:
        row = {
            'id': item['id'],
            'type': item['type'],
            'destination': item.get('destination', None),  # Используем get на случай отсутствия ключа
            **item['data']  # Распаковываем содержимое data в тот же словарь
        }
        rows.append(row)

    # Создаем DataFrame
    df_report = pl.DataFrame(rows)

    df_report.write_excel('report.xlsx', autofit=True)

    return df_report


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
#
# print(read_report())
