import polars as pl
import os
import df_report

#
# path_akb_report = r'L:\Tech_Maintenance\Обслуживание сети\Замены АКБ\2025'
# os.chdir(path_akb_report.replace('\\','/'))
#
# # akb_list = pl.read_excel('Weekly_PWR_fromRDB_20250216.xlsx', sheet_name='Weekly_PWR_fromRDB_20250216', columns=['REGION_CODE2','master_site'])
# # akb_list = akb_list.filter(pl.col('REGION_CODE2') == 'NS')
#
# pl.Config.set_tbl_rows(100)
# pl.Config.set_tbl_width_chars(9999)
# pl.Config.set_fmt_str_lengths(100)

files = os.listdir('Reports')

class Balance:

    def __init__(self, site_name):
        self.site_name = site_name


    def get_balances_path(self, file_name = 'ОС') -> str:
        self.file_name = file_name
        '''
        :param file_name: ОС / ТМЦ кириллицей
        :return: путь к фалайм остатки (ОС), либо ТМЦ
        '''

        path = [item for item in os.listdir('//corp.tele2.ru/NSMRFolders/MR_LOGISTIC/Новосибирск/Остатки') if
                        item.startswith('2540_остатки_'+file_name+'_Kryon_')]
        # "правильная" сортировка по дате в имени файла (гггг.мм.дд):
        path = sorted(path, key=lambda x: x[28:-5] + x[25:-10] + x[22:-13])
        path = '//corp.tele2.ru/NSMRFolders/MR_LOGISTIC/Новосибирск/Остатки/' + path[-1]

        return path

    def refund(self, contractor:str='ОТС')-> pl.dataframe:

        storage = {
            "ОТС": 'KZ01|K026',
            "НТК": 'KZ02|K046',
            "Лаукар": 'KZ03|K018'
        }

        paths = ['//corp.tele2.ru/NSMRFolders/MR_LOGISTIC/Новосибирск/Возврат/IN 2025 Актуальный.xlsx']

        result_df = pl.DataFrame()

        for file in paths:
            pl_refund = pl.read_excel(file, columns=['ОС', 'Название основного средства', 'БС', 'Куда ','Код ПО',
                                                     'Кол-во', 'Комментарий логиста', '№ заявки'])
            pl_refund = pl_refund.filter(pl.col('Код ПО').str.contains(r'(?i)'+storage[contractor]))
            filtered_df = pl_refund.filter(pl.col('БС') == self.site_name)
            result_df = pl.concat([result_df, filtered_df])

        return result_df

    def sap_os(self, re_filter='') -> pl.dataframe:

        path = self.get_balances_path()

        pl_os = pl.read_excel(path, columns=[3,4,6,13,14])
        pl_os = pl_os.filter(pl.col('Название основного средства').str.contains(r'(?i)'+re_filter))
        pl_os = pl_os.rename({"Номер партии": "Партия"}) #приводим к еди
        pl_os = pl_os.filter(
            (pl.col('Субномер') == '0') &
            (pl.col('Сайт') == self.site_name)
        ).drop(('Субномер'))

        return pl_os

    def sap_tmc(self, re_filter='') -> pl.dataframe:

        path = self.get_balances_path(file_name='ТМЦ')
        os_file = self.sap_os()

        pl_tmc = pl.read_excel(path, columns=[3, 4, 8, 9, 10], infer_schema_length=0)
        pl_tmc = pl_tmc.filter((pl.col('Склад') == self.site_name))
        pl_tmc = pl_tmc.filter(pl.col('КрТекстМатериала').str.contains(r'(?i)' + re_filter))

        #ДОБАВИТЬ УСЛОВИЯ ДЛЯ НЕШТУЧНЫХ ПОЗИЦИЙ
        pl_tmc = (
            pl_tmc.with_columns(pl.all().repeat_by("Количество запаса в партии"))  # Повторяем значения
            .explode(pl.all())  # Разбиваем на отдельные строки
            .with_columns(pl.lit(1).alias("Количество запаса в партии"))  # Заменяем 'count' на 1
        )

        pl_tmc = pl_tmc.join(os_file, on='Партия', how='left').fill_null('ТМЦ') #извлечь номера ОС
        return pl_tmc

    def read_report(self) -> pl.dataframe:

        combined_dem = pl.DataFrame()
        combined_mon = pl.DataFrame()

        for file in files:
            df_report_m = pl.read_excel('Reports/' + file, sheet_name='монтаж', read_options={"header_row": 6})
            df_report_m = df_report_m.filter(pl.col('БС') != 'null')
            df_report_m = df_report_m.filter(pl.col('Материал (новое/БУ)') == 'новое')
            df_report_m = df_report_m.rename({'Наименование материалов':'КрТекстМатериала'})
            combined_mon = pl.concat([combined_mon, df_report_m], how='vertical')

            df_report_d = pl.read_excel('Reports/' + file, sheet_name='демонтаж', read_options={"header_row": 4})
            df_report_d = df_report_d.filter(pl.col('NS___') != 'null')
            df_report_d = df_report_d.filter(pl.col('Перемещение осуществляется на склад/сайт').str.contains(r'(?i)NS'))
            df_report_d = df_report_d.rename({'Системный номер объекта (Основное средство)':'Основное средство'})
            combined_dem = pl.concat([combined_dem, df_report_d], how='vertical')

        return combined_dem, combined_mon

    def merge_tmc(self) -> pl.dataframe:

        df_report = self.read_report()
        df_sap_tmc = self.sap_tmc()

        df_merge_tmc = df_sap_tmc.join(df_report[0], on='Основное средство', how='left')
        # df_merge_tmc = df_sap_tmc.join(df_report[1], on='КрТекстМатериала', how='left')
        df_merge_tmc.write_excel('tmc.xlsx')

        return df_merge_tmc




# balance = Balance(site_name='KZ01')
# # #
#
# print(balance.merge_tmc(), balance.read_report()[0])




