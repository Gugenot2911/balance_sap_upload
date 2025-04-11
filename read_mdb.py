import zipfile
import tempfile
import pyodbc
import pandas as pd
import os

# Путь к ZIP-архиву, выбирается последний архив из папки
file_list = os.listdir('//corp.tele2.ru/cpfolders/STAT.CP.Reports/Weekly_HWInventory/Nokia/')
zip_path = '//corp.tele2.ru/cpfolders/STAT.CP.Reports/Weekly_HWInventory/Nokia/' + file_list[-1]
mdb_filename = file_list[-1][:-4]

# Открываем ZIP-архив. Сохранение временного файла
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    mdb_data = zip_ref.read(mdb_filename)

with tempfile.NamedTemporaryFile(delete=False, suffix='.mdb') as temp_mdb_file:
    temp_mdb_file.write(mdb_data)
    temp_mdb_path = temp_mdb_file.name
    
# Подключаемся к временному файлу MDB (Windows)
conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={temp_mdb_path};'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

#Получаем список таблиц
tables = [row.table_name for row in cursor.tables(tableType='TABLE')]

print("Список таблиц в базе данных:")
for table in tables:
    print(table)
    
# Имена файлов/столбцов и условие сортировки по региону
table_to_select = 'mdb'  
columns_to_select = ['Region', 'SiteName', 'inventoryUnitType'] 
filter_condition = "Region = 'NS'"  

# Проверяем, существует ли указанная таблица
if table_to_select in tables:
    columns_str = ', '.join(columns_to_select)

    # Выполняем запрос к таблице с фильтрацией
    query = f'SELECT {columns_str} FROM {table_to_select} WHERE {filter_condition}'
    df = pd.read_sql(query, conn)
    

     # Выводим DataFrame
    print(f'DataFrame из таблицы {table_to_select, file_list[-1]}:')
    print(df)
else:
    print(f'Таблица {table_to_select} не найдена в базе данных.')

# Закрываем соединение
cursor.close()
conn.close()

# Удаляем временный файл после использования
os.remove(temp_mdb_path)

# Очищаем оперативную память
del mdb_data
del temp_mdb_file




