import sys
import os
sys.path.insert(0, os.getcwd().lower())
import settings
from zebura_core.utils.csv_processor import pcsv
import pymysql
from zebura_core.knowledges.schema_loader import Loader


# Connect to MySQL
def connect():   
    cnx = pymysql.connect(
        host='localhost',		# 主机名（或IP地址）
        port=3306,				# 端口号，默认为3306
        user='root',			# 用户名
        password='zebura',	# 密码
        charset='utf8mb4'  		# 设置字符编码
    )
    return cnx
# Create the "it" database
def create_db(cnx, db_name):
    cursor = cnx.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.close()
    return cnx

def create_table(cnx, db_name, table_name, schema):
    cursor = cnx.cursor()
    cursor.execute(f"USE {db_name}")

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        )
    """)
    cursor.close()
    return cnx

def gen_schema(fields_csv):
    columns = pcsv().read_csv(fields_csv)
    txt_type = 'VARCHAR(255)'
    int_type = 'INT'
    decimal_type = 'DECIMAL(10, 2)'
    desc_type = 'TEXT'

    fields = []
    for key in columns[0].keys():
        field = key + ' '
        if columns[0].get(key) == 'int':
            field += int_type
        elif columns[0].get(key) == 'decimal':
            field += decimal_type
        else:
            field += txt_type
        fields.append(field)

    print(len(fields))
    return ', '.join(fields)


def load_data(cnx, db_name, table_name, csv_path):
    cursor = cnx.cursor()
    cursor.execute(f"USE {db_name}")
    csv_reader  = pcsv()
    data = csv_reader.read_csv(csv_path)
    for row in data:
        row = {k:v for k,v in row.items() if v is not None and v != ''}
        fields = ', '.join(row.keys())
        values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in row.values()])
        print(f"""INSERT INTO {table_name} ({fields})
            VALUES ({values})""")
        cursor.execute(f"""
            INSERT INTO {table_name} ({fields})
            VALUES ({values})
        """)
        
    cnx.commit()
    cursor.close()
    return cnx

# Example usage
if __name__ == '__main__':
    cnx = connect()
    create_db(cnx, 'it')
      
    base = os.getcwd()
    shc_path = os.path.join(base, 'datasets', 'fields.csv')
    schma = gen_schema(shc_path)
    create_table(cnx, 'it', 'products', schma)
    csv_path = os.path.join(base, 'datasets', 'leproducts.csv') 
    load_data(cnx, 'it', 'products', csv_path)
