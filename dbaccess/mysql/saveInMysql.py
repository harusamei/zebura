import sys
import os
sys.path.insert(0, os.getcwd().lower())
import settings
from zebura_core.utils.csv_processor import pcsv
import pymysql
from zebura_core.knowledges.schema_loader import Loader

sch_loader = None
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

def gen_schema(table_name):
    
    txt_type = 'VARCHAR(255)'
    int_type = 'INT'
    decimal_type = 'DECIMAL(10, 2)'
    desc_type = 'TEXT'
    date_type = 'DATE'
    primary_type = 'PRIMARY KEY'

    primary_fields = ['uid']
    decimal_fields = ['price']
    date_fields = ['time_to_market']
    int_fields = ['cpu_core_number','memory_slot_number']
    table_info = sch_loader.get_table_info(table_name)
    columns = table_info.get('columns')
    fields = []
    for column in columns:
        field = column['column_name']
        if field in decimal_fields:
            ty = decimal_type
        elif field in date_fields:
            ty = date_type
        elif field in int_fields:
            ty = int_type
        else:
            ty =  txt_type
        fields.append(field+' '+ty)
        if field in primary_fields:
            fields.append(primary_type+'('+field+')')

    print(len(fields))
    return ', '.join(fields)

def load_data(cnx, db_name, table_name, csv_path):
    cursor = cnx.cursor()
    cursor.execute(f"USE {db_name}")
    csv_reader  = pcsv()
    csv_path = os.path.join(os.getcwd(), 'dbaccess',csv_path)
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

def load_schema(schema_path):
    global sch_loader
    base = os.getcwd()
    schema_path = os.path.join(base, 'dbaccess',schema_path)
    sch_loader = Loader(schema_path)
    return sch_loader

def show_table_schema(cnx, table_name):
    cursor = cnx.cursor()
    show_table_query = f"SHOW CREATE TABLE {table_name}"
    cursor.execute(show_table_query)
    result = cursor.fetchone()
    print(f"Table Schema for {table_name}:")
    print(result[1])  # The second element of the result is the table schema

def test_query(cnx, db_name, query):
    cursor = cnx.cursor()
    cursor.execute(f"USE {db_name}")
    cursor.execute(query)
    result = cursor.fetchall()
    print(result)
    cursor.close()

def usecase():
    cnx = connect()
    load_schema('mysql/ikura_meta.json')
    db_name = sch_loader.project
    create_db(cnx, db_name)
    tables = sch_loader.get_table_nameList()
    for table_name in tables:
        tb_schema = gen_schema(table_name)
        create_table(cnx, db_name, table_name, tb_schema)
    
    load_data(cnx, db_name, 'products', 'mysql/leproducts.csv')

# Example usage
if __name__ == '__main__':

    sql_queries =[  "SELECT product_name, disk_capacity FROM products WHERE disk_capacity > '500GB'",
                    "SELECT brand FROM products;",
                  "SELECT target_audience, service_description FROM products;",
                  "SELECT size, width, foldability FROM products;",
                "SELECT product_name, screen_size, screen_type FROM products;"  
    ]
    db_name ='ikura'
    cnx = connect()
    for query in sql_queries:
        print(f"Executing query: {query}")
        test_query(cnx, db_name,query)
   
