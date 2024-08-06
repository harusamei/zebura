# 向mysql中写入数据，假设数据在csv文件中，schema在json文件中
# 主要为demo用，实际使用中，数据库应该已经具备
import sys
import os
sys.path.insert(0, os.getcwd().lower())
import settings
from zebura_core.utils.csv_processor import pcsv
import pymysql
from zebura_core.knowledges.schema_loader import Loader
import re

sch_loader = None
# Connect to MySQL
def connect():
    cnx = pymysql.connect(
        host='localhost',		# 主机名（或IP地址）
        port=3306,				# 端口号，默认为3306
        user='root',			# 用户名
        password='123456',	# 密码
        charset='utf8mb4'  		# 设置字符编码
    )
    return cnx
# Create the "it" database
def create_db(cnx, db_name):
    cursor = cnx.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    cursor.close()
    return cnx


def create_table(cnx, db_name, table_name, tb_struct,t_comment=''):
    cursor = cnx.cursor()
    cursor.execute(f"USE {db_name}")

    command = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {tb_struct}
        )COMMENT='{t_comment}';
    """
    cursor.execute(command)
    cnx.commit()  # 确保提交你的更改
    cursor.close()
    return cnx

# 生成构建DB的表结构，根据schema
def gen_struct(table_name):

    table_info = sch_loader.get_table_info(table_name)
    columns = table_info.get('columns')
    fields = []
    tmpl = '{field} {ty} {PRI} COMMENT "{comment}"'
    for column in columns:
        field = column['column_name']
        ty = column['type']
        if 'VIRTUAL_IN' in ty:      # 跳过自定义虚拟列,不在数据库中创建
            continue
        comment = f"{column.get('alias','')}; {column.get('desc','')}"
        if column.get('key','') == 'PRI':
            primary_type = 'PRIMARY KEY'
        else:
            primary_type = ''
        tStr = tmpl.format(field=field, ty=ty, PRI=primary_type, comment=comment)
        tStr= tStr.replace('COMMENT (; )','')
        fields.append(tStr)

    print(len(fields))
    return ',\n'.join(fields)


def refine_data(fields_ty, data):
    for k, v in data.items():
        if v is None or v == '':
            data[k] = None
        elif fields_ty[k] == 'int':
            cleaned_v = re.sub(r'[^0-9]', '', v)  # 替换非数字字符
            data[k] = int(cleaned_v) if cleaned_v else 0  # 防止空字符串转换
        elif fields_ty[k] == 'float':
            cleaned_v = re.sub(r'[^0-9.]', '', v)  # 保留点号，替换其他非数字字符
            data[k] = float(cleaned_v) if cleaned_v else 0  # 防止空字符串转换
        elif fields_ty[k] == 'varchar(255)':
            data[k] = str(v)[:254]
    return data


def insert_item(cursor, query, values):
    cursor.execute(query, values)
    cnx.commit()


def load_data(cnx, db_name, table_name, csv_path):
    cursor = cnx.cursor()
    cursor.execute(f"USE {db_name}")

    # 获取并打印表的列类型
    cursor.execute(f"DESCRIBE {table_name}")
    columns_info = cursor.fetchall()
    fields_ty = {}
    for column in columns_info:
        fields_ty[column[0]] =column[1]

    csv_reader  = pcsv()
    csv_path = os.path.join(os.getcwd(), 'dbaccess',csv_path)
    data = csv_reader.read_csv(csv_path)
    headers = data[0]
    fields = ', '.join(headers)
    vals ='%s, '*len(headers)
    vals = vals[:-2]
    #insert_query = f"INSERT INTO {table_name} (column1, column2, actual_price, discounted_price) VALUES (%s, %s, %s, %s)"
    insert_query = f"INSERT IGNORE INTO {table_name} ({fields}) VALUES ({vals})"

    for i, row in enumerate(data):
        row = refine_data(fields_ty, row)
        values = tuple(row.values())
        print(i, values[0])
        insert_item(cursor, insert_query, values)

    cnx.commit()
    cursor.close()
    return cnx


def load_schema(schema_path):
    global sch_loader
    base = os.getcwd()
    schema_path = os.path.join(base, 'dbaccess', schema_path)
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
    filePath = os.path.join(os.getcwd(), 'dbaccess/mysql/amazon_meta.json')
    load_schema(filePath)
    db_name = sch_loader.project
    create_db(cnx, db_name)
    tables = sch_loader.get_table_nameList()
    for table_name in tables:
        tb_schema = gen_struct(table_name)
        create_table(cnx, db_name, table_name, tb_schema)


# Example usage
if __name__ == '__main__':

    # usecase()
    # cnx = connect()
    # filePath = os.path.join(os.getcwd() , 'dbaccess/mysql/amazon.csv')
    # load_data(cnx, 'amazon', 'product', filePath)

    sql_queries = ["SELECT about_product FROM product WHERE rating > 4",
                   # "SELECT brand FROM product;",
                   # "SELECT target_audience, service_description FROM products;",
                   # "SELECT size, width, foldability FROM products;",
                   # "SELECT product_name, screen_size, screen_type FROM products;"
                   ]
    db_name = 'amazon'
    cnx = connect()
    for query in sql_queries[:]:
        print(f"Executing query: {query}")
        test_query(cnx, db_name, query)

