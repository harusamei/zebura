# import csv
# import pymysql
#
#
# # 从 CSV 文件中读取数据
# def read_data_from_csv(csv_file):
#     with open(csv_file, 'r', newline='',encoding="utf-8-sig") as file:
#         reader = csv.DictReader(file)
#         data = [row for row in reader]
#         for row in data:
#             # 检查字段是否为 'cpu_core_number'，并且其值可以转换为整数
#             int_fields = ['cpu_core_number',"disk_capacity",'max_memory_capacity','memory_capacity','memory_slot_number',"price","stock_number",'uid']
#             float_fields=['depth','height','width']
#             for field in int_fields:
#                 # 检查字段是否存在，并且其值可以转换为整数
#                 if field in row and row[field]!='':
#                     row[field] = int(row[field])
#                 else:
#                     row[field]=0
#             for flo in float_fields:
#                 if flo in row and row[flo]!='':
#                     row[flo]=float(row[flo])
#                 else:
#                     row[flo]=0.0
#     return data
#
#
# # 连接到 MySQL 数据库
# def connect_to_mysql(host, user, password, database):
#     return pymysql.connect(host=host, user=user, password=password, database=database,charset="utf8")
#
#
# # 创建表格
# def create_table(cursor):
#     # 创建表格的 SQL 语句，这里需要根据你的实际情况进行修改
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS products (
#     brand VARCHAR(255) DEFAULT "",
#     color VARCHAR(255) DEFAULT "",
#     cpu_brand VARCHAR(255) DEFAULT "",
#     cpu_core_number INT DEFAULT 0,
#     cpu_main_frequency VARCHAR(255) DEFAULT "",
#     depth DOUBLE DEFAULT 0.0,
#     disk_capacity INT DEFAULT 0,
#     disktype VARCHAR(255) DEFAULT "",
#     foldability VARCHAR(255) DEFAULT "",
#     goods_status VARCHAR(255) DEFAULT "",
#     gpu_brand VARCHAR(255) DEFAULT "",
#     gpu_series VARCHAR(255) DEFAULT "",
#     height DOUBLE DEFAULT 0.0,
#     intelligence VARCHAR(255) DEFAULT "",
#     max_memory_capacity INT DEFAULT 0,
#     memory_capacity INT DEFAULT 0,
#     memory_slot_number INT DEFAULT 0,
#     memory_type VARCHAR(255) DEFAULT "",
#     mtm_number VARCHAR(255) DEFAULT "",
#     os VARCHAR(255) DEFAULT "",
#     pre_installed_software VARCHAR(255) DEFAULT "",
#     price INT DEFAULT 0,
#     product_cate1 VARCHAR(255) DEFAULT "",
#     product_cate2 VARCHAR(255) DEFAULT "",
#     product_item_name VARCHAR(255) DEFAULT "",
#     product_name VARCHAR(255) DEFAULT "",
#     ram VARCHAR(255) DEFAULT "",
#     rom VARCHAR(255) DEFAULT "",
#     scene VARCHAR(255) DEFAULT "",
#     screen_size VARCHAR(255) DEFAULT "",
#     screen_type VARCHAR(255) DEFAULT "",
#     series VARCHAR(255) DEFAULT "",
#     service_description VARCHAR(255) DEFAULT "",
#     size VARCHAR(255) DEFAULT "",
#     stock_number INT DEFAULT 0,
#     target_audience VARCHAR(255) DEFAULT "",
#     time_to_market VARCHAR(255) DEFAULT "",
#     uid INT DEFAULT 0,
#     url VARCHAR(255) DEFAULT "",
#     video_card_type VARCHAR(255) DEFAULT "",
#     width DOUBLE DEFAULT 0.0)CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
#     """
#     cursor.execute(create_table_query)
#
#
# # 将数据导入到表格中
# def insert_data(cursor, data):
#     # 插入数据的 SQL 语句，这里需要根据你的实际情况进行修改
#     insert_query = """
#     INSERT INTO products (brand,color,cpu_brand,cpu_core_number,cpu_main_frequency,depth,
#     disk_capacity,disktype,foldability,goods_status,gpu_brand,gpu_series,height,
#     intelligence,max_memory_capacity,memory_capacity,memory_slot_number,memory_type,
#     mtm_number,os,pre_installed_software,price,product_cate1,product_cate2,product_item_name,
#     product_name,ram,rom,scene,screen_size,screen_type,series,service_description,size,
#     stock_number,target_audience,time_to_market,uid,url,video_card_type,width)
#     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     for row in data:
#         cursor.execute(insert_query, (row['brand'],row['color'],row['cpu_brand'],row['cpu_core_number'],row['cpu_main_frequency'],
#                                       row['depth'],row['disk_capacity'],row['disktype'],row['foldability'],row['goods_status'],row['gpu_brand']
#                                       ,row['gpu_series'],row['height'],row['intelligence'],row['max_memory_capacity'],row['memory_capacity']
#                                       ,row['memory_slot_number'],row['memory_type'],row['mtm_number'],row['os'],row['pre_installed_software'],
#                                       row['price'],row['product_cate1'],row['product_cate2'],row['product_item_name'],row['product_name'],
#                                       row['ram'],row['rom'],row['scene'],row['screen_size'],row['screen_type'],row['series'],row['service_description']
#                                       ,row['size'],row['stock_number'],row['target_audience'],row['time_to_market'],row['uid'],row['url'],row['video_card_type'],row['width']))
#
# # 主函数
# def main():
#     # CSV 文件路径
#     csv_file = './datasets/leproducts.csv'
#     # 数据库连接信息
#     host = 'localhost'
#     user = 'root'
#     password = '123456'
#     database = 'products'
#     # 读取数据
#     data = read_data_from_csv(csv_file)
#     # 连接到 MySQL 数据库
#     connection = connect_to_mysql(host, user, password, database)
#     cursor = connection.cursor()
#     # 创建表格
#     create_table(cursor)
#     # 将数据导入到表格中
#     insert_data(cursor, data)
#     # 提交更改并关闭连接
#     connection.commit()
#     connection.close()
# if __name__ == "__main__":
#     main()
import pymysql

conn = pymysql.connect(
    host='localhost',		# 主机名（或IP地址）
    port=3306,				# 端口号，默认为3306
    user='root',			# 用户名
    password='123456',	# 密码
    charset='utf8mb4'  		# 设置字符编码
)
print(conn.get_server_info())
cursor = conn.cursor()
conn.select_db("products")
cursor.execute('select * from products where price > 1000;')
answer= cursor.fetchall()
keys = [column[0] for column in cursor.description]

# Convert fetched rows to dictionaries with keys and values
result_dicts = []
for row in answer:
    result_dict = dict(zip(keys, row))
    result_dicts.append(result_dict)

# Print results
categoryList=[]
for result_dict in result_dicts:
    categoryList.append(result_dict)
print(categoryList)