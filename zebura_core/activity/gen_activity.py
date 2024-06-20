# 确定当前query的activity
# 一个NL2SQL转换的SQL 可能需要转换为几条SQL
import sys
import os
sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from zebura_core.knowledges.schema_loader import Loader

import pymysql
import logging
import re
class GenActivity:
    def __init__(self):

        self.sch_loader = Loader(z_config['Training','db_schema'])

        self.db_name = z_config['Training','project_code']
        db_type = z_config['TrainingDB','db_type']
        self.cnx = self.connect(db_type)
        if self.cnx is None:
            raise ValueError("Database connection failed")
        
        logging.info("GenActivity init done")

    def gen_activity(self, slots):
        # 1. 生成activity
        activity = {}
        activity['activity'] = 'query'
        activity['table_name'] = slots['from']
        activity['columns'] = slots['columns']
        activity['conditions'] = slots['conditions']
        return activity
    
    def gen_db_structures(self):
        # 生成db schema, 默认文件为 项目名_struct.txt
        cursor = self.cnx.cursor()
        cursor.execute(f"use {self.db_name}")
        cursor.execute("SHOW TABLES")
        tables = [table['Tables_in_' + self.db_name] for table in cursor.fetchall()]
        dbstruc = {}
        columns = []
        create_table= {}
        for table_name in tables:
            cursor.execute(f"show create table {table_name}")
            columns = cursor.fetchall()
            create_table[table_name] = columns[0]['Create Table']
            tDict = self.sch_loader.get_table_info(table_name)
            dbstruc[table_name] = {"desc":tDict.get('desc',''), "columns":[]}
            cols = self.sch_loader.get_all_columns(table_name)
            temDict = {}
            for col in cols:
                temDict[col['column_name']] = col
            dbstruc[table_name]['cols'] = temDict

        #{'Field': 'cpu_main_frequency', 'Type': 'varchar(255)', 'Null': 'YES', 'Key': '', 'Default': None, 'Extra': ''}
        tmp = "{fname}, {ftype}, {primary}, {foreign}, COMMENT({alias})"
        tmp_primary = "PRIMARY KEY (`{fname}`)"
        tmp_foreign = "FOREIGN KEY (`{fname}`) REFERENCES"
        for table_name in tables:
            
            cursor.execute(f"DESC {table_name}")
            cols = cursor.fetchall()
            for col in cols:
                col_desc =''
                fname = col['Field']
                ftype = col['Type']
                primary = ""
                foreign = ""
                tstr = tmp_primary.format(fname=fname)
                if tstr in create_table[table_name]:
                    primary = "PRIMARY KEY"
                tstr = tmp_foreign.format(fname=fname)
                if tstr in create_table[table_name]:
                    foreign = "FOREIGN KEY"
                    index = create_table[table_name].index(tstr)
                    match = re.search(r' REFERENCES (.*)\n', create_table[table_name][index:])
                    if match:
                        foreign += " REFERENCES "+match.group(1)
                alias = ""   
                temDict = dbstruc[table_name]['cols'][fname]
                alias =f"{temDict.get('name','')}, {temDict.get('alias','')},{temDict.get('name_zh','')},{temDict.get('alias_zh','')}"
                
                col_desc = tmp.format(fname=fname, ftype=ftype, primary=primary, foreign=foreign, alias=alias)
                col_desc = re.sub(r'(, )+',', ', col_desc)
                col_desc = col_desc.replace('COMMENT(,', 'COMMENT(')
                col_desc = col_desc.replace(',,)', ' )')
                col_desc = col_desc.replace('COMMENT(  )', '')

                dbstruc[table_name]['columns'].append(col_desc)
        cursor.close()
        return dbstruc

    @staticmethod
    def out_db_structures(filename,dbstruc):
        
        with open(filename, 'w') as f:
            for table_name in dbstruc.keys():
                f.write(f"Table: {table_name}\nPurpose: {dbstruc[table_name]['desc']}\n")
                f.write("Fields:\n")
                for col in dbstruc[table_name]['columns']:
                    f.write(f"{col}\n")
                f.write("\n")
        print(f"DB structures are saved to {filename}")

    @staticmethod
    def connect(type='mysql'):

        zoneName = 'TrainingDB'

        host = z_config[zoneName,'host']
        port = int(z_config[zoneName,'port']) 
        user = z_config[zoneName,'user']
        pwd  = z_config[zoneName,'pwd']

        if type == 'mysql':
            cnx = pymysql.connect(
                host= host,		        # 主机名（或IP地址）
                port= port,		        # 端口号，默认为3306
                user= user,		        # 用户名
                password= pwd,	        # 密码
                charset='utf8mb4',  		            # 设置字符编码
                cursorclass=pymysql.cursors.DictCursor  # 返回字典类型的游标
            )
            return cnx
        else:
            raise ValueError(f"Error: {type} not supported")
        
if __name__ == "__main__":
    gentor = GenActivity()
    db_stru =gentor.gen_db_structures()
    gentor.out_db_structures('ikura_struct.txt',db_stru)