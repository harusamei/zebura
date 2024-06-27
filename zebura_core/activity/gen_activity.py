# 根据query和生成的SQL确定查询DB的activity
# 一个NL2SQL转换的SQL 需要check 表名，列名，值，条件， revise SQL
# 可能需要转换为几条SQL
############################################
import sys
import os
sys.path.insert(0, os.getcwd().lower())
import asyncio

from settings import z_config
from zebura_core.knowledges.schema_loader import Loader
from zebura_core.query_parser.schema_linker import Sch_linking
from zebura_core.query_parser.extractor import Extractor
from server.msg_maker import make_a_log

import pymysql
import logging
import re
import datetime
# connect sql server
def conn_srv(host, port, user, pwd, type='mysql'):
    try:
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
            logging.error(f"Error: {type} not supported")
            return None
    except Exception as e:
        logging.error(f"connect_sql() error: {e}")
        return None
    
class CheckSQL:

    def __init__(self):

        zoneName = 'TrainingDB'
        db_type = z_config[zoneName,'db_type']
        host = z_config[zoneName,'host']
        port = int(z_config[zoneName,'port']) 
        user = z_config[zoneName,'user']
        pwd  = z_config[zoneName,'pwd']

        self.cnx = conn_srv(host, port, user, pwd, db_type)
        if self.cnx is None:
            raise ValueError("Database connection failed")
        
        cwd = os.getcwd()
        name = z_config['Training','db_schema']  # 'training\it\products_schema.json'
        self.sl = Sch_linking(os.path.join(cwd, name))

        self.te = Extractor()

        self.db_name = z_config['Training','project_code']
        self.db_Info = self.get_dbInfo(self.db_name)


    # 主功能
    def check_sql(self,sql):

        slots = self.te.extract(sql)

        all_checks ={ 'status': 'succ', 'msg':'correct SQL',
                      'table':{}, 'fields':{}, 'conds':{}
                    }
        if slots is None:
            all_checks['status'] = 'failed'
            all_checks['msg'] = 'SQL is not parsed'
            return all_checks
       
        # 表名 mapping
        table_check ={'status': 'succ', 'items':[]}
        table_name = self.te.get_table_name(slots)
        linked = self.sl.link_table(table_name)
        if linked != table_name:
            table_check['status'] = 'failed'
            table_check['items'].append((False,table_name,linked))
        else:
            table_check['items'].append((True,table_name,linked))
        
        # 列名 mapping
        table_name = linked         # 修正后的表名
        field_names = self.te.get_fields(slots)
        fields_check ={'status': 'succ', 'items':[]}
        for field in field_names:
            t1, f1 = self.sl.link_field(field)
            if f1 != field or t1 != table_name:
                fields_check['status'] = 'failed'
                fields_check['items'].append((False, field,f'{t1}.{f1}'))
            else:
                fields_check['items'].append((True, field,f'{t1}.{f1}'))
  
        # 条件检查
        conds_check = self.check_cond(slots,fields_check)
        all_checks['conds'] = conds_check
        all_checks['table'] = table_check
        all_checks['fields'] = fields_check

        if 'failed' in [table_check['status'], fields_check['status'], conds_check['status']]:
            all_checks['status'] = 'failed'
            all_checks['msg'] = 'SQL needs revise'

        return all_checks
    
    @staticmethod
    def get_col_val(parsed_sql):
        if parsed_sql is None:
            return None
        pairs = []
        for cond in parsed_sql['conditions']:
            if isinstance(cond,dict):
                pairs.append((cond.get('column',''), cond.get('value','')))
        pairs = list(set(pairs))
        return [pair for pair in pairs if pair[1] != '']
    
    # 数据库自有信息
    def get_dbInfo(self, db_name):
        db_Info = {}
        cursor = self.cnx.cursor()
        cursor.execute(f"use {db_name}")
        cursor.execute("SHOW TABLES")
        tables = [table['Tables_in_' + db_name] for table in cursor.fetchall()]
        for table_name in tables:
            cursor.execute(f"DESC {table_name}")
            columns = cursor.fetchall()
            db_Info[table_name] = {}
            for column in  columns:
                db_Info[table_name][column['Field']] = column
        
        return db_Info
    
    def check_cond(self, parsed_sql, fields_check):
        # # 值 mapping
        cv_pairs = self.get_col_val(parsed_sql)
        fields_mapping = fields_check['items']
        field_list = [f[1] for f in fields_mapping]
        # conditions check
        conds_check ={'status': 'succ', 'items':[]}
       
        for (col,val) in cv_pairs:
            idx = field_list.index(col)
            table = fields_mapping[idx][2].split('.')[0]
            flag,val,val_new =self.check_value(table, col,val)
            if not flag:
                conds_check['status'] = 'failed'
            conds_check['items'].append((flag, col,val,val_new))
        
        return conds_check

    def check_value(self, table_name, col, val):
        # 字符类value， 可能需要模糊和查询扩展      
        
        col_Info = self.db_Info[table_name][col]
        ty = col_Info.get('Type').lower()
        # 数字和日期类型
        if ty != 'varchar(255)' and ty != 'text':
            return self.check_num_date(col_Info, val)
        
        # 精确匹配
        query = f"SELECT {col} FROM {table_name} WHERE {col} = '{val}' LIMIT 1"
        # 模糊匹配
        query2 = f"SELECT {col} FROM {table_name} WHERE {col} LIKE '%{val}%' LIMIT 1"
        cursor = self.cnx.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) > 0:
            return (True, 'varchar', val, val)

        cursor.execute(query2)
        result = cursor.fetchall()
        if len(result) > 0:
            return (True,'varchar', val, f'%{val}%')
        
        return (False, 'varchar', val, '')

    def check_num_date(self, col_Info, val) -> tuple: 
        # TODO, date 格式未处理
        numeric = ['int', 'float', 'double', 'decimal', 'numeric', 'real', 'bigint', 'smallint', 'tinyint']
        date = ['date', 'datetime', 'timestamp', 'time', 'year']
        
        ty = col_Info.get('Type').lower()
        result = [True,'numeric', val, val]

        if ty in numeric:
           if val.isdigit():
            result[-1] = float(val)
           else:
            result[0] = False
            result[-1] = -1
        if ty in date:
            # 根据实际情况调整格式
            result[1] ='date'
            date_format = '%Y-%m-%d %H:%M:%S' if ty in ['datetime', 'timestamp'] else '%Y-%m-%d'
            try:
                result[-1] = datetime.strptime(val, date_format)
            except ValueError:
                result[0] = False
                result[-1] = ''
        
        return tuple(result)
    
class GenActivity:

    def __init__(self):

        zoneName = 'TrainingDB'
        db_type = z_config[zoneName,'db_type']
        host = z_config[zoneName,'host']
        port = int(z_config[zoneName,'port']) 
        user = z_config[zoneName,'user']
        pwd  = z_config[zoneName,'pwd']
        self.cnx = conn_srv(host, port, user, pwd, db_type)
        if self.cnx is None:
            raise ValueError("Database connection failed")
        
        self.sch_loader = Loader(z_config['Training','db_schema'])
        self.db_name = z_config['Training','project_code']

        self.checker = CheckSQL()
        logging.info("GenActivity init done")
    
    # 主功能
    async def gen_activity(self, query, sql):
        # 生成SQL2DB的1个或多个SQL, 构成一个activity
        resp = make_a_log('gen_activity')
        resp['msg'] = sql
        all_checks = self.checker.check_sql(sql)
        
        if all_checks['status'] == 'succ':
            return resp
        # 表或字段匹配不上，直接失败返回
        if 'failed' in [all_checks['table']['status'], all_checks['fields']['status']]:
            resp['hint'] = 'table or fields not in database'
            resp['status'] = 'failed'
            return resp
        # 值扩展
        conds_check = self.term_refine(query, all_checks['conds'])
        all_checks['conds'] = conds_check
        if conds_check['status'] == 'failed':
            resp['hint'] = 'some value in SQL not in database'
            resp['status'] = 'failed'
            return resp
        
        new_sql = await self.revise(query, sql, all_checks)
        resp['msg'] = new_sql  
        return resp
    
    # 考虑到DB是用户的，comment等信息，没有，需要自建，放在schema中
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

        
    #todo, check 不合格，需要revise
    async def revise(self,query, sql, checks):
        if checks['status'] == 'succ':
            return sql
        
        
        vals = checks['vals']
        fields = checks['fields']
        ni_terms =[]            # need improve terms
        if vals['status'] == 'failed':
            for val in vals['terms']:
                ni_terms.append(val[0])
            prompt = f"请从下面句子中抽出与输入的terms意思相同的部分\n句子:{query}"
            query ='\n'.join(ni_terms)
            query = f"terms:{query}"
            result = await self.norm.ask_agent(query, prompt)
        
        return sql
    
    # 简单合成，只做了select,form,where
    def gen_sql(self,slots):
        if slots is None:
            return None
        
        # "select * from 产品表 where "
        str_from = 'from '
        str_from += slots['from']
        str_select = 'select '
        str_select+= ",".join(slots["columns"])
        if slots['distinct']:
            str_select = str_select.replace("select","select distinct")
        str_where = 'where '
        for cond in slots['conditions']:
            if isinstance(cond,dict):
                if cond['value'].isdigit():
                    str_where += f"{cond['column']} {cond['op']} {cond['value']}  "
                else:
                    str_where += f"{cond['column']} {cond['op']} '{cond['value']}' "
            else:
                str_where += f"{cond} "

        return f"{str_select} {str_from} {str_where}"
    
    # TODO, 应该是refine SQL, 2024-06-24
    def term_refine(self,query, conds_check):
       
       ni_words =[]            # need improve terms
       for term in conds_check['items']:

        
        columns = slots['columns']
        for idx, column in enumerate(columns):
            st_table, st_col = self.link_field(column, tableName)
            columns[idx] = st_col
            
        # conditions
        for cond in slots.get('conditions', []):
            if isinstance(cond, str):
                continue
            st_table, st_col = self.link_field(cond['column'], tableName)
            cond['column'] = st_col
            
        return slots
    
    
        
if __name__ == "__main__":
    gentor = GenActivity()
    qalist = [('查一下价格大于1000的产品','SELECT *\nFROM product\nWHERE actual_price > 1000'),
              ('列出类别是电脑的产品名称',"SELECT product_name\nFROM product\nWHERE category = '电脑';")]
    for q, a in qalist[1:]:
        asyncio.run(gentor.gen_activity(q,a))
   
    # db_stru =gentor.gen_db_structures()
    # gentor.out_db_structures('amazon_struct.txt',db_stru)