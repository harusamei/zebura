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
from zebura_core.utils.sqlparser import ParseSQL
from zebura_core.LLM.llm_agent import LLMAgent
from server.msg_maker import make_a_log
from zebura_core.LLM.prompt_loader import prompt_generator

import pymysql
import logging
import re
import datetime
import random
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

# check SQL 错误，包括表名，列名，条件值
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

        self.sp = ParseSQL()

        self.db_name = z_config['Training','project_code']
        self.db_Info = self.get_dbInfo(self.db_name)


    # 主功能
    def check_sql(self,sql):

        slots = self.sp.parse_sql(sql)

        all_checks ={ 'status': 'succ', 'msg':'correct SQL',
                      'table':{}, 'fields':{}, 'conds':{}
                    }
        if slots is None:       # 无法解析
            all_checks['status'] = 'failed'
            all_checks['msg'] = 'SQL cannot be parsed, only SELECT-type is supported.'
            all_checks['table'] = None
            return all_checks
       
        # get check points
        ckps = self.get_checkPoints(slots)
        # 表名 mapping
        table_name = ckps['table']
        table_check = {'status':'succ'}
        linked = self.sl.link_table(table_name)
        if linked != table_name:
            table_check['status'] = 'failed'
            table_check[table_name]=(False,linked)
        else:
            table_check[table_name]=True
        
        # 列名 mapping
        table_name = linked         # 修正后的表名
        field_names = ckps['fields']
        fields_check ={'status': 'succ'}
        for field in field_names:
            t1, f1 = self.sl.link_field(field)
            if f1 != field or t1 != table_name:
                fields_check['status'] = 'failed'
                fields_check[field]=(False, f'{t1}.{f1}')
            else:
                fields_check[field]=True
  
        # 条件检查
        conds_check ={'status': 'succ'}
        conds = ckps['conds']
        
        for (col,val) in conds:
            table = table_name
            field = col
            tfield = fields_check.get(col, None)
            if  tfield is not None and tfield != True:
                table = tfield[1].split('.')[0]
                field = tfield[1].split('.')[1]

            check =self.check_value(table, field, val)
            conds_check[f'{col},{val}'] = check
            if not check[0]:
                conds_check['status'] = 'failed'
               
        all_checks['conds'] = conds_check
        all_checks['table'] = table_check
        all_checks['fields'] = fields_check

        if 'failed' in [table_check['status'], fields_check['status'], conds_check['status']]:
            all_checks['status'] = 'failed'
            all_checks['msg'] = 'SQL needs revise'

        return all_checks
    @staticmethod
    def get_checkPoints(slots):
        if slots is None:
            return None
        all_checks ={   'conds' : [],     # only equations
                        'fields': [],
                        'table' :''
                    }
        all_checks['table'] = slots['table']['name']
        # Note: Where 里面涉及的field，不放入 fields 中
        # 抽取condition中等式，其它条件不处理
        # WHERE (department = 'Sales' OR salary > 10000) AND department = 'Marketing'
        for cond in slots['conditions']:
            match = re.search(r'(?i)([^><() ]+)\s*(=|like)\s*([^><() ]+)',cond)
            if match:
                all_checks['conds'].append((match.group(1),match.group(3)))
        
        for k,v in slots['columns']['all_cols'].items():
            if v.get('ttype').lower()!= 'function':
                all_checks['fields'].append(k)

        others = ['order by','group by']
        for kname in others:
            if slots['table'].get(kname) != '':
                tVal = slots['table'][kname]
                if isinstance(tVal, list):
                    all_checks['fields'].extend(tVal)
                else:
                    all_checks['fields'].append(tVal)
        
        # 替换AS为原列名
        fields = list(set(all_checks['fields']))
        for k,v in slots['columns']['all_cols'].items():
            if v.get('as') != '':
                asName = v.get('as')
                if asName in fields:
                    fields.remove(asName)
                    if v.get('ttype').lower()!= 'function': 
                        fields.append(k)
        all_checks['fields'] = list(set(fields))
        all_checks['conds'] = list(set(all_checks['conds']))
        return all_checks
    
    def check_value(self, table_name, col, val):
        # 字符类value， 可能需要模糊和查询扩展              
        col_Info = self.db_Info[table_name][col]
        ty = col_Info.get('Type').lower()
        # 数字和日期类型
        if ty != 'varchar(255)' and ty != 'text':
            return self.check_format(col_Info, val)
        
        # 精确匹配
        val = val.strip('\'"')
        query = f"SELECT {col} FROM {table_name} WHERE {col} = '{val}' LIMIT 1"
        # 模糊匹配
        query2 = f"SELECT {col} FROM {table_name} WHERE {col} LIKE '%{val}%' LIMIT 1"
        cursor = self.cnx.cursor()
        check = [False, 'NIL','VARCHAR']
        if '%' not in val:
            cursor.execute(query)
            result = cursor.fetchall()
            if len(result) > 0:
                return True
        else:
            query2 = f"SELECT {col} FROM {table_name} WHERE {col} LIKE '{val}' LIMIT 1"    
        
        cursor.execute(query2)
        result = cursor.fetchall()
        if len(result) > 0 and '%' in val:
            check = True
        elif len(result) > 0:
            check[1] = f'%{val}%'
    
        return check
    
    # 数字和日期只检查格式
    def check_format(self, col_Info, val) -> tuple: 
        # TODO, date 格式未处理
        numeric = ['int', 'float', 'double', 'decimal', 'numeric', 'real', 'bigint', 'smallint', 'tinyint']
        date = ['date', 'datetime', 'timestamp', 'time', 'year']
        
        ty = col_Info.get('Type').lower()
        check = True
        if ty in numeric:
            if not val.isdigit():
                check = (False, 'NIL','NUMERIC')
        elif ty in date:
            # 根据实际情况调整格式
            date_format = '%Y-%m-%d %H:%M:%S' if ty in ['datetime', 'timestamp'] else '%Y-%m-%d'
            try:
                val = datetime.strptime(val, date_format)
                check = True
            except ValueError:
                check = (False, 'NIL','DATE')   
        
        return check
    
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
    
    # 字符类值的扩展vocabuary
    def check_expn(self, table_name, col, voc):
        limit = 10
        choice = [random.randint(0, len(voc)-1) for _ in range(limit)]
        tmpl = "SELECT {col} FROM {table_name} WHERE {col} LIKE '%{val}%' LIMIT 1"
        check = [False, 'NIL','EXPN']
        for indx in choice:
            val = voc[indx]
            query = tmpl.format(col=col, table_name=table_name, val=val)
            cursor = self.cnx.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            # 第一个匹配成功即可
            if len(result) > 0:
                return [True, val, 'EXPN']
        return check
       
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
        self.prompter = prompt_generator()
        self.llm = LLMAgent()
        logging.info("GenActivity init done")
    
    # 主功能
    async def gen_activity(self, query, sql):
        # 生成SQL2DB的1个或多个SQL, 构成一个activity
        resp = make_a_log('gen_activity')
        resp['msg'] = sql
        all_checks = self.checker.check_sql(sql)

        if all_checks['status'] == 'succ':
            return resp
        elif all_checks['table'] is None:   # 无法解析，彻底失败
            resp['status'] = 'failed'
            resp['msg'] = all_checks['msg']
            return resp
        
        # 表或字段 succ, conds failed, 需要refine conds
        if 'failed' not in [all_checks['table']['status'], all_checks['fields']['status']]:
            conds_check = await self.refine_conds(all_checks)
            all_checks['conds'] = conds_check

        new_sql, hint = self.revise(query, sql, all_checks)
        resp['hint'] = hint
        if sql is None:
            resp['status'] = 'failed'   
        else:
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

    # 生成check发现的错误信息
    # conds_check:{'status': 'failed', "category,'电脑'": (True, 'Computer', 'EXPN')}
    # conds_check:{'status': 'failed', "price, abc": (False, 'NIL','DATE')}, False, 'NIL','NUMERIC', （False, 'NIL','VARCHAR'）
    # table_check:{'status': 'failed', 'product': (False, 'products')}
    # fields_check:{'status': 'failed', 'product_name': (False, 'product.product_name')}
    def gen_checkMsgs(self, all_checks):
        checkMsgs ={ 'table':[], 'fields':[], 'conds':[]}
        table_tmpl = "'{table_name}' was not found. Please find a similar table name based on database schema"
        checks = all_checks['table']
        tlist = checks.keys()
        table_name = ' '.join(tlist).replace('status','')
        # table msg
        if checks['status'] == 'failed':
            checkMsgs['table'].append(table_tmpl.format(table_name=table_name))
        
        # fields msg
        checks = all_checks['fields']
        fields_tmpl = "The fields {cols} are not in the table '{table_name}'"
        join_tmpl = "The field {col} is not in {table_name}; it is in {table_name1}. You need to use a JOIN."
        cols = []
        cols_j = []
        for k,v in checks.items():
            if k == 'status' or v == True:
                continue
            table_name1 = v[1].split('.')[0]
            if table_name1 == table_name:
                cols.append(k)
            else:
                cols_j.append((k,table_name1))
        if checks['status'] == 'failed':
            if len(cols)>0:
                checkMsgs['fields'].append(fields_tmpl.format(cols=','.join(cols), table_name=table_name))
            if len(cols_j)>0:
                for col,table_name1 in cols_j:
                    checkMsgs['fields'].append(join_tmpl.format(col=col, table_name=table_name, table_name1=table_name1))

        # conds msg
        conds_tmpl_1 = "value {vals} was not found in the fields '{cols}'."
        conds_tmpl_2 = "value {val} was not found in the field {col}. It is recommended to replace it with '{new_val}'."
        conds_tmpl_3 = "format of value {vals} are incorrect'."
        checks = all_checks['conds']
        names = [[],[],[]]
        for k,v in checks.items():
            if k == 'status' or v == True:
                continue
            if v[2] == 'VARCHAR':   # 无值，且term expansion 无结果
                names[0].append(k)
            elif v[2] == 'EXPN':    # expansion 有结果
                names[1].append((k,v[1]))
            else:                   # 日期数字格式错误
                names[2].append(k)
        if checks['status'] == 'failed':
            if len(names[0])>0:
                vals = [x.split(',')[1] for x in names[0]]
                cols = [x.split(',')[0] for x in names[0]]    
                checkMsgs['conds'].append(conds_tmpl_1.format(vals=','.join(vals), cols=','.join(cols)))
            if len(names[1])>0:
                for k,new_val in names[1]:
                    val = k.split(',')[1]
                    col = k.split(',')[0]
                    checkMsgs['conds'].append(conds_tmpl_2.format(val=val, col=col, new_val=new_val))
            if len(names[2])>0:
                vals = [x.split(',')[1] for x in names[2]]
                cols = [x.split(',')[0] for x in names[2]]
                checkMsgs['conds'].append(conds_tmpl_3.format(vals=','.join(vals), cols=','.join(cols)))

        return checkMsgs
    
    #check 不合格，需要revise, 返回 sql, hint
    def revise(self, query, sql, all_checks) -> tuple:
        if all_checks['status'] == 'succ':
            return (sql, '')
        
        checkMsgs = {}
        checkMsgs = self.gen_checkMsgs(all_checks)
        hints = []
        for k,v in checkMsgs.items():
            if len(v) == 0:
                continue
            hints.append(f"{k} errors:")
            hints.extend(v)
        print("checkMsgs:"+'\n'.join(hints))
        
        return sql, '\n'.join(hints)
                
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
    # 解析term_expansion from LLM
    def parse_expansion(self,llm_answer) -> dict:
        tlist = llm_answer.split('\n')
        tem_dic ={}
        kword = '##'
        for temstr in tlist:
            if '[Keyword:' in temstr:
                kword = temstr.split(':')[1]
                kword = re.sub(r'\s*]\s*','',kword).strip()
                tem_dic[kword] = ''
            else:
                temstr = re.sub(r'-.*:','',temstr).strip()
                tem_dic[kword]+= temstr+'\n'
        new_terms = {}
        for k,v in tem_dic.items():
            v = v.replace('OR','\n')
            v = v.replace('(','\n')
            v = v.replace(')','\n')
            v = re.sub('[ ]+',' ',v)
            v = re.sub(r'(\s*\n\s*)+','\n',v)
            v=v.strip()
            new_terms[k] = list(set(v.split('\n')))
        return new_terms

    # term expansion to refine the equations in Where
    async def refine_conds(self, all_check):
       
        conds_check = all_check['conds']
        if conds_check['status'] == 'succ':
            return conds_check
        
        ni_words ={}           # need improve terms
        for cond,v in conds_check.items():
            if v == True:
                continue
            if v[2] == 'VARCHAR':
                col, word = cond.split(',')
                word = word.strip('\'"')
                ni_words[word] = [col,cond]

        query = ','.join(ni_words.keys())
        prompt = self.prompter.tasks['term_expansion']
        result = await self.llm.ask_query(query,prompt)
        new_terms = self.parse_expansion(result)

        table = all_check['table'].keys()
        tname = ' '.join(table).replace('status','')

        for word,voc in new_terms.items():
            tItem = ni_words.get(word,None)
            if tItem is None:
                logging.error(f"Error: {word} not in ni_words")
                continue
            col,cond = tItem
            check = self.checker.check_expn(tname, col, voc)
            conds_check[cond] = check

        print(f"refined conds_check:{conds_check}")
        return conds_check
               
if __name__ == "__main__":
    gentor = GenActivity()
    qalist = [('查一下价格大于1000的产品','SELECT *\nFROM product\nWHERE actual_price > 1000'),
              ('列出类别是电脑的产品名称',"SELECT product_name\nFROM product\nWHERE category = '电脑';")]
    for q, a in qalist[1:]:
        asyncio.run(gentor.gen_activity(q,a))
   
    # db_stru =gentor.gen_db_structures()
    # gentor.out_db_structures('amazon_struct.txt',db_stru)