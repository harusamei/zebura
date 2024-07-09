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
from zebura_core.LLM.llm_agent import LLMAgent
from server.msg_maker import make_a_log
from zebura_core.LLM.prompt_loader import prompt_generator
from zebura_core.activity.sql_checker import CheckSQL
import pymysql
import logging
import re

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

        self.checker = CheckSQL(self.cnx, self.db_name,self.sch_loader)
        self.prompter = prompt_generator()
        self.db_struct = self.prompter.get_dbSchema()
        self.llm = LLMAgent()
        logging.info("GenActivity init done")

    # replace virtual columns
    def replace_virtual(self, sql):
        
        slots = self.checker.sp.parse_sql(sql)
        virt_Info = self.checker.virt_Info
        subs = {'columns':[], 'conds':[]}

        for col_name, v in slots['columns']['all_cols'].items():
            if virt_Info.get(col_name):
                ori_col = virt_Info[col_name][0]
                subs['columns'].append((col_name,ori_col))      # virt, orig
        
        for key,val in slots['table'].items():
            if virt_Info.get(str(val)):
                ori_col = virt_Info[val][0]
                subs['columns'].append((str(val),ori_col))      # virt, orig
        
        for cond in slots['conditions']:
            cond = cond.strip()
            matched = re.search(r'(\S+)\s+(\S+)\s+(\S+)', cond)
            if matched:
                if '.' in matched.group(1):
                    pfx,col = matched.group(1).split('.')
                    pfx+='.'
                else:
                    pfx =''
                    col = matched.group(1)
                val = matched.group(3)
                if virt_Info.get(col):
                    ori_col =virt_Info[col][0]
                    patn = virt_Info[col][1]
                    val=val[1:-1]   # 去掉引号
                    new_val = patn.format(value=val)
                    subs['conds'].append((cond, f"{pfx}{ori_col} LIKE '{new_val}'"))

        for con_sub in subs['conds']:
            sql = sql.replace(con_sub[0],con_sub[1])
        for col_sub in subs['columns']:
            sql = sql.replace(col_sub[0],con_sub[1])

        return sql
    
    def refine_sql(self, sql):

        if '*' in sql and 'LIMIT' not in sql.upper():
            k_limit = 10
            sql = sql.rstrip(";")
            sql = sql + f" LIMIT {k_limit};"
        return sql
    
    # 清除virtual_info, 不完整，没有处理join, between
    def refine_query(self, sql):

        slots = self.checker.sp.parse_sql(sql)
        virt_Info = self.checker.virt_Info      
        new_all_cols = {}
        for col_name, v in slots['columns']['all_cols'].items():
            if virt_Info.get(col_name):
                ori_col = virt_Info[col_name][0]
                new_all_cols[ori_col] = v
            else:
                new_all_cols[col_name] = v
        slots['columns']['all_cols'] = new_all_cols

        new_table = {}
        for key,val in slots['table'].items():
            if virt_Info.get(str(val)):
                ori_col = virt_Info[val][0]
                new_table[key] = ori_col
            else:
                new_table[key] = val
        slots['table'] = new_table

        new_conds = []
        for cond in slots['conditions']:
            flag = False
            cond = cond.strip()
            matched = re.search(r'(\S+)\s+(\S+)\s+(\S+)', cond)
            if matched:
                col = matched.group(1)
                val = matched.group(3)
                if virt_Info.get(col):
                    ori_col =virt_Info[col][0]
                    patn = virt_Info[col][1]
                    val=val[1:-1]   # 去掉引号
                    new_val = patn.format(value=val)
                    new_conds.append(f"{ori_col} LIKE '{new_val}'")
                    flag = True
               
            if not flag:
                new_conds.append(cond)
        slots['conditions'] = new_conds
        print(f"after slots:{slots}")
        return self.gen_sql(slots)
    
    # 主功能
    async def gen_activity(self, query, sql):
        # 生成SQL2DB的1个或多个SQL, 构成一个activity
        resp = make_a_log('gen_activity')
        resp['msg'] = sql
        all_checks = self.checker.check_sql(sql)
        #print(f"before revise: all_checks:{all_checks}")
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

        new_sql, hint = await self.revise(sql, all_checks)
        resp['hint'] = hint
        if sql is None:
            resp['status'] = 'failed'
            return resp
        
        new_sql = self.replace_virtual(new_sql)
        resp['msg'] =self.refine_sql(new_sql)

        print(f"gen_activity resp:{resp}")

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
        table_tmpl = "Name Non-existence: '{table_name}' is non-existent in the database."
        checks = all_checks['table']
        tlist = checks.keys()
        table_name = ' '.join(tlist).replace('status','')
        # table msg
        if checks['status'] == 'failed':
            checkMsgs['table'].append(table_tmpl.format(table_name=table_name))
        
        # fields msg
        checks = all_checks['fields']
        fields_tmpl = "Name Non-existence: the fields {cols} are non-existent in the '{table_name}' table."
        join_tmpl = "Join Error: The field {col} is not in {table_name}; it is in {table_name1}. "
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
        conds_tmpl_1 = "Value Issues: value {vals} was not found."
        conds_tmpl_2 = "Value Issues: value {val} was not found in the '{col}'. It is recommended to replace it with '{new_val}'."
        conds_tmpl_3 = "Format Errors: value {vals} are incorrect format."
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
                checkMsgs['conds'].append(conds_tmpl_1.format(vals=','.join(vals)))
            if len(names[1])>0:
                for k,new_val in names[1]:
                    val = k.split(',')[1]
                    col = k.split(',')[0]
                    checkMsgs['conds'].append(conds_tmpl_2.format(val=val, col=col, new_val=new_val))
            if len(names[2])>0:
                vals = [x.split(',')[1] for x in names[2]]
                checkMsgs['conds'].append(conds_tmpl_3.format(vals=','.join(vals)))

        return checkMsgs
    
    #check 不合格，需要revise, 返回 sql, hint
    async def revise(self, sql, all_checks) -> tuple:
        if all_checks['status'] == 'succ':
            return (sql, '')
        
        checkMsgs = {}
        checkMsgs = self.gen_checkMsgs(all_checks)
        hints = []
        for k,v in checkMsgs.items():
            if len(v) == 0:
                continue
            hints.extend(v)
        print("checkMsgs:"+'\n'.join(hints))
        
        # revise by LLM
        tmpl = self.prompter.tasks['sql_revise']
        orisql = sql
        db_struct = self.db_struct
        errmsg = '\n'.join(hints)

        query = tmpl.format(dbSchema=db_struct,ori_sql=orisql, err_msgs=errmsg)
        result = await self.llm.ask_query(query,'')

        patn = "```sql\n(.*?)\n```"
        matches = re.findall(patn, result, re.DOTALL | re.IGNORECASE)
        new_sql = None
        msg = ''
        if matches:
            new_sql = matches[0]
            for hint in hints:
                if 'Value Issues' in hint:
                    msg += hint+'\n'
        else:
            new_sql = None
            msg = "SQL revise failed, please check the error messages."
            
        return new_sql, hint
                
    # 由slots生成SQL
    def gen_sql(self,slots):
        if slots is None:
            return None
        # columns, table, conditions
        tmpl = "SELECT {str_select} FROM {str_from} WHERE {str_where}"
        str_from = ''
        str_select =''
        str_where = ''
        # columns
        for col,item in slots['columns']['all_cols'].items():
            if item.get('distinct') == True:
                str_select += f"DISTINCT {col}, "
            else:
                str_select += f"{col}, "
        str_select = str_select.rstrip(', ')
        # table
        for key in slots['table'].keys():
            tstr = slots['table'][key]
            if key == 'name':
                str_from += f"{slots['table'][key]}, "
            elif key == 'join':
                str_from += f"{' '.join(tstr)}, "
            elif len(tstr) > 0:
                str_from += f"{key} {slots['table'][key]}, "
        str_from = str_from.rstrip(', ')
        # conditions
        for cond in slots['conditions']:
            str_where += f"{cond} "
        return tmpl.format(str_select=str_select, str_from=str_from, str_where=str_where)

    
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
            if v[2] in ['varchar','virtual_in','text']:
                col, word = cond.split(',')
                word = word.strip('\'"')
                ni_words[word] = [col,cond]

        query = ','.join(ni_words.keys())
        prompt = self.prompter.tasks['term_expansion']
        result = await self.llm.ask_query(query,prompt)
        new_terms = self.parse_expansion(result)

        table = all_check['table'].keys()
        tname = ' '.join(table).replace('status','')
        tname = tname.strip()
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
    qalist = [('查一下价格大于1000的产品','SELECT *\nFROM product\nWHERE actual_price = 1000 AND brand = "苹果";'),
              ('列出品牌是电脑的产品名称',"SELECT product_name\nFROM product\nWHERE brand LIKE '%apple%';")]
    for q, a in qalist:
        asyncio.run(gentor.gen_activity(q,a))
   
    # db_stru =gentor.gen_db_structures()
    # gentor.out_db_structures('amazon_struct.txt',db_stru)