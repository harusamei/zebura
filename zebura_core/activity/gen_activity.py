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
from zebura_core.constants import D_SELECT_LIMIT as k_limit
from zebura_core.LLM.prompt_loader import prompt_generator
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.activity.sql_checker import CheckSQL
import sqlparse

import pymysql
import logging
import re


# connect sql server
def conn_srv(host, port, user, pwd, type='mysql'):
    try:
        if type == 'mysql':
            cnx = pymysql.connect(
                host=host,  # 主机名（或IP地址）
                port=port,  # 端口号，默认为3306
                user=user,  # 用户名
                password=pwd,  # 密码
                charset='utf8mb4',  # 设置字符编码
                cursorclass=pymysql.cursors.DictCursor  # 返回字典类型的游标
            )
            return cnx
        else:
            logging.error(f"ERR_cursor, {type} not supported")
            return None
    except Exception as e:
        logging.error(f"connect_sql() error: {e}")
        return None


class GenActivity:

    def __init__(self):

        zoneName = 'TrainingDB'
        db_type = z_config[zoneName, 'db_type']
        host = z_config[zoneName, 'host']
        port = int(z_config[zoneName, 'port'])
        user = z_config[zoneName, 'user']
        pwd = z_config[zoneName, 'pwd']
        self.cnx = conn_srv(host, port, user, pwd, db_type)
        if self.cnx is None:
            raise ValueError("Database connection failed")

        self.sch_loader = Loader(z_config['Training', 'db_schema'])
        self.db_name = z_config['Training', 'project_code']

        self.checker = CheckSQL(self.cnx, self.db_name, self.sch_loader)
        self.prompter = prompt_generator()
        self.ans_extr = AnsExtractor()

        self.db_struct = self.prompter.get_dbSchema()
        self.llm = LLMAgent()
        logging.info("GenActivity init done")

    # replace virtual columns
    def replace_virtual(self, sql):
        slots = self.checker.sp.parse_sql(sql)
        virt_Info = self.checker.virt_Info
        subs = {'columns': [], 'conds': []}

        for col_name, v in slots['columns']['all_cols'].items():
            if virt_Info.get(col_name):
                ori_col = virt_Info[col_name][0]
                subs['columns'].append((col_name, ori_col))  # virt, orig

        for key, val in slots['table'].items():
            if virt_Info.get(str(val)):
                ori_col = virt_Info[val][0]
                subs['columns'].append((str(val), ori_col))  # virt, orig

        for cond in slots['conditions']:
            cond = cond.strip()
            matched = re.search(r'(\S+)\s+(\S+)\s+(\S+)', cond)
            if matched:
                if '.' in matched.group(1):
                    pfx, col = matched.group(1).split('.')
                    pfx += '.'
                else:
                    pfx = ''
                    col = matched.group(1)
                val = matched.group(3)
                if virt_Info.get(col):
                    ori_col = virt_Info[col][0]
                    patn = virt_Info[col][1]
                    val = val.strip('\'"')  # 去掉引号
                    new_val = patn.format(value=val)
                    new_val = re.sub(r'%+', '%', new_val)
                    subs['conds'].append((cond, f"{pfx}{ori_col} LIKE '{new_val}'"))

        for con_sub in subs['conds']:
            sql = sql.replace(con_sub[0], con_sub[1])
        for col_sub in subs['columns']:
            sql = sql.replace(col_sub[0], col_sub[1])

        return sql

    def refine_sql(self, sql):
        tsql = sql.lower()
        # 加limit的情况
        if 'limit' not in tsql and 'count' not in tsql:
            sql = sql.rstrip(";")
            sql = sql + f" LIMIT {k_limit};"
        # column 去重, 未考虑distince情况，不充分
        matched = re.search(r'SELECT\s+(.*)\s*FROM', sql, re.DOTALL)
        if matched:
            ori_select = matched.group(1)
            tlist = ori_select.split(',')
            tlist = list(set([x.strip() for x in tlist]))
            new_select = ','.join(tlist)
            new_select.strip(',')
            sql = sql.replace(ori_select, new_select + '\n')
            sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
        return sql

    # 主功能, 生成最终用于查询的SQL
    async def gen_activity(self, query, sql):
        # 生成SQL2DB的1个或多个SQL, 构成一个activity
        resp = make_a_log('gen_activity')
        resp['msg'] = sql
        all_checks = self.checker.check_sql(sql)

        if all_checks['status'] == 'succ':
            new_sql = self.replace_virtual(sql)
            resp['msg'] = self.refine_sql(new_sql)
            return resp

        if all_checks['table'] is None:  # error_parsesql，彻底失败
            resp['status'] = 'failed'
            resp['msg'] = all_checks['msg']
            return resp

        # 表或字段 succ, conds failed, 需要refine conds
        if 'failed' not in [all_checks['table']['status'], all_checks['fields']['status']]:
            conds_check = await self.refine_conds(all_checks)
            all_checks['conds'] = conds_check

        new_sql, hint = await self.revise(sql, all_checks)
        resp['hint'] = hint
        if new_sql is None:
            resp['status'] = 'failed'
            resp['msg'] = "ERR_parsesql, wrong sql structure."
            return resp

        new_sql = self.replace_virtual(new_sql)
        resp['msg'] = self.refine_sql(new_sql)
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
        create_table = {}
        for table_name in tables:
            cursor.execute(f"show create table {table_name}")
            columns = cursor.fetchall()
            create_table[table_name] = columns[0]['Create Table']
            tDict = self.sch_loader.get_table_info(table_name)
            dbstruc[table_name] = {"desc": tDict.get('desc', ''), "columns": []}
            cols = self.sch_loader.get_all_columns(table_name)
            temDict = {}
            for col in cols:
                temDict[col['column_name']] = col
            dbstruc[table_name]['cols'] = temDict

        # {'Field': 'cpu_main_frequency', 'Type': 'varchar(255)', 'Null': 'YES', 'Key': '', 'Default': None, 'Extra': ''}
        tmp = "{fname}, {ftype}, {primary}, {foreign}, COMMENT({alias})"
        tmp_primary = "PRIMARY KEY (`{fname}`)"
        tmp_foreign = "FOREIGN KEY (`{fname}`) REFERENCES"
        for table_name in tables:

            cursor.execute(f"DESC {table_name}")
            cols = cursor.fetchall()
            for col in cols:
                col_desc = ''
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
                        foreign += " REFERENCES " + match.group(1)
                alias = ""
                temDict = dbstruc[table_name]['cols'][fname]
                alias = f"{temDict.get('name', '')}, {temDict.get('alias', '')},{temDict.get('name_zh', '')},{temDict.get('alias_zh', '')}"

                col_desc = tmp.format(fname=fname, ftype=ftype, primary=primary, foreign=foreign, alias=alias)
                col_desc = re.sub(r'(, )+', ', ', col_desc)
                col_desc = col_desc.replace('COMMENT(,', 'COMMENT(')
                col_desc = col_desc.replace(',,)', ' )')
                col_desc = col_desc.replace('COMMENT(  )', '')

                dbstruc[table_name]['columns'].append(col_desc)
        cursor.close()
        return dbstruc

    @staticmethod
    def out_db_structures(filename, dbstruc):

        with open(filename, 'w') as f:
            for table_name in dbstruc.keys():
                f.write(f"Table: {table_name}\nPurpose: {dbstruc[table_name]['desc']}\n")
                f.write("Fields:\n")
                for col in dbstruc[table_name]['columns']:
                    f.write(f"{col}\n")
                f.write("\n")
        print(f"DB structures are saved to {filename}")

    # 生成check功能发现的错误信息
    # conds_check:{'status': 'failed', "category,'电脑'": (True, 'Computer', 'EXPN')}
    # conds_check:{'status': 'failed', "price, abc": (False, 'NIL','DATE')}, False, 'NIL','NUMERIC', （False, 'NIL','VARCHAR'）
    # table_check:{'status': 'failed', 'product': (False, 'products')}
    # fields_check:{'status': 'failed', 'product_name': (False, 'product.product_name')}
    def gen_checkMsgs(self, all_checks):
        checkMsgs = {'table': [], 'fields': [], 'conds': []}
        table_tmpl = "Name Non-existence: '{table_name}' is non-existent in the database."
        checks = all_checks['table']
        tlist = checks.keys()
        table_name = ' '.join(tlist).replace('status', '')
        # table msg
        if checks['status'] == 'failed':
            checkMsgs['table'].append(table_tmpl.format(table_name=table_name))

        # fields msg
        checks = all_checks['fields']
        fields_tmpl = "Name Non-existence: the fields {cols} are non-existent in the '{table_name}' table."
        join_tmpl = "Join Error: The field {col} is not in {table_name}; it is in {table_name1}. "
        cols = []
        cols_j = []
        for k, v in checks.items():
            if k == 'status' or v == True:
                continue
            table_name1 = v[1].split('.')[0]
            if table_name1 == table_name:
                cols.append(k)
            else:
                cols_j.append((k, table_name1))
        if checks['status'] == 'failed':
            if len(cols) > 0:
                checkMsgs['fields'].append(fields_tmpl.format(cols=','.join(cols), table_name=table_name))
            if len(cols_j) > 0:
                for col, table_name1 in cols_j:
                    checkMsgs['fields'].append(
                        join_tmpl.format(col=col, table_name=table_name, table_name1=table_name1))

        # conds msg
        conds_tmpl_1 = "Value Issues: value {vals} was not found."
        conds_tmpl_2 = "Value Issues: value {val} was not found in the '{col}'. It is recommended to replace it with '{new_val}'."
        conds_tmpl_3 = "Format Errors: value {vals} are incorrect format."
        checks = all_checks['conds']
        names = [[], [], []]
        for k, v in checks.items():
            if k == 'status' or v == True:
                continue
            if v[1].lower() == 'nil':  # 无值，且term expansion 无结果
                names[0].append(k)
            elif v[2].lower() == 'expn':  # expansion 有结果
                names[1].append((k, v[1]))
            else:  # 日期数字格式错误
                names[2].append(k)

        if checks['status'] == 'failed':
            if len(names[0]) > 0:
                vals = [x.split(',')[1] for x in names[0]]
                checkMsgs['conds'].append(conds_tmpl_1.format(vals=','.join(vals)))
            if len(names[1]) > 0:
                for k, new_val in names[1]:
                    val = k.split(',')[1]
                    col = k.split(',')[0]
                    checkMsgs['conds'].append(conds_tmpl_2.format(val=val, col=col, new_val=new_val))
            if len(names[2]) > 0:
                vals = [x.split(',')[1] for x in names[2]]
                checkMsgs['conds'].append(conds_tmpl_3.format(vals=','.join(vals)))

        return checkMsgs

    # check 不合格，需要revise, 返回 sql, hint
    async def revise(self, sql, all_checks) -> tuple:
        if all_checks['status'] == 'succ':
            return (sql, '')

        checkMsgs = {}
        checkMsgs = self.gen_checkMsgs(all_checks)
        hints = []
        for k, v in checkMsgs.items():
            if len(v) == 0:
                continue
            hints.extend(v)
        logging.info("checkMsgs in revise:" + '\n'.join(hints))

        # revise by LLM
        tmpl = self.prompter.tasks['sql_revise']
        orisql = sql
        db_struct = self.db_struct
        errmsg = '\n'.join(hints)

        query = tmpl.format(dbSchema=db_struct, ori_sql=orisql, err_msgs=errmsg)
        result = await self.llm.ask_query(query, '')

        parsed = self.ans_extr.output_extr('sql_revise',result)
        new_sql = parsed['msg']
        msg = new_sql
        if parsed['status'] == 'succ':
            for hint in hints:
                if 'Value Issues' in hint:
                    msg += hint + '\n'
        else:
            new_sql = None
            msg = "ERR: revise failed. "+ msg
        return new_sql, msg

    # 由slots生成SQL,不完善
    def gen_sql(self, slots):
        if slots is None:
            return None
        # columns, table, conditions
        tmpl = "SELECT {str_select} FROM {str_from} WHERE {str_where}"
        str_from = ''
        str_select = ''
        str_where = ''
        # columns
        for col, item in slots['columns']['all_cols'].items():
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

    # term expansion to refine the equations in Where
    async def refine_conds(self, all_check):
        conds_check = all_check['conds']
        if conds_check['status'] == 'succ':
            return conds_check

        ni_words = {}  # need improve terms
        for cond, v in conds_check.items():
            if v == True:
                continue
            if v[2] in ['varchar', 'virtual_in', 'text']:
                col, word = cond.split(',')
                word = word.strip('\'"')
                ni_words[word] = [col, cond]

        if len(ni_words) == 0:
            conds_check['status'] == 'succ'
            return conds_check

        query = ','.join(ni_words.keys())
        prompt = self.prompter.tasks['term_expansion']
        result = await self.llm.ask_query(query, prompt)
        parsed = self.ans_extr.output_extr('term_expansion', result)
        
        if parsed['status'] == 'failed':
            conds_check['status'] == 'failed'
            return conds_check
        
        new_terms = parsed['msg']
        table = all_check['table'].keys()
        tname = ' '.join(table).replace('status', '')
        tname = tname.strip()
        # 匹配忽略大小写
        ni_words_lower = {key.lower(): value for key, value in ni_words.items()}
        for word, voc in new_terms.items():
            word = word.lower()
            tItem = ni_words_lower.get(word, None)
            if tItem is None:
                logging.error(f"Error: {word} not in ni_words")
                continue
            col, cond = tItem
            check = self.checker.check_expn(tname, col, voc)
            conds_check[cond] = check

        return conds_check


if __name__ == "__main__":
    gentor = GenActivity()
    qalist = [('请告诉我苹果产品的类别', 'SELECT DISTINCT category\nFROM product\nWHERE brand = "Apple";'),
              ('请告诉我风扇的所有价格', 'SELECT actual_price, discounted_price FROM product WHERE category = "fan";'),
              ('查一下价格大于1000的产品', 'SELECT *\nFROM product\nWHERE actual_price = 1000 AND brand = "苹果";'),
              ('列出品牌是电脑的产品名称', "SELECT product_name\nFROM product\nWHERE brand LIKE '%apple%';")]
    for q, a in qalist:
        resp = asyncio.run(gentor.gen_activity(q, a))
        print(f"query:{q}\nresp:{resp['msg']}")

    db_stru =gentor.gen_db_structures()
    gentor.out_db_structures('amazon_struct.txt',db_stru)