# check SQL 错误，包括表名，列名，条件值
import sys
import os
sys.path.insert(0, os.getcwd().lower())
from zebura_core.query_parser.schema_linker import Sch_linking
from zebura_core.utils.sqlparser import ParseSQL
from zebura_core.knowledges.schema_loader import Loader
import logging
import re
import datetime
import random

class CheckSQL:
    def __init__(self, db_cnx, db_name, scha_loader:Loader):

        self.db_name = db_name
        self.scha_loader = scha_loader
        self.cnx = db_cnx   
        cursor = self.cnx.cursor()
        cursor.execute(f"use {self.db_name}")

        self.sl = Sch_linking(scha_file=None, scha_loader=self.scha_loader)
        self.sp = ParseSQL()
        self.db_Info,self.virt_Info = self.get_schaInfo()
        logging.info("CheckSQL init done")

    # 主功能
    def check_sql(self,sql):

        slots = self.sp.parse_sql(sql)
        print(slots)
        
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
            if t1 =='*':
                fields_check[field]=True
                continue
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
            if check != True:
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
        # 只处理condition中 =, like，其它条件不处理
        # WHERE (department = 'Sales' OR salary > 10000) AND department = 'Marketing'
        pattern = re.compile(r'(?i)([^><() ]+)\s*(=|like)\s*([^><() ]+)')
        for cond in slots['conditions']:
            match = pattern.search(cond)
            if match:
                all_checks['conds'].append((match.group(1),match.group(3)))
        
        for k,v in slots['columns']['all_cols'].items():
            if v.get('ttype').lower()!= 'function' and '*' not in k:
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
    
    # True 存在， False 不存在
    def is_value_exist(self, table_name, col, val):
        
        quy1 = "SELECT {col} FROM {table_name} WHERE {col} = '{val}' LIMIT 1"
        quy2 = "SELECT {col} FROM {table_name} WHERE {col} LIKE '%{val}%' LIMIT 1"
        quy3 = "SELECT {col} FROM {table_name} WHERE {col} LIKE '{val}' LIMIT 1"  # val 格式预先设置好

        col_Info = self.db_Info[table_name].get(col, None)
        # 字段不存在
        if col_Info is None:
            return False
        ty = col_Info.get('type').lower()
        val = val.strip('\'"')
        # virtual 类型 "type": "VIRTUAL_IN(product_name.%{value}%)"
        if ty.split('(')[0] == 'virtual_in' and '{' in ty:
            val = val.replace('%','')
            tStr = ty.split('(')[1].strip(')')  # 从virtual_in(中取出pattern
            col =tStr.split('.')[0]
            val = tStr.split('.')[1].format(value=val)    # 替换pattern中的值
            query = quy3.format(col=col, table_name=table_name, val=val)
        elif '%' in val and ('varchar'in ty or 'text' in ty):
            query = quy2.format(col=col, table_name=table_name, val=val)
        elif 'varchar' in ty or 'text' in ty:
            query = quy1.format(col=col, table_name=table_name, val=val)
        else:
            return False
        
        cursor = self.cnx.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) > 0:
            return True
        return False

    def check_value(self, table_name, col, val):
        # 字符类value， 可能需要模糊和查询扩展              
        col_Info = self.db_Info[table_name].get(col, None)
        # 字段不存在
        if col_Info is None:
            return (False, 'NIL','NOTEXIST')
        ty = col_Info.get('type').lower()
        ttype = ty.split('(')[0]
        # 数字和日期类型
        if ttype not in ['varchar','text','virtual_in']:
            return self.check_format(col_Info, val)
        
        check = [False, 'NIL',ttype]
        
        flag = self.is_value_exist(table_name, col, val)
        if flag:
            return True
        
        if '%' not in val and ttype in ['varchar','text']:
            val = f'%{val}%'
            flag = self.is_value_exist(table_name, col, val)
        if flag:
            return True
        
        return check
    
    # 数字和日期只检查格式
    def check_format(self, col_Info, val) -> tuple: 
        # TODO, date 格式未处理
        numeric = ['int', 'float', 'double', 'decimal', 'numeric', 'real', 'bigint', 'smallint', 'tinyint']
        date = ['date', 'datetime', 'timestamp', 'time', 'year']
        
        ty = col_Info.get('type').lower()
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
    
    # get from schema file, 前提是schema文件已经加载， db_name = project_code
    def get_schaInfo(self):
        db_Info = {}        # DB中所有表及字段信息
        virt_Info = {}      # 虚拟字段信息
        table_names =self.scha_loader.get_table_nameList() 
        for table_name in table_names:
            db_Info[table_name] = {}
            table_info = self.scha_loader.get_table_info(table_name)
            columns = table_info.get('columns')
            colDict = {}
            for col in columns:
                colDict[col['column_name']] = col
                if 'virtual_in' in col['type'].lower():
                    virt_Info[col['column_name']] = col['type']
            db_Info[table_name] = colDict
        
        for name,tStr in virt_Info.items():
            tStr = tStr.split('(')[1].strip(')')
            col =tStr.split('.')[0]
            val = tStr.split('.')[1]
            virt_Info[name]=(col, val)

        return db_Info, virt_Info
    
    # 直接从DB中获取 dbInfo
    def get_dbStruct(self, db_name):
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
        exec_limit = 10
        choice = [random.randint(0, len(voc)-1) for _ in range(exec_limit)]
        #tmpl = "SELECT {col} FROM {table_name} WHERE {col} LIKE '%{val}%' LIMIT 1"
        check = [False, 'NIL','EXPN']
        for indx in choice:
            val = voc[indx]
            flag = self.is_value_exist(table_name, col, val)
            if flag:           
                return [True, val, 'EXPN']
        return check
    
if __name__ == "__main__":
    from settings import z_config
    from gen_activity import conn_srv
    zoneName = 'TrainingDB'
    db_type = z_config[zoneName,'db_type']
    host = z_config[zoneName,'host']
    port = int(z_config[zoneName,'port']) 
    user = z_config[zoneName,'user']
    pwd  = z_config[zoneName,'pwd']
    cnx = conn_srv(host, port, user, pwd, db_type)
    if cnx is None:
        raise ValueError("Database connection failed")
    
    sch_loader = Loader(z_config['Training','db_schema'])
    db_name = z_config['Training','project_code']

    checker = CheckSQL(cnx, db_name,sch_loader)
    qalist = ['SELECT *\nFROM product\nWHERE actual_price = 1000 AND brand = "苹果";',
              "SELECT product_name\nFROM product\nWHERE brand LIKE '%apple%';",
              "SELECT d.department_name AS Department,COUNT(e.employee_id) AS NumberOfEmployees FROM departments d  LEFT JOIN employees e ON d.department_id = e.department_id  GROUP BY d.department_name  ORDER BY  NumberOfEmployees DESC;"]
    for qa in qalist[2:]:
        print(checker.check_sql(qa))
    cnx.close()