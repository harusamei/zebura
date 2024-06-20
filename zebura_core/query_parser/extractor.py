##########################################################
# 对SQL进行解析，提取基本元素, check the SQL syntax
##########################################################
import os
import sys
import re
sys.path.insert(0, os.getcwd())
import settings
from zebura_core.utils.sqlparser import parse_sql
import logging

class Extractor:
    def __init__(self):
        self.where_pats = [r'(\S+)\s+(LIKE)\s+\W(.*)\W', 
                           r'(\S+)\s*([><=]+)\s*(\d+)',
                           r'(\S+)\s*([><=]+)\s*\W(.*)\W',
                           r'(\S+)\s*(=)\s*\W(.*)\W']
        

# 只能解析select 开头的， 可以解析的信息存在slots中
# slots = dict.fromkeys(['columns','from', 'conditions', 'distinct', 'limit', 'offset','order by','group by'])
    def extract(self,sql):
        if sql is None:
            return  None
        
        if isinstance(sql, list):
            sql = sql[0]
        slots= parse_sql(sql)
        if not slots:
            return None
        
        conditions = []
        if len(slots['conditions'])>0 :
            for cond in slots['conditions']:
                parsed_condition = self.parse_cond(cond)
                conditions.append(parsed_condition)
        slots['conditions'] = conditions
        return slots
    
    def parse_cond(self,cond):
        parsed_condition ={'column':"", 'op':"", 'value':""}
        pts = self.where_pats
        matched = False
        for pt in pts:
            match = re.search(pt, cond, re.IGNORECASE)
            if match:
                parsed_condition['column'] = match.group(1)
                parsed_condition['op'] = match.group(2)
                parsed_condition['value'] = match.group(3)
                matched = True
                break
        if not matched:
            parsed_condition = cond
        return parsed_condition
    
    @staticmethod
    def get_table_name(parsed_sql):
        if parsed_sql is None:
            return None
        return parsed_sql['from']
    
    @staticmethod
    def get_values(parsed_sql):
        if parsed_sql is None:
            return None
        
        vals =[]
        for cond in parsed_sql['conditions']:
            if isinstance(cond,dict):
                vals.append(cond.get('value',''))
        if '' in vals:
            logging.warning("incorrect value in conditions of SQL")
        vals = list(set(vals))
        vals = list(filter(None, vals))
        return vals
    
    @staticmethod
    def get_fields(parsed_sql):
        if parsed_sql is None:
            return None
        
        f_names =[]
        for cond in parsed_sql['conditions']:
            if isinstance(cond,dict):
                f_names.append(cond.get('column',''))
        f_names.extend(parsed_sql.get('columns',''))
        f_names.append(parsed_sql['order by'])
        f_names.append(parsed_sql['group by'])
        
        f_names = list(filter(None, f_names))
        f_names = list(filter(lambda x: not '*' in x and not '(' in x, f_names))
        f_names = [x.strip().split(' ')[0] for x in f_names]
        f_names = list(set(f_names))
                
        return f_names
    
if __name__ == '__main__':
    te = Extractor()
    sqls ="""
        SELECT column1 AS renamed_column1, column2 AS renamed_column2 FROM table_name;
        SELECT column1, column2 FROM table_name;
        SELECT * FROM products WHERE release_date >= '2024-01-01';
        SELECT DISTINCT column1 FROM table_name;
        SELECT column1 FROM table_name WHERE age >10 AND column1="xxx";
        select * from table_name;
        SELECT column1 FROM table_name WHERE column2 = 'value';
        SELECT column1, COUNT(*) FROM table_name GROUP BY column1;
        SELECT column1, COUNT(*) FROM table_name GROUP BY column1 HAVING COUNT(*) > 1;
        SELECT column1 FROM table_name ORDER BY column1 ASC;
        SELECT column1 FROM table_name LIMIT 10;
        SELECT column1 FROM table_name LIMIT 10 OFFSET 20;
        SELECT column1 FROM table_name ORDER BY column1 FETCH FIRST 10 ROWS ONLY;
        SELECT 价格 FROM 产品信息表 WHERE 品牌 = '联想' AND 系列 = '小新' AND 产品名 LIKE '%小新%';
        SELECT * FROM products WHERE product_name LIKE '%鼠标%';
        SELECT * FROM products WHERE product_cate1 = 'Consumer electronics';
    """.split(";")
    for sql in sqls[:-1]:
        print(sql)
        d = te.extract(sql)
        print(te.get_fields(d))
        print(te.get_values(d))
    
   
    

