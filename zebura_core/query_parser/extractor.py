##########################################################
# 对SQL进行解析，提取基本元素
##########################################################
import os
import sys
import re
sys.path.insert(0, os.getcwd())
import settings
from zebura_core.utils.sqlparser import parse_sql

class Extractor:
    def __init__(self):
        self.where_pats = [r'(\S+)\s+(LIKE)\s+\W(.*)\W', 
                           r'(\S+)\s*([><=]+)\s*(\d+)',
                           r'(\S+)\s*(=)\s*\W(.*)\W']


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
    
if __name__ == '__main__':
    te = Extractor()
    sql_querys ="""
        SELECT column1 FROM table_name WHERE age >10 AND column1="xxx"
        select * from table_name;
        SELECT column1, column2 FROM table_name;
        SELECT DISTINCT column1 FROM table_name;
        SELECT column1 FROM table_name WHERE column2 = 'value';
        SELECT column1, COUNT(*) FROM table_name GROUP BY column1;
        SELECT column1, COUNT(*) FROM table_name GROUP BY column1 HAVING COUNT(*) > 1;
        SELECT column1 FROM table_name ORDER BY column1 ASC;
        SELECT column1 FROM table_name LIMIT 10;
        SELECT column1 FROM table_name LIMIT 10 OFFSET 20;
        SELECT column1 FROM table_name ORDER BY column1 FETCH FIRST 10 ROWS ONLY;"
    """.split(";")
    # for sql in sql_querys:
    #     d = te.extract(sql)
    #     print(d) 
    sql ="SELECT 价格 FROM 产品信息表 WHERE 品牌 = '联想' AND 系列 = '小新' AND 产品名 LIKE '%小新%';"
    print(te.extract(sql))
   
    

