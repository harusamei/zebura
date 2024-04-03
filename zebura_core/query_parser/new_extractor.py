import os
import sys
import re
sys.path.insert(0, os.getcwd())
import settings
from tools.sqlparser import parse_sql
from knowledges.info_loader import InfoLoader

class Extractor:
    def __init__(self):
        self.info_loader = InfoLoader()
        self.where_pats = [r'(\S+)\s+(LIKE)\s+\W(.*)\W', 
                           r'(\S+)\s*([><=]+)\s*(\d+)',
                           r'(\S+)\s*(=)\s*\W(.*)\W']


    def extract(self,sql):
        
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

    def refine(self,slots):
        # check table name
        table = slots['from']
        db_tables = self.info_loader.tables
        flag = False
        for db_table in db_tables:
            db_name = db_table['table']
            termList =','.split(f"{db_name},{db_table['table_zh']},{db_table['alias_en']},{db_table['alias_zh']}")
            s = self.similar(table,termList)
            if (s.score >0.8):
                slots['from'] = db_name
                flag = True
                break
        if not flag:
            slots['from'] = slots['from']+'?'
            return slots
           
        columns = slots['columns']
        for cond in slots['conditions']:
            if cond['column'] not in columns:
                columns.append(cond['column'])
        # check column name
        table = slots['from']
        flag = False
        for column in columns:
            for db_column in self.info_loader.get_columnList(table):
                db_colName = db_column['column_en']
                termList =','.split(f"{db_colName},{db_column['column_zh']},{db_column['alias_en']},{db_column['alias_zh']}")
                s = self.similar(column,termList)
                if (s.score >0.8):
                    columns[columns.index(column)] = db_colName
                    flag = True
                    break
            if not flag:
                columns[columns.index(column)] = column+'?'
    
                
    def similar(self,term,termList):
        pass
        return 0
    
    def parse_cond(self,cond):
        parsed_condition ={'column':"", 'operator':"", 'value':""}
        pts = self.where_pats
        matched = False
        for pt in pts:
            match = re.search(pt, cond, re.IGNORECASE)
            if match:
                parsed_condition['column'] = match.group(1)
                parsed_condition['operator'] = match.group(2)
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
    for sql in sql_querys:
        d = te.extract(sql)
        print(d) 