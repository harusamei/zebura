import os
import sys
import re
sys.path.insert(0, os.getcwd())
import settings
from tools.sqlparser import parse_sql
from knowledges.info_loader import InfoLoader
from compare import similarity

class Extractor:
    def __init__(self):
        self.info_loader = InfoLoader()
        self.where_pats = [r'(\S+)\s+(LIKE)\s+\W(.*)\W', 
                           r'(\S+)\s*([><=]+)\s*(\d+)',
                           r'(\S+)\s*(=)\s*\W(.*)\W']
        self.similarity = similarity()


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

    def substitute(self, term, tableName='', type='table'):

        if type not in ['table', 'column']:
            raise ValueError(f"Unknown type {type}")
        
        subst = {'conf': 0, 'new_term': ""}
        if type == 'table':
            tables = self.info_loader.tables
            for table in tables:
                db_name = table['table']
                temStr = f"{db_name},{table['table_zh']},{table['alias_en']},{table['alias_zh']}"
                termList = temStr.split(',')    
                s = self.similar(term,termList)
                if (s['score'] > subst['conf']):
                    subst['conf'] = s['score']
                    subst['new_term'] = db_name
            if subst['new_term']=='':
                subst['new_term'] = tables[0]['table']
            return subst

        columns_dict = self.info_loader.get_columnList(tableName)
        for col in columns_dict:
            db_colName = col['column_en']
            temStr = f"{db_colName},{col['column_zh']},{col['alias_en']},{col['alias_zh']}"
            termList = temStr.split(',')
            s = self.similar(term,termList)
            if (s['score'] > subst['conf']):
                print(s)
                subst['conf'] = s['score']
                subst['new_term'] = db_colName

        if subst['new_term']=='':
            subst['new_term'] = columns_dict[0]['column_en']
        return subst
    
    def refine(self,slots):

        tableName = slots['from']
        subst = self.substitute(tableName)
        tableName = slots['from'] = subst['new_term']
        if subst['conf'] < 0.5:
            slots['from'] += '?'

        columns = slots['columns']
        for idx, column in enumerate(columns):
            subst = self.substitute(column, tableName, 'column')
            columns[idx] = subst['new_term']
            if subst['conf'] < 0.5:
                columns[idx] += '?'

        # condictions
        for cond in slots['conditions']:
            subst = self.substitute(cond['column'], tableName, 'column')
            cond['column'] = subst['new_term']
            if subst['conf'] < 0.5:
                cond['column'] += '?'

        return slots


    def similar(self,term,candidates):
        lang = self.similarity.getLang(term)
        candidates = [c for c in candidates if self.similarity.getLang(c) == lang]
        matched = {'term':candidates[0], 'score':0}
       
        for ref in candidates:
            s = self.similarity.getUpperSimil(term,ref)
            if s > matched['score']:
                matched['score'] = s
                matched['term'] = ref
        return matched
    
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
    # for sql in sql_querys:
    #     d = te.extract(sql)
    #     print(d) 
    d = te.extract("SELECT Category FROM Products WHERE ProductName = '鼠标';")
    print(d)
    d = te.refine(d)
    print(d)

