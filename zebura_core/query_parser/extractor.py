import os
import sys
import re
sys.path.insert(0, os.getcwd())
import settings
import info_loader

class Extractor:

    def __init__(self):
        self.loader = info_loader.PatLoader()
       
    def create_empty_slots(self):
        slots = dict.fromkeys(self.loader.sql_pats.keys())
        return slots
    
    def extract_info(self,sql):
        if sql[-1] != ';':
            sql += ';'
        # Regular expression pattern to match sql statements
        steps = ['select','sql_from', 'where', 'distinct', 'limit', 'offset', 'group_by', 'order_by', 'as', 'like']
        slots = self.create_empty_slots()
        for step in steps:
            pat = self.loader[step]   
            if not pat:
               print(f"Error: {step} pattern is not found")
               continue
            # Extract the table name and columns using regex
            match = re.search(pat, sql, re.IGNORECASE)
            if match:
                slots[step] = match.group(1)

        # refine the slots
        slots['select all'] = True if slots['select'] == '*' else False
        slots['distinct'] = True if slots['distinct'] else False
        str = slots['select']
        slots['select'] = str.split(',') if str else []
        return slots
            
# 示例SQL语句
sql_querys ="""
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

# Example usage
if __name__ == '__main__':
    te = Extractor()
    for sql in sql_querys:
        print(sql)
        d = te.extract_info(sql)
        print(d)
    
