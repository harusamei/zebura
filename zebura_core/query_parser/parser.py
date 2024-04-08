# query parser，可利用的信息包含 slots from extractor, good cases, schema of db, ask db, gpt
import os
import sys
import re
sys.path.insert(0, os.getcwd())
import settings
from new_extractor import Extractor
from normalizer import Normalizer
from schemalinking import Sch_linking

class Parser:
        
    def __init__(self):
        self.norm =Normalizer()
        self.te = Extractor()
        self.sl = Sch_linking()

    
    def parse(self, table_name, query):
        
        # 1. Normalize the query by sql format  
        sql_query = self.norm.convert_to_sql(table_name, query)
        if not sql_query:
            return "not sql"
        # 2. Extract the slots from the query
        slots = self.te.extract(sql_query)
        slots = self.sl.refine(slots)
        
        return slots

if __name__ == '__main__':
    query = '请问联想小新电脑多少钱'
    table_name = '产品表'
    parser = Parser()
    result = parser.parse(table_name, query)
    print(result)