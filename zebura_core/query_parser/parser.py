# query parser，可利用的信息包含 slots from extractor, good cases, schema of db, ask db, gpt
import os
import sys
import re
sys.path.insert(0, os.getcwd())
import settings
from new_extractor import Extractor
from normalizer import Normalizer

#f"有一张表名为{table_name}，下面句子如果是关于查询{table_name}请转换为SQL查询，如果不是，请直接输出not sql"

class parser:
        
    def __init__(self):
        self.norm =Normalizer()
        self.te = Extractor()

    
    def parse(self, table_name, query):
        
        # 1. Normalize the query by sql format  
        sql_query = self.norm.convert_to_sql(table_name, query)
        if sql_query:
            return sql_query
        else:
            return "not sql"
        # 2. Extract the slots from the query
        slots = self.te.extract(sql_query)
        slots = self.te.refine(slots)
        
        return slots

if __name__ == '__main__':
    query = '请问联想小新电脑多少钱'
    table_name = 'product'
    parser = parser()
    result = parser.parse(table_name, query)
    print(result)