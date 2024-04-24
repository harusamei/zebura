#######################################################################################
# query parser模块的主代码
# 功能： 将query解析为符合当前 db schema的SQL
# 需要信息： slots from extractor, good cases, schema of db, ask db, gpt
#######################################################################################
import os
import sys
sys.path.insert(0, os.getcwd())
from settings import z_config
from new_extractor import Extractor
from normalizer import Normalizer
from schema_linker import Sch_linking

class Parser:
        
    def __init__(self):
        self.norm =Normalizer()
        self.te = Extractor()

        cwd = os.getcwd()
        name = z_config['Tables','schema']  # 'datasets\products_schema.json'
        self.sl = Sch_linking(os.path.join(cwd, name))

    
    def parse(self, table_name, query):
        
        # 1. Normalize the query by sql format  
        sql_query = self.norm.convert_sql(table_name, query)
        if not sql_query:
            return "not sql"
        # 2. Extract the slots from the query
        slots1 = self.te.extract(sql_query)
        # 3. Link the slots to the schema
        slots2 = self.sl.refine(slots1)
        # 3. revise the sql query by the slots
        sql2 = self.gen_sql(slots2)
        # sql1, slots1 为修正前，sql2, slots2 为修正后
        return {"sql1":sql_query,"sql2":sql2,"slots1":slots1, "slots2":slots2}
    
    # 简单合成，只做了select,form,where
    def gen_sql(self,slots):
        # "select * from 产品表 where "
        str_from = 'from '
        str_from += slots['from']
        str_select = 'select '
        str_select+= ",".join(slots["columns"])
        if slots['distinct']:
            str_select = str_select.replace("select","select distinct")
        str_where = 'where '
        for cond in slots['conditions']:
            if isinstance(cond,dict):
                if cond['value'].isdigit():
                    str_where += f"{cond['column']} {cond['op']} {cond['value']}  "
                else:
                    str_where += f"{cond['column']} {cond['op']} '{cond['value']}' "
            else:
                str_where += f"{cond} "

        return f"{str_select} {str_from} {str_where}"


# Example usage
if __name__ == '__main__':
    query = '请从产品表里查一下联想小新电脑的价格'
    table_name = '产品表'
    parser = Parser()
    result = parser.parse(table_name, query)
    print(result)
