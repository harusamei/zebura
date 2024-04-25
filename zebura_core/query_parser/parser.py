#######################################################################################
# query parser模块的主代码
# 功能： 将query解析为符合当前 db schema的SQL
# 需要信息： slots from extractor, good cases, schema of db, ask db, gpt
# 解析信息：['columns','from', 'conditions', 'distinct', 'limit', 'offset','order by','group by']
#######################################################################################
import os
import sys
sys.path.insert(0, os.getcwd())
from settings import z_config
from zebura_core.query_parser.extractor import Extractor
from normalizer import Normalizer
from schema_linker import Sch_linking
from activity_generator.study_cases import CaseStudy

class Parser:
        
    def __init__(self):
        sa="You are a SQL programmer, you can generate SQL queries based on natural language input."
        self.norm =Normalizer(sa)
        self.te = Extractor()
        self.gc = CaseStudy()

        cwd = os.getcwd()
        name = z_config['Tables','schema']  # 'datasets\products_schema.json'
        self.sl = Sch_linking(os.path.join(cwd, name))
        

    # main function
    async def apply(self, table_name, query) -> dict:
        
        
        # 1. Normalize the query to sql format by LLM
        if not self.norm.gen_prompts(table_name):
            print("ERR: no such table in schema")
            return {"status":False,"msg":"no such table in schema"}
        
        prompt_zh = self.norm.prompt["sql_zh"]
        # sql_1 存在失败可能，为None, 但继续
        sql_1 = await self.norm.apply(query, prompt_zh)
        
        results = self.find_good_cases(query,sql_1)
        isCred = self.is_credible(results,query)
        if sql_1 is None and isCred:
            sql_1 = results[0]['doc']['sql']
        # 2. Extract the slots from the query
        slots1 = self.te.extract(sql_1)
        # 3. Link the slots to the schema
        slots2 = self.sl.refine(slots1)
        # 3. revise the sql query by the slots
        sql2 = self.gen_sql(slots2)
        # sql1, slots1 为修正前，sql2, slots2 为修正后
        return {"status":True, "sql1":sql_1,"sql2":sql2,"slots1":slots1, "slots2":slots2}
    
    # 怎么知道检索的是对的？ todo
    def is_credible(self,results,query) -> bool:
        return True
    
    def find_good_cases(self,query,sql):
        results = self.gc.assemble_find(query,sql)
        return results
    
    # 简单合成，只做了select,form,where
    def gen_sql(self,slots):
        if slots is None:
            return None
        
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
    import asyncio

    querys = ['查一下联想小新电脑的价格','哪些产品属于笔记本类别？','查一下价格大于1000的产品']
    table_name = 'product'
    parser = Parser()
    for query in querys:
        result = asyncio.run(parser.apply(table_name, query))
        print(result)
