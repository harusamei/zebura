#######################################################################################
# query parser模块的主代码
# 功能： 将query解析为符合当前 db schema的SQL
# 需要信息： slots from extractor, good cases, schema of db, ask db, gpt
# 解析信息：['columns','from', 'conditions', 'distinct', 'limit', 'offset','order by','group by']
#######################################################################################
import os
import sys
# sys.path.insert(0, os.getcwd())
from settings import z_config
from zebura_core.query_parser.extractor import Extractor
from zebura_core.query_parser.normalizer import Normalizer
from zebura_core.query_parser.schema_linker import Sch_linking
from zebura_core.query_parser.study_cases import CaseStudy
import zebura_core.LLM.agent_prompt as ap

class Parser:
        
    def __init__(self):
        self.norm =Normalizer()
        self.te = Extractor()
        self.gc = CaseStudy()

        # cwd = os.getcwd()
        name = z_config['Training','db_schema']  # 'training\it\products_schema.json'
        self.sl = Sch_linking(name)
        

    # main function
    async def apply(self, table_name, query) -> dict:
         
        # 1. Normalize the query to sql format by LLM
        prompts = self.norm.gen_dbInfo(table_name)
        if prompts is None:
            print("ERR: no such table in schema")
            return {"status":False,"msg":"no such table in schema"}
        # prompt组成：self awareness + task description + table schema
        prompt_zh = (
            f'{ap.roles["sql_assistant"]}\n{ap.tasks["nl2sql"]}\n'
            f'specific details about the database schema:\n{prompts["sql_zh"]}'
        )
        
        # few shots from existed good cases
        results = self.find_good_cases(query,topK=3)
        shot_prompt = self.gen_shots(results)
        prompt_zh += "\n"+shot_prompt
        print("prompt_zh:",prompt_zh)
        # sql_1 失败为None
        sql_1 = await self.norm.apply(query, prompt_zh)
        if sql_1 is None:
            print("ERR: failed to normalize query")
            return {"status":False,"msg":"no sql query generated"}
        
        # 2. Extract the slots from the query
        slots1 = self.te.extract(sql_1)
        # 3. Link the slots to the schema
        slots2 = self.sl.refine(slots1)
        # 3. revise the sql query by the slots
        sql2 = self.gen_sql(slots2)
        # sql1, slots1 为修正前，sql2, slots2 为修正后
        return {"status":True, "sql1":sql_1,"sql2":sql2,"slots1":slots1, "slots2":slots2}
    
    def gen_shots(self,results):
        shot_prompt = ""
        for res in results:
            shot_prompt += f"Q: {res['doc']['query']}\n"
            shot_prompt += f"A: {res['doc']['sql']}\n\n"
        return shot_prompt
    
    
    def find_good_cases(self,query,sql=None,topK=5):
        return self.gc.assemble_find(query,sql,topK)
    
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
        if result["status"] is True:
            print(result["sql1"][0])