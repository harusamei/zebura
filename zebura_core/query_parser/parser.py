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
import logging
from zebura_core.query_parser.extractor import Extractor
from normalizer import Normalizer
from schema_linker import Sch_linking
from zebura_core.case_retriever.study_cases import CaseStudy
from zebura_core.LLM.prompt_loader import prompt_generator
from zebura_core.constants import D_TOP_GOODCASES as topK
from constants import D_TOP_GOODCASES as topK
from server.msg_maker import make_a_log

class Parser:

    def __init__(self):
        self.norm =Normalizer()
        self.te = Extractor()
        self.gc = CaseStudy()
        self.prompter = prompt_generator()

        cwd = os.getcwd()
        name = z_config['Training','db_schema']  # 'training\it\products_schema.json'
        self.sl = Sch_linking(os.path.join(cwd, name))
        logging.debug("Parser init success")
        

    # main function
    # table_name None, 为多表查询
    # todo, refine()
    async def apply(self, query, table_name=None) -> dict:
        # 1. Normalize the query to sql format by LLM
        if table_name is None:
            print(f"parse.apply()-> all tables, query:{query}")
        else:
            print(f"parse.apply()-> table:{table_name}, query:{query}")     

        resp = make_a_log("parse")
        # few shots from existed good cases
        gcases = self.find_good_cases(query,topK=topK)

        #得到 system prompt, fewshots prompt
        prompt1 = self.prompter.gen_sql_prompt_dial(gcases, table_name,style='lite')
     
        logging.info(f"parse.apply()-> generate prompt and call Normalizer for {table_name} and {query}")
        # query to sql
        answ = await self.norm.apply(query, prompt1['system'],prompt1['fewshots'])
        resp['msg'] = answ['msg']
        resp['status'] = answ['status']
        if answ['status'] == "failed":
            return resp

        sql_1 = resp['msg']
        # 2. Extract the slots from the query
        slots1 = self.te.extract(sql_1)
        # 3. Link the slots to the schema
        slots2 = self.sl.refine(slots1)
        # 3. revise the sql query by the slots
        sql2 = self.gen_sql(slots2)
        # sql1, slots1 为修正前，sql2, slots2 为修正后
        if slots1 is None:
            resp['status'] = "failed"

        merged_dict = {**resp, **{"sql1":sql_1,"sql2":sql2,"slots1":slots1, "slots2":slots2}}

        return merged_dict

    def find_good_cases(self,query,sql=None,topK=topK):
        # 从ES中获得候选 topK*1.5
        # {'doc':docs[id[0]], 'rank':i+1, 'score':id[1]}
        results = self.gc.assemble_find(query,sql,int(topK*1.5))
        # TODO 用score 过滤？rerank
        new_results = []
        for res in results[:topK]:
            new_results.append(res['doc'])
        
        return new_results
    
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

    querys = ['列出类别是电脑的产品名称','哪些产品属于笔记本类别？','列出所有的产品类别']
    querys =['帮我查一下小新的价格','查一下联想小新电脑的价格','查一下价格大于1000的产品']
    table_name = 'products'
    parser = Parser()
    for query in querys:
        result = asyncio.run(parser.apply(query))
        print(result)
