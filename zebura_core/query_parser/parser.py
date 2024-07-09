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
from normalizer import Normalizer
from zebura_core.case_retriever.study_cases import CaseStudy
from zebura_core.LLM.prompt_loader import prompt_generator
from zebura_core.constants import D_TOP_GOODCASES as topK
from constants import D_TOP_GOODCASES as topK
from server.msg_maker import make_a_log

class Parser:

    def __init__(self):
        self.norm =Normalizer()
        self.gc = CaseStudy()
        self.prompter = prompt_generator()
        logging.debug("Parser init success")
        
    # main function
    # table_name None, 为多表查询

    async def apply(self, query, table_name=None) -> dict:
        # 1. Normalize the query to sql format by LLM
        if table_name is None:
            print(f"parse.apply()-> all tables, query:{query}")
        else:
            print(f"parse.apply()-> table:{table_name}, query:{query}")     

        resp = make_a_log("parse")
        # few shots from existed good cases
        # TODO: build amazon_gcases index
        ##################
        gcases = [] #self.find_good_cases(query,topK=topK)
        for case in gcases:
            if case.get('qemb'):
                del case['qemb']
        #得到 system prompt, fewshots prompt
        prompt = self.prompter.gen_sql_prompt_fewshots(gcases, table_name)
     
        logging.info(f"parse.apply()-> generate prompt and call Normalizer for {table_name} and {query}")
        # query to sql
        answ = await self.norm.apply(query, prompt['system'],prompt['fewshots'])
        
        resp['msg'] = answ['msg']
        resp['status'] = answ['status']
        resp['note'] = answ.get('note','')
        resp['others']['gcases'] = gcases
        if answ['status'] == "failed":
            resp['hint'] = answ.get('hint','')
        return resp
   
    def find_good_cases(self,query,sql=None,topK=topK):
        # 从ES中获得候选 topK*1.5
        # {'doc':docs[id[0]], 'rank':i+1, 'score':id[1]}
        results = self.gc.assemble_find(query,sql,int(topK*1.5))
        # TODO 用score 过滤？rerank
        new_results = []
        for res in results[:topK]:
            new_results.append(res['doc'])
        
        return new_results
    
# Example usage
if __name__ == '__main__':
    import asyncio

    querys = ['Find the types of fans available in the database','电脑都是多少价格的','hello','查一下联想小新电脑的价格','帮我查一下小新的价格','What computer brands are available for me to choose from?','列出类别是电脑的产品名称','哪些产品属于笔记本类别？',
              '列出所有的产品类别','查一下价格大于1000的产品']
    table_name = 'product'
    parser = Parser()
    for query in querys:
        result = asyncio.run(parser.apply(query))
        print(f"query:{query}, result:{result}")
