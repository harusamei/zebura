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
        
    # table_name None, 为多表查询
    # 主函数， 将query归一化为SQL
    async def apply(self, query, table_name=None) -> dict:

        # if table_name is None:
        #     print(f"parse.apply()-> all tables, query:{query}")
        # else:
        #     print(f"parse.apply()-> table:{table_name}, query:{query}")     

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
     
        logging.info(f"parse.apply()-> table_name and query is {table_name} and {query}")
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

    querys = ['目前有哪些电子产品的折扣价格低于500元？','评分在4.5以上的产品有哪些？找出其中最高的不超过5个',
              '哪些产品的折扣最大？能推荐几款吗？','我想知道这款产品（ID为B09RFB2SJQ）的详细信息，包括名称、价格、折扣和评分。',
              '我想看看这款产品（ID为B09RFB2SJQ）的用户评论。', '用户RAMKISAN之前写的评论都在哪儿可以找到？',
              '手动搅拌机这款产品的评分有多少个？平均评分是多少？','电子产品分类下，评分最高的几款产品有哪些？'
              ]
    table_name = 'product'
    parser = Parser()
    for query in querys[0:1]:
        result = asyncio.run(parser.apply(query))
        print(f"query:{query}, result:{result}")
