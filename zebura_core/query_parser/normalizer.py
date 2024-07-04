##########################################################
# 利用LLM将自然语言转换为SQL，以SQL作为句子的规范化形式
##########################################################
import sys
import os
import asyncio
import re
sys.path.insert(0, os.getcwd())
from settings import z_config
import logging
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.knowledges.schema_loader import Loader
from zebura_core.LLM.prompt_loader import prompt_generator

class Normalizer:
    
    def __init__(self):
        # context for the LLM
        self.llm = LLMAgent()
        self.sch_loader = Loader(z_config['Training','db_schema'])

        logging.debug("Normalizer init done")

    # main method of class, convert natural language to SQL
    # C_ERR_TAGS = ['ERR: LLM','ERR: NOSQL','ERR: CURSOR']    # error tags
    async def apply(self,query:str,prompt:str,fewshots=None) -> dict:

        logging.debug(f"normalizer.apply()-> query:{query}, prompt:{prompt[:100]}")

        result = await self.convert_sql(query,prompt,fewshots)
        # 结果有三种情况， LLM无回应，no sql，提取SQL   
        resp = {"status":"failed","msg":result,"from":"convert_sql"}
        if 'ERR' in result:
            resp["note"] = "ERR: LLM"
        elif "nosql" in result.lower():
            resp["note"] = "ERR: NOSQL"
            result = re.sub(r'nosql\W*', '', result, flags=re.IGNORECASE)
            resp["hint"] = result
        else:
            resp = self.extract_sql(result)
            resp["from"] = "extract_sql"
        return resp
 
    async def ask_agent(self, querys, sys_prompt,fewshots=None):
        if isinstance(querys,str):
            results = await self.llm.ask_query(querys, sys_prompt,fewshots)
        elif isinstance(querys,list):
            results = await self.llm.ask_query_list(querys, sys_prompt)
            if len(results) != len(querys):
                print("ERR: queries and results do not match")
        else:
            print("ERR: queries is not string or list")
            return None
        return results
    
    # 提取SQL代码, 提取sql 全部小写
    def extract_sql(self,result:str) -> dict:
        # Extract the SQL code from the LLM result
        logging.info(f"extract sql from LLM result: {result}")
        if not isinstance(result, str):
            print("ERR: result is not string")
            return {"status":"failed","msg":"ERR: [wrong format]"}
        
        if result.lower().startswith("```sql"):
            code_pa = "```sql\n(.*?)\n```"      # 标准code输出
        elif 'select' in result.lower():
            result = re.sub('\n|\t',' ', result)
            result = re.sub(' +', ' ', result)
            code_pa = "(select.*?from[^;]+;)"  # 不一定有where
        else:
            print("ERR: no sql found in result")
            return {"status":"failed","msg":"ERR: NOSQL"}
        matches = re.findall(code_pa, result, re.DOTALL | re.IGNORECASE)
        if len(matches) == 0:
            print("ERR: no sql found in result")
            return {"status":"failed","msg":"ERR: NOSQL"}
        else:
            return {"status":"succ","msg":matches[0]}
    
    # LLM 只负责转换，不对结果进行处理   
    async def convert_sql(self,queries,sys_prompt,fewshots=None):
        # Ask the GPT agent to convert the query to SQL
        results = await self.ask_agent(queries, sys_prompt,fewshots)
        
        return results
   
    # 
    async def bulk_sql(self, queries, prompt_en=""):
        if queries is None or len(queries)==0:
            return None, None, None
        
        results = await self.convert_sql(queries,prompt_en)

        hard_ids = []
        for i, result in enumerate(results):
            if result is None and results[i] is None and len(queries[i])>0:
                hard_ids.append(i)

        print(f" fail sql ratio: {len(hard_ids)/len(queries)}")
        hard_queries = [queries[i] for i in hard_ids]

        return results

# Example usage
if __name__ == '__main__':
    from utils.csv_processor import pcsv
    import time

    normalizer = Normalizer()
    prompter = prompt_generator()
    query ="A: SELECT * FROM products WHERE goods_status = 'Newly released';\nQ: 有什么与鼠标有关的产品\nA: SELECT * FROM products WHERE product_name LIKE '%鼠标%';"
    print(normalizer.extract_sql(query))
    gcases =[]
    promptInfo = prompter.gen_sql_prompt_fewshots(gcases)
    prompt_en = promptInfo['system']

    cp = pcsv()
    rows = cp.read_csv('tests/sql_test.csv')
    queries = [row['query'] for row in rows]
    start = time.time()
    results = asyncio.run(normalizer.bulk_sql(queries,prompt_en))
    print(f"bulk sql done, time: {time.time()-start}")
    print(f'query:{len(queries)}, results: {len(results)}')
    count = 0
    for i, row in enumerate(rows):
        if results[i] is not None:
            rows[i]['sql_new'] = results[i]
            count+=1
        
    print(f"success rate: {count/len(queries)}")
    cp.write_csv(rows, 'tests/sql_test.csv')
