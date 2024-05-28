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
from LLM.llm_agent import LLMAgent
import LLM.agent_prompt as ap
from knowledges.schema_loader import Loader

class Normalizer:
    
    def __init__(self):
        # context for the LLM
        self.llm = LLMAgent()
        self.sch_loader = Loader(z_config['Training','db_schema'])
        logging.info("Normalizer init done")

    # main method of class, convert natural language to SQL
    async def apply(self,query:str,prompt:str):
        result = await self.convert_sql(query,prompt)
        if result is None:
            return None
        
        sql_list = self.extract_sql(result)
        if len(sql_list) == 0:
            return None
        return sql_list

    # 生成table details of prompts for nl2sql
    # table_name, cloumn_name， 是DB的正式名，且作为英文名
    def gen_dbInfo(self, table_name) -> dict:
        
        table_info = self.sch_loader.get_table_info(table_name)
        if table_info is None:
            return None
        
        desc = table_info.get("desc",'')
        columns = self.sch_loader.get_all_columns(table_name)
        columnInfo = [f"{c.get('name_zh','')}: {c.get('desc','')}" for c in columns]
        columnInfo = "\n".join(columnInfo)
        db_zh = (
                    f"表名是{table_info.get('name_zh','')}，用途是{desc}，包含的"
                    f"列及其含义如下\n{columnInfo}\n"
                )
        
        columnInfo = [f"{c.get('column_name','')}， {c.get('desc','')}" for c in columns]
        columnInfo = "\n".join(columnInfo)
        # nothing, 语句太长，分开写
        db_en = (
                    f"The table name is {table_name},the purpose is {desc}, "
                    f"the columns name are as follows:\n{columnInfo}\n"
                )
        
        return {   
                "zh":db_zh,
                "en":db_en
            }
      
    # 不确定数据信息prompt summary一下，是否效果更好？
    async def summary(self,content,lang="zh"):
        
        if lang=="zh":
            prompt = ap.lang_mappings["zh_doc_assistant"]
            prompt += ap.lang_mappings["zh_summary"]
        else:
            prompt = ap.roles["doc_assistant"] + ap.tasks["summary"]
        result = await self.llm.ask_query(content, prompt)
        return result

    async def ask_agent(self, querys, prompt):
        results = await self.llm.ask_query_list(querys, prompt)
        return results
    
    # 提取SQL代码, 提取sql 全部小写
    def extract_sql(self,result:str):
        # Extract the SQL code from the LLM result
        logging.info(f"extract sql from LLM result: {result}")
        result = result.lower()
        if result.startswith("```sql"):
            code_pa = "```sql\n(.*?)\n```"
        elif 'select' in result:
            code_pa = "(select.*?from.*?where.*)"

        matches = re.findall(code_pa, result, re.DOTALL)
        return matches
        
    async def convert_sql(self,queries,prompt):
        # Ask the GPT agent to convert the query to SQL

        if isinstance(queries, str):
            results = [await self.llm.ask_query(queries, prompt)]
        else:
            results = await self.ask_agent(queries, prompt)
            if len(results) != len(queries):
                print(f"Error: Number of queries{len(queries)} and results {len(results)} do not match")

        # filter the successful SQL
        for i in range(len(results)):
            if not isinstance(results[i],str):
                print("ERR: no str",queries[i], results[i])
                results[i] = ""
        results = [
                    r if re.search(r'SELECT|FROM|WHERE', r, re.IGNORECASE) 
                    else None for r in results
                  ]
        print("converse sql done")
        # input str, output str; input list output list
        if isinstance(queries, str):
            return results[0]
        else:
            return results
    
    # 补全
    async def rewrite(self, queries, prompt):
        if isinstance(queries, str):
            results = [await self.llm.ask_query(queries, prompt)]
        else:
            results = await self.ask_agent(queries, prompt)
            if len(results) != len(queries):
                print("ERR: queries and results do not match")
        return results
    
    # 中英文提示全跑，失败的再跑rewrite
    async def bulk_sql(self, queries, prompt_zh="", prompt_en=""):
        sql_zh =prompt_zh
        sql_en = prompt_en
        results = await self.convert_sql(queries,sql_zh)
        en_results = await self.convert_sql(queries,sql_en)

        rewrite =[None]*len(queries)
        hard_ids = []
        for i, result in enumerate(results):
            if result is None and en_results[i] is None and len(queries[i])>0:
                hard_ids.append(i)

        print(f" fail sql ratio: {len(hard_ids)/len(queries)}")
        hard_queries = [queries[i] for i in hard_ids]
        
        prompt = ap.roles["doc_assistant"]+ap.tasks["rewrite"]
        new_queries= await normalizer.rewrite(hard_queries,prompt)
        hard_results = await normalizer.convert_sql(new_queries,sql_en)
        for i,result in enumerate(hard_results):
            en_results[hard_ids[i]]=result
            rewrite[hard_ids[i]]=new_queries[i]

        return results,en_results, rewrite

# Example usage
if __name__ == '__main__':
    from utils.csv_processor import pcsv
    normalizer = Normalizer()
    
    cp = pcsv()
    rows = cp.read_csv('sql_result.csv')
    
    prompts = normalizer.gen_dbInfo('product')
    sql_zh = f"{ap.roles['sql_assistant']}\n{ap.tasks['nl2sql']}\n{prompts['zh']}\n"
    sql_en = f"{ap.roles['sql_assistant']}\n{ap.tasks['nl2sql']}\n{prompts['en']}\n"
    queries = [row['query'] for row in rows]
    results,en_results, rewrite = asyncio.run(normalizer.bulk_sql(queries,sql_zh,sql_en))
    print(f'query:{len(queries)}, results: {len(results)}, rewrite:{len(rewrite)}')
    count = 0
    for i, row in enumerate(rows):
        if results[i] is not None:
            rows[i]['sql_zh'] = results[i]
        if en_results[i] is not None:
            rows[i]['sql_en']= en_results[i]
        if en_results[i] is not None or results[i] is not None:
            count+=1
        rows[i]['rewrite']=rewrite[i]
    print(f"success rate: {count/len(queries)}")
    cp.write_csv(rows, 'sql_result.csv')
