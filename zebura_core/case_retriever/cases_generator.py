# 通过LLM生成DB的golden cases
import os
import sys
sys.path.insert(0, os.getcwd())
import asyncio
import logging
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.prompt_loader import prompt_generator

class case_generator:
    def __init__(self):
        self.prompter = prompt_generator()
        self.llm = LLMAgent()
        logging.debug("case_generator init success")

    # 生成golden cases
    async def gen_cases(self,table_name=None) -> dict:

        dbSchema = self.prompter.get_dbSchema(table_name,style='lite')
        prompt = self.prompter.gen_default_prompt('db2nl')
        results = await self.llm.ask_query(dbSchema, prompt)

        return results
    
    async def gen_sql(self,table_name=None) -> dict:

        dbSchema = self.prompter.get_dbSchema(table_name,style='lite')
        prompt = self.prompter.gen_default_prompt('db2sql')
        results = await self.llm.ask_query(dbSchema, prompt)

        return results
    
# Example usage
if __name__ == '__main__':   
    gen = case_generator()
    table_name = 'products'
    results = asyncio.run(gen.gen_sql())
    print(results)