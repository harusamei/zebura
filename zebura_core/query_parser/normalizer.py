# 利用LLM将自然语言转换为SQL，以SQL作为句子的规范化形式
import sys
import os
import asyncio
import re
sys.path.insert(0, os.getcwd())
import settings
from LLM.gptAgent import askGPT_agent

class Normalizer:
    
    def __init__(self):
        pass

    def gen_prompt(self, table_name):
        return f"有一张表名为{table_name}，下面句子如果查询{table_name}请转换为SQL语句，如果不是，请直接输出not sql"

    def askGPT_agent(self, querys, prompt):
        answers = asyncio.run(askGPT_agent(querys,prompt))
        return answers.get("choices")[0].get("message").get("content")
    
    def convert_to_sql(self,table_name, query):
        # Ask the GPT agent to convert the query to SQL
        querys = [query]
        prompt = self.gen_prompt(table_name)        
        result = self.askGPT_agent(querys, prompt)
        # Use regex to find the SQL query
        match = re.search(r'SELECT.*;', result, re.IGNORECASE)
        if match:
            return match.group()
        else:
            return ""

# Example usage
if __name__ == '__main__':
    query = '有没有价格低于50的鼠标'
    normalizer = Normalizer()
    result = normalizer.convert_to_sql('product',query)
    print(result)
