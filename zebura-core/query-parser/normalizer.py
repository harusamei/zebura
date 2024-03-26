# 利用LLM将自然语言转换为SQL，以SQL作为句子的规范化形式
import sys
import os
import asyncio
import re
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from LLM.gptAgent import askGPT_agent

class normalizer:
    
    def __init__(self,tabel_name,shema):
        self.table_name = tabel_name
        self.shema = shema
        self.prompt = f"有一张表名为{tabel_name}，下面句子如果是关于查询{tabel_name}请转换为SQL查询，如果不是，请直接输出not sql"

    
    def convert_to_sql(self,query):
        # Ask the GPT agent to convert the query to SQL
        querys = [self.prompt,query]          
        answers = asyncio.run(askGPT_agent(querys,"你是一个编程助手，可以将自然语言转化为SQL查询："))
        result = answers.get("choices")[0].get("message").get("content")
        # Use regex to find the SQL query
        match = re.search(r'SELECT.*;', result, re.IGNORECASE)
        if match:
            return match.group()
        else:
            return ""

# Example usage
if __name__ == '__main__':
    query = '请问联想小新电脑多少钱'
    normalizer = normalizer('products','')
    result = normalizer.convert_to_sql(query)
    print(result)
