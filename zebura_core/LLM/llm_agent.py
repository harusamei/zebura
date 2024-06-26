import sys
import os
#import requests
#import json
import asyncio
sys.path.insert(0, os.getcwd())
import settings
from zebura_core.LLM.llm_base import LLMBase

class LLMAgent(LLMBase):
   
    def __init__(self, agentName="CHATANYWHERE", model="gpt-3.5-turbo"):
        super().__init__(agentName,model)
        

    async def ask_query_list(self, queries:list[str], prompt:str) -> list[str]:
        # create a task list
        if len(queries) == 0:
            return []
        
        tasks = []
        # 只处理前1000
        prompt = prompt
        for query in queries[:1000]:
            query= query 
            task = asyncio.create_task(self.ask_query(query,prompt))
            tasks = tasks + [task]

        print(f"total {len(tasks)} queries")    
        
        # 每次只执行100个任务
        batch_size = 100
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size])

        # get the result of the task
        results = [None]*len(tasks)
        for i, task in enumerate(tasks): 
            answer = task.result()
            results[i] = answer
        
        return results

    async def ask_query(self,query:str,prompt:str)->str:
        print(f"query: {query}")
        if query is None or len(query) == 0:
            return ""
        messages = [{"role": "system", "content": prompt}]
        messages.append({"role": "user", "content": query})  # Convert the list of queries to a string with newlines between the]
    
        try:
            answer = self.postMessage(messages)
            return answer
        except Exception as e:
            return e


# Example usage  
if __name__ == '__main__':
    import zebura_core.LLM.agent_prompt as ap
    print(ap.roles)
    querys = [  "请问联想小新电脑多少钱",
                "联想小新电脑多少钱",
                "请问小新电脑是什么品牌的",
                "今天天气挺好的，你觉得呢？"]

    agent = LLMAgent()
    answers = asyncio.run(agent.ask_query(querys[1],ap.roles["sql_assistant"]+ap.tasks["nl2sql"]))
    print(answers)
    results = asyncio.run(agent.ask_query_list(querys,ap.roles["sql_assistant"]+ap.tasks["nl2sql"]))
    print(results)