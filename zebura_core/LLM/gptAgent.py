import sys
import os
import requests
import json
import asyncio
sys.path.insert(0, os.getcwd())
import settings
class GPTAgent:
   
    def __init__(self, sa):
        # set self awareness/ 最底层的prompt
        self.sa = sa
        api_key = os.environ.get("GPT_AGENT_KEY")
        self.url = "https://openai-lr-ai-platform-cv.openai.azure.com/openai/deployments/IntentTest/chat/completions?api-version=2023-07-01-preview"
        self.header = {
            "Content-Type": "application/json",
            "api-key": api_key 
        }
        self.post_dict = {
            "frequency_penalty": 0,
            "presence_penalty": 0,
            "top_p": 0.95,
            "temperature": 0.95,
            "messages": [
                            {
                                "role": "system",
                                "content": sa
                            },
                            {
                                "role": "user",
                                "content": "Hello, what can you do?"
                            }
                        ]
            }
        try:
            response = requests.post(self.url, headers=self.header, data=json.dumps(self.post_dict))
            response.raise_for_status()  # Raise an error for bad responses
            result = response.json()

            # Simplify the output by extracting relevant information
            result = {
                "choices": result.get("choices", []),
                "usage": result.get("usage", {})
            }
            print(result.get("choices")[0].get("message").get("content"))
        except requests.exceptions.RequestException as err:
            raise ValueError(err)
        
    def update_sa(self, sa:str):
        old_sa = self.sa
        self.sa = sa
        return old_sa

    async def ask_query_list(self, queries:list[str], prompt:str) -> list[str]:
        # create a task list
        if len(queries) == 0:
            return []
        
        tasks = []
        # 只处理前1000
        prompt = self.sa+ prompt
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
        post_dict = self.post_dict
        post_dict["messages"] = messages
    
        try:
            response = requests.post(self.url, headers=self.header, data=json.dumps(post_dict))
            response.raise_for_status()  # Raise an error for bad responses
            result = response.json()

            # Simplify the output by extracting relevant information
            result = {
                "choices": result.get("choices", []),
                "usage": result.get("usage", {})
            }
            return result.get("choices")[0].get("message").get("content")
        except requests.exceptions.RequestException as err:
            return err


# Example usage  
if __name__ == '__main__':

    querys = ["我有一个电脑产品表，表名是products, column names有price, product name, cpu, release date, 下面句子如果与电脑相关，请转换为SQL查询， 如果不相关，请输出 not sql",
                "请问联想小新电脑多少钱",
                "联想小新电脑多少钱",
                "请问小新电脑是什么品牌的",
                "今天天气挺好的，你觉得呢？"]

    agent = GPTAgent("你是一个编程助手，可以将自然语言转化为SQL查询")

    answers = asyncio.run(agent.ask_query(querys[1],querys[0]))
    print(answers)
    results = asyncio.run(agent.ask_query_list(querys[1:],querys[0]))
    print(results)