import sys
import os
import asyncio
sys.path.insert(0, os.getcwd())
import settings
from zebura_core.LLM.llm_base import LLMBase
import logging

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

# 调用例子
# response = openai.ChatCompletion.create(
#       model=MODEL,
#       messages=[
#         {"role": "system", "content": "You are a helpful, pattern-following assistant."},
#         {"role": "user", "content": "Help me translate the following corporate jargon into plain English."},
#         {"role": "assistant", "content": "Sure, I'd be happy to!"},
#         {"role": "user", "content": "New synergies will help drive top-line growth."},
#         {"role": "assistant", "content": "Things working well together will increase revenue."},
#         {"role": "user", "content": "Let's circle back when we have more bandwidth to touch base on opportunities for increased leverage."},
#         {"role": "assistant", "content": "Let's talk later when we're less busy about how to do better."},
#         {"role": "user", "content": "This late pivot means we don't have time to boil the ocean for the client deliverable."},
#     ],
#     temperature=0,
# )
    # fewshots 单独时， shots是一个list，包含{user,assistant}
    async def ask_query(self,query:str, prompt:str,shots=None)->str:
        logging.info(f"ask_query() -> query: {query}, shots: {shots}")

        if query is None or len(query) == 0:
            return ""
        messages = [{"role": "system", "content": prompt}]
        if shots is not None:   # few shots 与 system prompt 分开
            for shot in shots:
                messages.append({"role": "user", "content": shot['user']})
                messages.append({"role": "assistant", "content": shot['assistant']})
        messages.append({"role": "user", "content": query}) 
    
        try:
            answer = self.postMessage(messages)
            return answer
        except Exception as e:
            return e


# Example usage  
if __name__ == '__main__':
    from zebura_core.LLM.prompt_loader import prompt_generator
    
    querys = [  "What is the price of a Lenovo Xiaoxin computer?",
                "How much does a Lenovo Xiaoxin computer cost?",
                "Which brand is the Xiaoxin computer?",
                "The weather is pretty nice today, don't you think?"]
    querys1 =[   "请问联想小新电脑多少钱",
                "联想小新电脑多少钱",
                "请问小新电脑是什么品牌的",
                "今天天气挺好的，你觉得呢？"]
    pg = prompt_generator()
    prompt = pg.gen_sql_prompt(style='lite')
    agent = LLMAgent()
    answers = asyncio.run(agent.ask_query(querys[1],prompt))
    print(answers)
    results = asyncio.run(agent.ask_query_list(querys,prompt))
    print(results)