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
        
    async def ask_query_list(self, queries:list[str], prompt:str,shotInfo:str='') -> list[str]:
        # create a task list
        if len(queries) == 0:
            return []
        
        tasks = []
        # 只处理前1000
        prompt = prompt
        for query in queries[:1000]:
            query= query 
            task = asyncio.create_task(self.ask_query(query,prompt,shotInfo))
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
    
    async def ask_query(self,query:str, prompt:str,shotInfo="")->str:

        if query is None or len(query) == 0:
            return ""
        if len(shotInfo)>0:
            content = f'{prompt}\n{shotInfo}'
        else:
            content = prompt

        messages = [{"role": "system", "content": content}]
        messages.append({"role": "user", "content": query})

        #输出prompt 和 query check
        cur_loglevel = logging.getLogger().getEffectiveLevel()
        if cur_loglevel <=20:
            outFile = 'message.txt'
            with open(outFile, 'a', encoding='utf-8') as f:
                for message in messages:
                    f.write(f"{message['role']}: {message['content']}\n")
                f.write("----------------------------end\n")

        try:
            answer = self.postMessage(messages)
            return answer
        except Exception as e:
            return f"ERR_llm, {e.args[0]}"
        

    # # fewshots 单独时， shots是一个list，包含{user,assistant}
    # async def ask_query(self,query:str, prompt:str,shots=None)->str:
    #     logging.info(f"LLMAgent:ask_query() -> query: {query[:100]}, shots: {shots}")

    #     if query is None or len(query) == 0:
    #         return ""
    #     messages = [{"role": "system", "content": prompt}]
    #     if shots is not None:   # few shots 与 system prompt 分开
    #         for shot in shots:
    #             messages.append({"role": "user", "content": shot['user']})
    #             messages.append({"role": "assistant", "content": shot['assistant']})
    #     messages.append({"role": "user", "content": query}) 
    #     #输出prompt 和 query check
    #     cur_loglevel = logging.getLogger().getEffectiveLevel()
    #     if cur_loglevel <=20:
    #         outFile = 'message.txt'
    #         with open(outFile, 'a', encoding='utf-8') as f:
    #             for message in messages:
    #                 f.write(f"{message['role']}: {message['content']}\n")
    #             f.write("----------------------------end\n")
    #     try:
    #         answer = self.postMessage(messages)
    #         return answer
    #     except Exception as e:
    #         return f"ERR_llm, {e.args[0]}"

# Example usage  
if __name__ == '__main__':
    from zebura_core.LLM.prompt_loader import prompt_generator
    import time

    querys = [  "What is the price of a Lenovo Xiaoxin computer?",
                "How much does a Lenovo Xiaoxin computer cost?",
                "Which brand is the Xiaoxin computer?",
                "The weather is pretty nice today, don't you think?",
                "请问联想小新电脑多少钱",
                "联想小新电脑多少钱",
                "请问小新电脑是什么品牌的",
                "今天天气挺好的，你觉得呢？"]
    pg = prompt_generator()
    prompt,fewshots = pg.gen_nl2sql_prompt()
    agent = LLMAgent()
    start = time.time() 
    answers = asyncio.run(agent.ask_query(querys[1],prompt))
    print(answers)
    print(f"single query time: {time.time()-start}")
    start =time.time()
    results = asyncio.run(agent.ask_query_list(querys,prompt))
    for i, result in enumerate(results):
        print(f"query:{querys[i]}\n{result}")
    print(f"batch query time: {time.time()-start}")