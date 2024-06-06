# 与chatbot交互的接口, 内部是一个总控制器，负责调度各个模块最终完成DB查询，返回结果
import sys
import os
sys.path.insert(0, os.getcwd().lower())
import settings
import multiprocessing    

import logging
import asyncio
import inspect
from zebura_core.query_parser.parser import Parser
from zebura_core.answer_refiner.synthesizer import Synthesizer
# from zebura_core.answer_refiner.explainer import Explainer  
from zebura_core.activity_executor.executor import Executor
from zebura_core.LLM.llm_agent import LLMAgent

# 一个传递request的pipeline
# 从 Chatbot request 开始，到 type变为assistant 结束
# request={
#         "msg": content,
#         "context": context,
#         "type": "user/assistant/transaction", # 用户， controller, 增加 action间切换
#         "format": "text/md/sql...", # content格式，与显示相关
#         "status": "new/hold/failed/succ", # 新对话,多轮继续；执行失败；执行成
#         
# 增加      "from": "nl2sql/sql4db/interpret/polish" # 当前任务
# 增加      "others": 当前步骤产生的次要信息 # 下一个任务
#     } 

class Controller:
    llm = LLMAgent("AZURE","gpt-3.5-turbo")
    parser = Parser()
    st_matrix = {
            "(new,user)": "nl2sql",
            "(hold,user)": "rewrite",
            "(succ,rewrite)": "nl2sql",
            "(succ,nl2sql)": "sql4db",
            "(succ,sql4db)": "polish",
            "(succ,polish)" : "genAnswer",
            "(failed,end)" : "genAnswer",
            "(succ,end)" : "genAnswer",
            "(failed,*)"  : "transit",   # 失败则重新设置状态
            "(*,*)" : "genAnswer"        # no way
    }
    
    def __init__(self):
        
        self.matrix = Controller.st_matrix
        self.parser = Controller.parser
        self.prompter = Controller.parser.prompter      # prompt generator
        self.asw_refiner = Synthesizer()
        self.executor = Executor()

        logging.info(f"Controller init success")

    def get_next(self,pipeline):

        lastLog = pipeline[-1]
        curSt =f'({lastLog["status"]},{lastLog["from"]})'
        count = 1
        while curSt not in self.matrix and count < 4:
            if count == 1:
                curSt = f'({lastLog["status"]},*)'
            if count == 2:
                curSt = f'(*,{lastLog["from"]})'
            if count == 3:
                curSt = f'(*,*)'
            count += 1
        return self.matrix[curSt]
    
    async def nl2sql(self, pipeline):

        log = pipeline[-1]
        content = log['msg']
        result = await self.parser.apply(content)
        
        new_Log = {
            'msg':result['msg'],
            'status': result["status"],
            'from':"nl2sql",
            'type':'transaction',
            'others': result
        }
        pipeline.append(new_Log)

    async def rewrite(self,pipeline):

        history=[]
        log = pipeline[-1]
        if log.get('context') is not None:
            context = log['context']
            for one_req in context[-3:]:
                msg = f"{one_req['type']}: {one_req['msg']}"
                history.append(msg)
        
        history_context= "\n".join(history)
        query = log['msg']
        template = self.prompter.gen_default_prompt("rewrite")
        prompt = template.format(history_context=history_context,query=query)
        result = await self.llm.ask_query(query, prompt)
        
        print(result)
        new_Log = dict(log)
        new_Log['from'] ='rewrite'
        new_Log['msg'] = result
        new_Log['status'] = 'succ'
        new_Log['type'] ='transaction'
        
        pipeline.append(new_Log)

    def transit(self,pipeline):
        log = pipeline[-1]
        new_Log = dict(log)
        new_Log['from'] ='end'
        
        pipeline.append(new_Log)
            
    def genAnswer(self,pipeline):
        log = pipeline[-1]
        response = dict(log)
        response['type'] = 'assistant'
        response["format"]= "text"

        if log['status'] == "failed":
            response['msg'] += "\n please tell me more details for your database."    
        pipeline.append(response) 

    # 查库
    def sql4db(self,queue):
        sql = queue.get()
        queue.put("sql4db finsihed")

    #上一步执行不成功，给出解释
    def interpret(self,queue):
        last_step = queue.get()
        queue.put("interpret finsihed")

    # 生成答案
    def polish(self,queue):
        result = queue.get()
        queue.put("polish finsihed")

    async def askLLM(self,query,prompt):
        result = await self.llm.ask_query(query,prompt) 
        print(result)

    # 主要的处理逻辑, assign tasks to different workers


async def apply(request):

    print(request)
    controller = Controller()
    pipeline = list()
    request['from'] = "user"
    pipeline.append(request)
    nextStep = controller.get_next(pipeline)

    while nextStep != "genAnswer":
        print(nextStep)
        if inspect.iscoroutinefunction(getattr(controller,nextStep)):
            await getattr(controller,nextStep)(pipeline)
        else:
            getattr(controller,nextStep)(pipeline)
        nextStep = controller.get_next(pipeline)
    controller.genAnswer(pipeline)
    return pipeline[-1]

async def main():
    request = {'msg': 'hello', 'context': [], 'type': 'user', 'format': 'text', 'status': 'new'}
    context = [request]
    resp = await apply(request)
    context.append(resp)
    request1 = {'msg': 'hello again', 'context': context, 'type': 'user', 'format': 'text', 'status': 'hold'}
    resp = await apply(request1)

if __name__ == "__main__":
      
    asyncio.run(main())
