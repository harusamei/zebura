# 与chatbot交互的接口, 内部是一个总控制器，负责调度各个模块最终完成DB查询，返回结果
import sys
import os
sys.path.insert(0, os.getcwd().lower())
import settings
from tabulate import tabulate

import logging
import asyncio
import inspect
from zebura_core.query_parser.parser import Parser
from zebura_core.answer_refiner.synthesizer import Synthesizer
# from zebura_core.answer_refiner.explainer import Explainer  
from zebura_core.activity.exe_activity import ExeActivity
from zebura_core.LLM.llm_agent import LLMAgent

# 一个传递request的pipeline
# 从 Chatbot request 开始，到 type变为assistant 结束
# request={
#         "msg": content,
#         "context": context,
#         "type": "user/assistant/transaction", # 用户， controller, 增加 action间切换
#         "format": "text/md/sql/dict...", # content格式，与显示相关
#         "status": "new/hold/failed/succ", # 新对话,多轮继续；执行失败；执行成
#         
# 增加      "from": "nl2sql/sql4db/interpret/polish" # 当前任务
# 增加      "others": 当前步骤产生的次要信息 # 下一个任务
#     } 

class Controller:
    llm = LLMAgent("AZURE","gpt-3.5-turbo")
    parser = Parser()
    st_matrix = {
            "(new,user)"    : "nl2sql",
            "(hold,user)"   : "rewrite",
            "(succ,rewrite)": "nl2sql",
            "(succ,nl2sql)" : "sql4db",
            "(succ,sql4db)" : "polish",
            "(succ,polish)" : "genAnswer",
            "(failed,end)"  : "genAnswer",
            "(succ,end)"    : "genAnswer",
            "(failed,*)"    : "transit",        # 失败则重新设置状态
            "(*,*)"         : "genAnswer"       # send to user
    }
    
    def __init__(self):
        
        self.matrix = Controller.st_matrix
        self.llm = Controller.llm

        self.parser = Controller.parser
        self.sch_loader = Controller.parser.norm.sch_loader
        self.prompter = Controller.parser.prompter      # prompt generator

        self.asw_refiner = Synthesizer()
        self.executor = ExeActivity('mysql',self.sch_loader)

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
        print(result['msg'])
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
        response["format"]= log.get('format','text')

        if log['status'] == "failed":
            response['msg'] += "\n please tell me more details for your database."   
        else:
            response['msg'] += response.get('note','')
             
        pipeline.append(response) 

    # 查库
    def sql4db(self,pipeline):
        log = pipeline[-1]
        query = log['msg']
        resp = self.executor.exeQuery(query)
        resp['from'] ="sql4db"
        resp['type'] ='transaction'
        pipeline.append(resp)

    #上一步执行不成功，给出解释
    def interpret(self,queue):
        last_step = queue.get()
        queue.put("interpret finsihed")

    # 美化sql结果，生成答案
    def polish(self, pipeline):
        log = pipeline[-1]
        markdown = tabulate(log['msg'], headers="keys", tablefmt="pipe")
        new_Log = dict(log)
        new_Log['msg'] = markdown
        new_Log['from'] = 'polish'
        new_Log['format'] = 'md'
        new_Log['status'] = 'succ'
        if markdown == "":
           new_Log['note'] = log.get('note','')+ '\nsql is correct, but no result found'
        pipeline.append(new_Log)

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
    request = {'msg': '找出所有内存容量大于16 GB的服务器的SQL查询语句应该怎么写？', 'context': [], 'type': 'user', 'format': 'text', 'status': 'new'}
    context = [request]
    resp = await apply(request)
    # context.append(resp)
    # # msg ='Remote end closed connection without response'
    # request1 = {'msg': '查一下产品名 ', 'context': context, 'type': 'user', 'format': 'text', 'status': 'hold'}
    # resp = await apply(request1)

if __name__ == "__main__":
      
    asyncio.run(main())
