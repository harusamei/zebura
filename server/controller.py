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
from zebura_core.activity.exe_activity import ExeActivity
from zebura_core.LLM.llm_agent import LLMAgent
from msg_maker import (make_a_log,make_a_req)
import json
import random
import re
# 一个传递request的pipeline
# 从 Chatbot request 开始，到 type变为assistant 结束

D_RANDINT = random.randint(0,2)
class Controller:
    llm = LLMAgent("CHATANYWHERE","gpt-3.5-turbo")
    parser = Parser()
    st_matrix = {
            "(new,user)"        : "nl2sql",
            "(hold,user)"       : "nl2sql",
            "(succ,nl2sql)"     : "sql4db",
            "(failed,nl2sql)"   : "transit", # reset action
            "(failed,transit)"  : "end",    # send to user
            "(succ,sql4db)"     : "polish",
            "(failed,sql4db)"   : "end",
            "(*,polish)"        : "end",
            "(succ,rewrite)"    : "nl2sql",        
            "(failed,rewrite)"  : "end",    # send to user
            "(*,*)"             : "end"
    }
    def __init__(self):
        
        self.matrix = Controller.st_matrix
        self.llm = Controller.llm

        self.parser = Controller.parser
        self.sch_loader = Controller.parser.norm.sch_loader
        self.prompter = Controller.parser.prompter      # prompt generator

        self.asw_refiner = Synthesizer()
        self.executor = ExeActivity('mysql',self.sch_loader)
        # 一些套话
        self.utterance = {}
        with open("server\\utterances.json","r") as f:
            self.utterance = json.load(f)
        
        logging.info(f"Controller init success")

    def get_next(self,pipeline):

        lastLog = pipeline[-1]
        if lastLog['type'] == "reset" and lastLog['status'] == "succ":
            return lastLog['from']
        
        curSt =f'({lastLog["status"]},{lastLog["from"]})'
        count = 1
        while curSt not in self.matrix and count < 3:
            if count == 1:
                curSt = f'(*,{lastLog["from"]})'
            if count == 2:
                curSt = f'({lastLog["status"]},*)'
            if count == 3:
                curSt = f'(*,*)'
            count += 1
        return self.matrix[curSt]
    
    async def nl2sql(self, pipeline):

        log = pipeline[-1]
        content = log['msg']
        result = await self.parser.apply(content)
        new_Log = make_a_log("nl2sql")
        new_Log['msg'] = result['msg']
        new_Log['status'] = result["status"]
        if result["status"] == "succ":
            new_Log['format'] = 'sql'
        new_Log['others'] = result
        pipeline.append(new_Log)

    async def rewrite(self,pipeline):

        history=[]
        log = pipeline[0]
        new_Log = make_a_log("rewrite")

        if log['status'] == "new":  # 多轮才需要重写
            new_Log['status'] = "failed"
            new_Log['note'] = "ERR: NOCONTEXT, new request can't rewrite"
            pipeline.append(new_Log)
            return
    
        context = log['context']
        for one_req in context[-3:]:
            msg = f"{one_req['type']}: {one_req['msg']}"
            history.append(msg)
        
        history_context= "\n".join(history)
        query = log['msg']
        template = self.prompter.gen_default_prompt("rewrite")
        prompt = template.format(history_context=history_context,query=query)
        result = await self.askLLM(query, prompt)
        if "ERR" in result:
            new_Log['status'] = "failed"
            new_Log['note'] = result
        else:
            new_Log['msg'] = result
        pipeline.append(new_Log)


    def transit(self,pipeline):
        
        new_log=make_a_log("transit")
        new_log['status'] = "failed"
        new_log['type']="reset"

        rewritted = False
        for log in pipeline:
            if log['from'] == "rewrite":
                rewritted = True
                break
        log = pipeline[0]
        # 多轮且没有重写过
        if rewritted is False and log['status'] == "hold":
            new_log['from'] = "rewrite"  
            new_log['status'] = "succ"  
                    
        pipeline.append(new_log)
            
    def genAnswer(self,pipeline):
        
        answer = ""
        notes = []
        hints =[]
        for log in pipeline[1:]:
            if log['from'] == "polish":
                answer = log['msg']
                continue
            notes.append(f"{log['from']}: {log['status']}, {log['note']}")
            if len(log['hint'])>0:
                hints.append(f"{log['from']}: {log.get('hint')}")
            
        resp = make_a_req(answer)
        resp['note'] = "\n".join(notes)
        resp['type'] = "assistant"
        if len(hints)>0:
            resp['hint'] = "\n".join(hints)
            resp['status'] = "failed"
        else:
            resp['status'] = 'succ'
        return resp
             
    # 查库
    def sql4db(self,pipeline):
        log = pipeline[-1]
        query = log['msg']
        new_Log = self.executor.exeQuery(query)
        new_Log['from'] = "sql4db"
        pipeline.append(new_Log)

    #上一步执行不成功，给出提示
    def interpret(self,pipeline):

        for log in pipeline[1:]:
            if log['type'] == 'reset':
                log['from'] = 'transit' # 占用恢复
            
            match = re.search(r"ERR: (\w+)",log['note'])
            if match is not None:
                errtype = match.group(1)
                hint = self.utterance.get("en_error_"+errtype.lower(),'')
                if hint !='':
                    log["hint"] = hint['msg'][D_RANDINT]
        print([log['from'] for log in pipeline])

    # 美化sql结果，生成答案
    def polish(self, pipeline):
        log = pipeline[-1]
        new_Log = make_a_log("polish")
       
        markdown = tabulate(log['msg'], headers="keys", tablefmt="pipe")
        new_Log['msg'] = markdown
        new_Log['format'] = 'md'
        if len(markdown) == 0:
           new_Log['note'] ="ERR: NORESULT"
           new_Log['status'] = "failed"
        pipeline.append(new_Log)

    async def askLLM(self,query,prompt):
        result = await self.llm.ask_query(query,prompt) 
        print(result)
        return result
    

# 主要的处理逻辑, assign tasks to different workers
async def apply(request):

    print(request)
    D_RANDINT = random.randint(0,2)
    controller = Controller()
    pipeline = list()
    request['from'] = "user"
    pipeline.append(request)
    nextStep = controller.get_next(pipeline)

    while nextStep != "end":
        print(nextStep)
        if inspect.iscoroutinefunction(getattr(controller,nextStep)):
            await getattr(controller,nextStep)(pipeline)
        else:
            getattr(controller,nextStep)(pipeline)
        nextStep = controller.get_next(pipeline)
    controller.interpret(pipeline)
    return controller.genAnswer(pipeline)
    

async def main():
    request = {'msg': '查询颜色是黑色的小新电脑', 'context': [], 'type': 'user', 'format': 'text', 'status': 'new'}
    context = [request]
    resp = await apply(request)
    print(resp['msg']+f"\n\n{resp['note']}")
    context.append(resp)
    request1 = {'msg': '查小新电脑 ', 'context': context, 'type': 'user', 'format': 'text', 'status': 'hold'}
    resp = await apply(request1)
    print(resp['msg']+f"\n\n{resp['note']}")

if __name__ == "__main__":
      
    asyncio.run(main())
