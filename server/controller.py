# 与chatbot交互的接口, 内部是一个总控制器，负责调度各个模块最终完成DB查询，返回结果
############################################
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
from zebura_core.activity.gen_activity import GenActivity

from zebura_core.LLM.llm_agent import LLMAgent
from msg_maker import (make_a_log,make_a_req)
import json
import re
# 一个传递request的pipeline
# 从 Chatbot request 开始，到 type变为assistant 结束

class Controller:
    llm = LLMAgent("CHATANYWHERE","gpt-3.5-turbo")
    parser = Parser()
    # 一些应急话术
    utterance = {}
    with open("server\\utterances.json","r") as f:
        utterance = json.load(f)

    st_matrix = {
            "(new,user)"        : "nl2sql",
            "(hold,user)"       : "nl2sql",
            "(succ,nl2sql)"     : "sql_refine",
            "(succ,sql_refine)"   : "sql4db",
            "(failed,sql_refine)" : "end",      # send to user
            "(failed,nl2sql)"   : "transit",    # reset action
            "(failed,transit)"  : "end",        # send to user
            "(succ,sql4db)"     : "polish",
            "(failed,sql4db)"   : "end",
            "(*,polish)"        : "end",
            "(succ,rewrite)"    : "nl2sql",        
            "(failed,rewrite)"  : "end",        # send to user
            "(*,*)"             : "end"
    }
    def __init__(self):
        
        self.matrix = Controller.st_matrix
        self.llm = Controller.llm

        self.parser = Controller.parser
        self.sch_loader = Controller.parser.norm.sch_loader
        self.prompter = Controller.parser.prompter      # prompt generator
        self.utterance = Controller.utterance

        self.act_maker = GenActivity()
        self.asw_refiner = Synthesizer()
        self.executor = ExeActivity(self.sch_loader)
        logging.info(f"Controller init success")

    def get_next(self,pipeline):

        lastLog = pipeline[-1]
        print(f"------\n{lastLog['from']}\n{lastLog['msg']}")

        # 强制跳转
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
        new_Log = make_a_log("nl2sql")

        query = log['msg']
        result = await self.parser.apply(query)
        for k in ['msg','status','note','others','hint']:
            new_Log[k] = result[k]
        
        if result["status"] == "succ":
            new_Log['format'] = 'sql'
        pipeline.append(new_Log)

    async def sql_refine(self,pipeline):
        log = pipeline[-1]
        new_Log = make_a_log("sql_refine")

        query = pipeline[0]['msg']
        result = await self.act_maker.gen_activity(query, log['msg'])
        for k in ['msg','status','note','others','hint']:
            new_Log[k] = result[k]
        
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
            msg = f"{one_req['type']}: {one_req.get('msg')}"
            index = msg.find("Root Cause")
            if index != -1:
                msg = msg[:index]
            history.append(msg)
        
        history_context= "\n".join(history)
        query = log['msg']
        tmpl = self.prompter.gen_rewrite_prompt()
        prompt = tmpl.format(history_context=history_context,query=query)
        result = await self.llm.ask_query(prompt,"")
        print('rewrite:',result)
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
            
    async def genAnswer(self,pipeline):
        
        tmpl_succ = "{polish_msg}\nNote:{db2sql_note}\n\n---Detailed Reasoning Steps---\n{stepInfo}"
        tmpl_failed ="no results matching your query.\n{hint}\nRoot Cause:{e_tag}\n\n---Detailed Reasoning Steps---\n{stepInfo} "
        
        resp = pipeline.pop()
        resp['type'] = "assistant"
        status = resp['status']   
        if status =='succ':
            tmpl = tmpl_succ
            steps = ['nl2sql', 'rewrite']
        else:
            tmpl =tmpl_failed
            steps =[ 'nl2sql', 'rewrite', 'sql4db']
            e_tag = resp['note']

        polish_msg = db2sql_note = stepInfo =""
        hint = ""
        for log in pipeline[1:]:           
            if log['from'] in steps:
                stepInfo += f"{log['from']}:{log['status']}\n" # , {log['msg']}
                hint = log.get('hint')+'\n'     # 最后一个hint
            
            if log['from'] == "polish":
                polish_msg = log['msg']
            if log['from'] == "sql4db":
                db2sql_note = log['note']

        if status == "succ":
            resp['msg'] = tmpl.format(polish_msg=polish_msg,db2sql_note=db2sql_note,stepInfo=stepInfo)
            resp['note'] = db2sql_note
        else:
            resp['msg'] = tmpl.format(hint =hint, e_tag=e_tag,stepInfo=stepInfo)
            
        resp['msg'] = re.sub(r'\n+', '\n', resp['msg'])
        resp['hint'] = re.sub(r'\n+', '\n', hint)
        return resp
             
    # 查库
    def sql4db(self,pipeline):
        log = pipeline[-1]
        new_Log = make_a_log("sql4db")
        sql = log['msg']
        print(f"sql4db: {sql}")

        result = self.executor.exeSQL(sql)
        for k in ['msg','status','note','others','hint']:
            new_Log[k] = result[k]
        
        pipeline.append(new_Log)

    #对整个pipeline信息进行整理，分为msg主信息，note， hint
    def interpret(self,pipeline):
        resp = make_a_req("waiting reply")
        final_status = "failed"
        for log in pipeline[1:]:
            if log['type'] == 'reset':
                log['from'] = 'transit'         # 恢复之前状态机占用
            if log['from'] == "polish":
                final_status = log['status']    # 只有走到polish才算成功
                break
        resp['status'] = final_status

        root_cause = ""
        for log in pipeline[1:]:    
            match = re.search(r"ERR_(\w+)",log['note'])
            if match is not None:
                errtype = match.group(1)
                root_cause = f"step {log['from']} met error of {errtype}"
                utts = self.utterance.get("error_"+errtype.lower(),'')
                if utts !='' and log['hint'] == "":  # 如果没有hint，就用默认的
                    log["hint"] = utts['msg']
        # 最后一次失败原因是整个pipeline最终原因
        resp['note'] = root_cause
        pipeline.append(resp)

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

# 主函数, assign tasks to different workers
async def apply(request):

    controller = Controller()
    pipeline = list()
    request['from'] = "user"
    pipeline.append(request)
    nextStep = controller.get_next(pipeline)

    while nextStep != "end":
        if inspect.iscoroutinefunction(getattr(controller,nextStep)):
            await getattr(controller,nextStep)(pipeline)
        else:
            getattr(controller,nextStep)(pipeline)
        nextStep = controller.get_next(pipeline)
    controller.interpret(pipeline)
    return await controller.genAnswer(pipeline)
    

async def main():
    
    request = {'msg': '电子产品分类下，评分最高的几款产品有哪些？', 'context': [], 'type': 'user', 'format': 'text', 'status': 'new'}
    #request ={'msg':'Find the types of fans available in the database.', 'context': [], 'type': 'user', 'format': 'text', 'status': 'new'}
    context = [request]
    resp = await apply(request)
    print(resp)
    context.append(resp)
    request1 = {'msg': '查类型 ', 'context': context, 'type': 'user', 'format': 'text', 'status': 'hold'}
    # resp = await apply(request1)
    # print(resp)

if __name__ == "__main__":
      
    asyncio.run(main())
