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
from msg_maker import (make_a_log, make_a_req)
import json
import time

# 一个传递request的pipeline
# 从 Chatbot request 开始，到 type变为assistant 结束

class Controller:
    llm = LLMAgent("CHATANYWHERE", "gpt-3.5-turbo")
    parser = Parser()
    # 一些应急话术
    utterance = {}
    with open("server\\utterances.json", "r") as f:
        utterance = json.load(f)


    def __init__(self):

        self.llm = Controller.llm

        self.parser = Controller.parser
        self.sch_loader = Controller.parser.norm.sch_loader
        self.prompter = Controller.parser.prompter  # prompt generator
        self.utterance = Controller.utterance

        self.act_maker = GenActivity()
        self.asw_refiner = Synthesizer()
        self.executor = ExeActivity(self.sch_loader)

        self.matrix = {
            "(new,user)": self.nl2sql,
            "(hold,user)": self.nl2sql,
            "(succ,nl2sql)": self.sql_refine,
            "(succ,sql_refine)": self.sql4db,
            "(failed,sql_refine)": self.end,  # send to user
            "(failed,nl2sql)": self.transit,  # reset action
            "(failed,transit)": self.end,  # send to user
            "(succ,sql4db)": self.polish,
            "(failed,sql4db)": self.end,
            "(*,polish)": self.end,
            "(succ,rewrite)": self.nl2sql,
            "(failed,rewrite)": self.end,  # send to user
            "(*,*)": self.end
        }
        logging.info(f"Controller init success")

    def get_next(self, pipeline):

        lastLog = pipeline[-1]

        # 强制跳转
        if lastLog['type'] == "reset" and lastLog['status'] == "succ":
            method = getattr(self, lastLog['from'])
            return method

        curSt = f'({lastLog["status"]},{lastLog["from"]})'
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
        for k in ['msg', 'status', 'note', 'others', 'hint']:
            new_Log[k] = result[k]

        if result["status"] == "succ":
            new_Log['format'] = 'sql'
        pipeline.append(new_Log)

    async def sql_refine(self, pipeline):
        log = pipeline[-1]
        new_Log = make_a_log("sql_refine")

        query = pipeline[0]['msg']
        result = await self.act_maker.gen_activity(query, log['msg'])
        for k in ['msg', 'status', 'note', 'others', 'hint']:
            new_Log[k] = result[k]

        pipeline.append(new_Log)

    async def rewrite(self, pipeline):

        history = []
        log = pipeline[0]
        new_Log = make_a_log("rewrite")

        if log['status'] == "new":  # 多轮才需要重写
            new_Log['status'] = "failed"
            new_Log['note'] = "ERR: NOCONTEXT, new request can't rewrite"
            pipeline.append(new_Log)
            return

        context = log['context']
        # 保留最近6轮的请求
        for one_req in context[-6:]:
            msg = f"{one_req['type']}: {one_req.get('msg')}"
            history.append(msg)

        history_context = "\n".join(history)
        query = log['msg']
        tmpl = self.prompter.gen_prompt('rewrite')
        # TODO, prompt 写得有问题
        prompt = tmpl.format(history_context=history_context, query=query)

        # outFile = 'output.txt'
        # with open(outFile, 'a', encoding='utf-8') as f:
        #     f.write(prompt)
        #     f.write("\n----------------------------end\n")

        result = await self.llm.ask_query(prompt, "")
        if "ERR" in result:
            new_Log['status'] = "failed"
            new_Log['note'] = result
        else:
            new_Log['msg'] = result
        pipeline.append(new_Log)

    def transit(self, pipeline):

        new_log = make_a_log("transit")
        new_log['status'] = "failed"
        new_log['type'] = "reset"

        fromList = [log['from'] for log in pipeline]
        # 多轮且没有重写过
        if 'rewrite' not in fromList and pipeline[0]['status'] == "hold":
            new_log['from'] = "rewrite"
            new_log['status'] = "succ"

        pipeline.append(new_log)

    async def genAnswer(self, pipeline):

        resp = pipeline.pop()
        resp['msg'] = f"{resp['msg']}\n\nNote:\n\n{resp['note']}"

        # outFile = 'output.txt'
        # with open(outFile, 'a', encoding='utf-8') as f:
        #     f.write(resp['msg'])
        #     f.write("\n----------------------------end\n")

        return resp

    # 查库
    def sql4db(self, pipeline):
        log = pipeline[-1]
        new_Log = make_a_log("sql4db")
        sql = log['msg']
        print(f"sql4db: {sql}")

        result = self.executor.exeSQL(sql)
        for k in ['msg', 'status', 'note', 'others', 'hint']:
            new_Log[k] = result[k]

        pipeline.append(new_Log)

    # 对整个pipeline信息进行整理，分为msg主信息，note， hint
    def interpret(self, pipeline):

        resp = make_a_req("interpret")
        resp['type'] = 'assistant'
        resp['msg'] = pipeline[-1]['msg']
        for log in pipeline[1:]:
            if log['type'] == 'reset':
                log['from'] = 'transit'  # 恢复之前状态机转移时的占用

        steps = [pipeline['from'] for pipeline in pipeline]
        more = ['nl2sql', 'rewrite', 'sql_refine']
        steps_info = ['Reasoning Steps:']
        for log in pipeline[1:]:
            if log['from'] in more:
                steps_info.append(f"{log['from']}:{log['status']}\n{log['msg']}\n")
            else:
                steps_info.append(f"{log['from']}:{log['status']}")
        # 只有走到polish才算成功
        if "polish" in steps:
            resp['status'] = "succ"
            resp['note'] = "SUCCESS\n"
        else:
            resp['status'] = "failed"
            resp['note'] = "ERR: NORESULT\n"

        resp['note'] += '\n'.join(steps_info)
        pipeline.append(resp)

    # 美化sql结果，生成答案
    def polish(self, pipeline):
        log = pipeline[-1]
        new_Log = make_a_log("polish")

        markdown = tabulate(log['msg'], headers="keys", tablefmt="pipe")
        new_Log['msg'] = markdown
        new_Log['format'] = 'md'
        if len(markdown) == 0:
            new_Log['note'] = "ERR: NORESULT"
            new_Log['status'] = "failed"
        pipeline.append(new_Log)

    def end(self):
        return "end"

# 主函数, assign tasks to different workers
async def apply(request):
    start = time.time()
    controller = Controller()
    print(f"Controller init time: {time.time() - start}")
    pipeline = list()
    request['from'] = "user"
    pipeline.append(request)
    nextStep = controller.get_next(pipeline)

    while nextStep != controller.end:
        if inspect.iscoroutinefunction(nextStep):
            await nextStep(pipeline)
        else:
            nextStep(pipeline)
        nextStep = controller.get_next(pipeline)
    controller.interpret(pipeline)
    return await controller.genAnswer(pipeline)


async def main():
    request = {'msg': '请查一下联想笔记本的价格', 'context': [], 'type': 'user', 'format': 'text',
               'status': 'new'}
    # request ={'msg':'Find the types of fans available in the database.', 'context': [], 'type': 'user', 'format': 'text', 'status': 'new'}
    context = [request]
    resp = await apply(request)
    print(resp['msg'])
    print(resp['note'])
    context.append(resp)
    request1 = {'msg': '列出所有属于家居与厨房类别的最贵商品。', 'context': context, 'type': 'user', 'format': 'text',
                'status': 'hold'}
    resp = await apply(request1)
    print(resp)


if __name__ == "__main__":
    asyncio.run(main())
