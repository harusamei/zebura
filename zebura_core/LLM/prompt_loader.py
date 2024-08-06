# 典型prompt, 分为三层， roles最基本对应自我认知， tasks 对应指令， details 对应细节， shots对应实例
# 读取 prompt.txt中的指令模板，构建prompt
############################################
import os
import sys

sys.path.insert(0, os.getcwd())
import re
from settings import z_config
import logging
from zebura_core.knowledges.schema_loader import Loader


# prompt 模板通过文件导入，默认文件为当前目录下prompt.txt
class prompt_generator:
    _is_initialized = False

    def __init__(self, prompt_file=None):

        if not prompt_generator._is_initialized:
            prompt_generator._is_initialized = True
            self.tasks = {}
            self.db_structs = ""

            cwd = os.getcwd()
            name = z_config['Training', 'db_schema']  # 'training\ikura\ikura_meta.json'
            self.sch_loader = Loader(os.path.join(cwd, name))
            name = z_config['Training', 'db_struct']  # 'training\ikura\ikura_db_structures.txt'
            self.load_dbstruct(os.path.join(cwd, name))  # 读取数据库结构

            if prompt_file is None:
                # base = os.getcwd()
                prompt_file="E:/zebura/zebura_core/LLM/prompt.txt"
                # prompt_file = os.path.join(base, "zebura_core/LLM/prompt.txt")  # 自带模板文件

            if self.load_prompt(prompt_file):
                logging.debug("prompt_generator init success")
            else:
                logging.debug("no prompt file, only generate default prompt")

            prompt_generator.tasks = self.tasks
            prompt_generator.db_structs = self.db_structs

    def load_dbstruct(self, db_struct_file):
        if not os.path.exists(db_struct_file):
            return False
        with open(db_struct_file, "r", encoding='utf-8') as f:
            self.db_structs = f.read()
        return True

    def load_prompt(self, prompt_file):
        if not os.path.exists(prompt_file):
            return False
        tList = []
        content = ""
        with open(prompt_file, "r", encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith("#"):
                    continue
                if line.startswith("<TASK:"):
                    task_name = line.split(":")[1].strip()
                    task_name = re.sub(r'[^\w]', '', task_name)
                    task_name = task_name.lower()
                    self.tasks[task_name.lower()] = ""
                    content = ""
                elif line.startswith("</TASK>"):
                    self.tasks[task_name] = content
                    tList.append(task_name)
                else:
                    content += line
        return True

    # 获得/合成prompt
    # "rewrite","nl2sql","sql_revise","term_expansion","db2nl","db2sql"
    def gen_prompt(self, taskname, gcases=None):
        if 'nl2sql' in taskname.lower():
            return self.gen_nl2sql(taskname.lower(), gcases)
        return self.tasks.get(taskname, f"please do {taskname}")

    def gen_nl2sql(self, taskname, gcases: list = []) -> dict:

        tmpl = self.tasks[taskname]
        dbSchema = self.get_dbSchema()
        # 生成user/assistant对
        fewshots = []
        for case in gcases:
            fewshots.append({'user': case['query'], 'assistant': case['sql']})

        return tmpl.format(dbSchema=dbSchema), fewshots

    def get_dbSchema(self, table_name=None) -> str:
        # TODO， 按table_name拆分
        return self.db_structs


# Example usage
if __name__ == '__main__':
    from zebura_core.LLM.llm_agent import LLMAgent
    import asyncio

    llm = LLMAgent()
    pg = prompt_generator()
    # print(pg.get_dbSchema())
    # print(pg.tasks["nl2sql"])
    # prompt = pg.tasks['term_expansion']
    # keywords = "product, price, 笔记本, 联想小新, lenovo, computer"
    # result = asyncio.run(llm.ask_query(keywords,prompt))
    # print(result)

    prompt = pg.tasks['sql_revise']
    db_struct = pg.get_dbSchema()
    orisql = "SELECT product_name\nFROM produt\nWHERE category = '电脑';"
    errmsg = ("table name errors:\n"
              "no 'produt' table found in the database schema.\n"
              "conditions errors:\n"
              "value '电脑' was not found in the field category. ")
    query = prompt.format(dbSchema=db_struct, ori_sql=orisql, err_msgs=errmsg)
    result = asyncio.run(llm.ask_query(query, ''))
    print(query)
    print(result)