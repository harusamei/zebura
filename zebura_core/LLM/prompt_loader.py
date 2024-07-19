# 读取 prompt.txt中的指令模板，构建prompt
import os
import sys

sys.path.insert(0, os.getcwd())
import re
from settings import z_config
import logging
from zebura_core.knowledges.schema_loader import Loader


# 典型prompt, 分为三层， roles最基本对应自我认知， tasks 对应指令， details 对应细节， shots对应实例
# prompt 模板通过文件导入，默认文件为当前目录下prompt.txt
class prompt_generator:

    def __init__(self, prompt_file=None):

        self.roles = {}
        self.tasks = {}
        self.db_structs = ""
        self.set_defaults()

        cwd = os.getcwd()
        name = z_config['Training', 'db_schema']  # 'training\ikura\ikura_meta.json'
        self.sch_loader = Loader(os.path.join(cwd, name))
        name = z_config['Training', 'db_struct']  # 'training\ikura\ikura_db_structures.txt'
        self.load_dbstruct(os.path.join(cwd, name))  # 读取数据库结构

        if prompt_file is None:
            base = os.getcwd()
            prompt_file = os.path.join(base, "zebura_core/LLM/prompt.txt")  # 自带模板文件

        if self.load_prompt(prompt_file):
            logging.debug("prompt_generator init success")
        else:
            logging.debug("no prompt file, generate prompt by default templates")

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

    def gen_default_prompt(self, task_name: str) -> str:
        if task_name.lower() not in self.tasks.keys():
            return ""
        if task_name.lower() == "nl2sql":
            return self.roles['sql_assistant'] + '\n' + self.tasks[task_name]
        else:
            return self.roles['doc_assistant'] + '\n' + self.tasks[task_name]

    def gen_rewrite_prompt(self) -> str:
        return self.tasks["rewrite"]

    def gen_nl2sql_prompt(self, gcases: list = []) -> dict:
        tmpl = self.tasks["nl2sql"]
        dbSchema = self.get_dbSchema()
        # 生成user/assistant对
        fewshots = []
        for case in gcases:
            fewshots.append({'user': case['query'], 'assistant': case['sql']})

        return tmpl.format(dbSchema=dbSchema), fewshots

    def get_dbSchema(self, table_name=None) -> str:
        # TODO， 按table_name拆分
        return self.db_structs

    def set_defaults(self):
        self.roles["sql_assistant"] = (
            "You are a SQL conversion assistant. "
            "Your task is to convert natural language statements into SQL queries. "
        )
        self.roles[
            "doc_assistant"] = "You are a document assistant responsible for helping users create, edit, and format various types of documents."
        self.roles["code_reviewer"] = "You are a code reviewer, responsible for fixing bugs in the SQL queries."

        # default instructions
        self.tasks[
            "rewrite"] = "Please rewrite the following sentence to clearly express the query intent and remove irrelevant information. If you cannot rewrite it, please output the original sentence."
        self.tasks["nl2sql"] = "Complete SQL query only and with no explaination"
        self.tasks["summary"] = "Please generate a summary of the following document content, no more than 100 words"
        self.tasks["create_sql"] = "Create SQL queries for the given tables and columns."
        self.tasks["sql2nl"] = "Create a question based on the given SQL statement."
        self.tasks["create_query"] = "Create a query based on the given table and column names."
        self.tasks[
            "complex_nl2sql"] = "Use the intermediate representation and the database schema to generate the SQL queries for each of the questions"
        self.tasks[
            "correct_sql"] = "For the given question, use the provided tables, columns to fix the given SQL query for any issues. If there are any problems, fix them. If there are no issues, return SQL query as is."
        self.tasks[
            "term_definition"] = "Please provide definition for given term related to database table column names. "
        self.tasks["term_alias"] = "Please provide the alias of the given term."


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