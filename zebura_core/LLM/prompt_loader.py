# 读取 prompt.txt中的指令模板，构建prompt
import os
import sys
sys.path.insert(0, os.getcwd())
import re
from settings import z_config
import logging
from knowledges.schema_loader import Loader
# 典型prompt, 分为三层， roles最基本对应自我认知， tasks 对应指令， details 对应细节， shots对应实例
# 这里只存 roles 和 tasks
class prompt_generator:

    def __init__(self,prompt_file=None):

        self.roles = {}
        self.tasks = {}
        self.set_defaults()

        cwd = os.getcwd()
        name = z_config['Training','db_schema']  # 'training\ikura\ikura_meta.json'
        self.sch_loader = Loader(os.path.join(cwd, name))

        if prompt_file is None:
            base= os.getcwd()
            prompt_file = os.path.join(base,"zebura_core/LLM/prompt.txt")
        
        if self.load_prompt(prompt_file):
            logging.debug("prompt_generator init success")
        else:
            logging.debug("no prompt file, generate prompt by default templates")
    
    def load_prompt(self,prompt_file):
        if not os.path.exists(prompt_file):
            return False
        tList =[]
        content = ""
        with open(prompt_file, "r",encoding='utf-8') as f:
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
        print("loaded tasks: ",tList)     
        return True
    
    def gen_default_prompt(self,task_name:str) -> str:
        if task_name.lower() not in self.tasks.keys():
            return ""
        
        return self.roles['doc_assistant']+'\n'+self.tasks[task_name]
    
    def gen_sql_prompt(self,gcases,table_name=None,style='full') -> str:

        task_name = "nl2sql"
        template = self.tasks[task_name]
        pos_fewShots = self.gen_fewShots(task_name,gcases)
        neg_fewShots = self.gen_negShots()
        fewShots = pos_fewShots + neg_fewShots
        dbSchema = self.gen_dbSchema(table_name,style=style)
        
        prompt= template.format(fewShots=fewShots,dbSchema=dbSchema)
        role = self.roles["sql_assistant"]

        return role +"\n"+ prompt
    
    # gcases: ES的返回结果 golden cases
    # fields = ["no", "query", "qemb", "sql", "gt", "activity","explain","category", "updated_date"] 
    # Input: "Find all users who registered in 2021."
    # Output:
    # sql
    # SELECT * FROM users WHERE registration_year = 2021;
    def gen_fewShots(self,task_name:str, gcases:list) -> str:

        if task_name.lower() !="nl2sql":
            return ""
        tlist=[]
        for case in gcases:
            s = f"Input:{case['doc']['query']}\nOutput:\nsql\n{case['doc']['sql']}\n"
            tlist.append(s)
        return "\n".join(tlist)
    
    # Input: "Show me the sales data for March."
    # Output: NOSQL (if table and column names are not provided)
    def gen_negShots(self) -> str:
        ncase = (   "Input: show me the big data for March.\n"
                    "Output: NOSQL (if table and column names are not provided)\n"
                )
        return ncase
    
    # 生成table details of prompts for nl2sql， 样式见下
    # Table names and purposes:
    # Table name: users, Purpose: Stores user information

    # Table fields and their aliases:
    # Table: users 
    # id: Unique identifier for the user (Alias: user_id)
    # name: Name of the user (Alias: user_name)
    # email: Email address of the user (Alias: user_email)
    # registration_date: Date when the user registered (Alias: user_registration_date)
    # registration_year: Year when the user registered (Alias: user_registration_year)
    # style= full, zh, lite
    def gen_dbSchema(self, table_name=None, style='full') -> str:

        tList =['Table names and purposes:\n']
        if table_name is None:
            tableList = self.sch_loader.get_table_nameList()
        else:
            tableList = [table_name]

        for table_name in tableList:
            tDict = self.sch_loader.get_table_info(table_name)
            tList.append(f"Table name: {table_name}, Purpose: {tDict.get('desc','')}\n")
            tList[-1]=tList[-1].replace("(Purpose: ,","Purpose: ")

        tList.append('\nTable fields and their aliases:\n')
        for table_name in tableList:
            tDict = self.sch_loader.get_table_info(table_name)
            columns = self.sch_loader.get_all_columns(table_name)
            tList.append(f"Table: {table_name}\n")
            for column in columns:
                if style == 'full':
                    tList.append(f"{column['column_name']}: {column.get('desc','')} (Alias: {column.get('name','')}, {column.get('alias','')})\n")
                elif style == 'zh':
                    tList.append(f"{column.get('column_name','')}: {column.get('desc','')} (Alias: {column.get('name_zh','')}, {column.get('alias_zh','')})\n")
                else:
                    tList.append(f"{column.get('column_name','')}: (Alias: {column.get('name','')}, {column.get('alias','')})\n")
                tList[-1]=tList[-1].replace("(Alias: ,","(Alias: ")

        tStr="\n".join(tList)
        return re.sub('\n+', '\n', tStr)
    
    def set_defaults(self):
        self.roles["sql_assistant"]=(
                        "You are a SQL conversion assistant. "
                        "Your task is to convert natural language statements into SQL queries. "
                        )
        self.roles["doc_assistant"]="You are a document assistant responsible for helping users create, edit, and format various types of documents."
        self.roles["code_reviewer"]="You are a code reviewer, responsible for fixing bugs in the SQL queries."

        # default instructions
        self.tasks["rewrite"]= "Please rewrite the following sentence to clearly express the query intent and remove irrelevant information. If you cannot rewrite it, please output the original sentence."
        self.tasks["nl2sql"]="Complete SQL query only and with no explaination"
        self.tasks["summary"] ="Please generate a summary of the following document content, no more than 100 words"
        self.tasks["create_sql"] ="Create SQL queries for the given tables and columns."
        self.tasks["sql2nl"] ="Create a question based on the given SQL statement."
        self.tasks["create_query"] ="Create a query based on the given table and column names."
        self.tasks["complex_nl2sql"] ="Use the intermediate representation and the database schema to generate the SQL queries for each of the questions"
        self.tasks["correct_sql"] ="For the given question, use the provided tables, columns to fix the given SQL query for any issues. If there are any problems, fix them. If there are no issues, return SQL query as is."
        self.tasks["term_definition"] ="Please provide definition for given term related to database table column names. "
        self.tasks["term_alias"] ="Please provide the alias of the given term."
    
    
# Example usage
if __name__ == '__main__':
    pg = prompt_generator()
    print(pg.gen_dbSchema())
    print(pg.gen_negShots())