# 典型prompt, 分为三层， roles最基本对应自我认知， tasks 对应指令， details 对应细节， shots对应实例
# 这里只存 roles 和 tasks

roles={}
tasks={}            # task description
lang_mappings={}    # prompt 对应的多语言翻译

# "You are a SQL programmer, you can generate SQL queries based on natural language input."

roles["sql_assistant"]=(
                        "You are SQL programmer, you can generate SQL queries based on the "
                        "given natural language input and the table information."
                        )
roles["doc_assistant"]="You are a document assistant responsible for helping users create, edit, and format various types of documents."
roles["code_reviewer"]="You are a code reviewer, responsible for fixing bugs in the SQL queries."

tasks["rewrite"]= "Please rewrite the following sentence to clearly express the query intent and remove irrelevant information. If you cannot rewrite it, please output the original sentence."
tasks["nl2sql"]="Complete SQL query only and with no explaination"
tasks["summary"] ="Please generate a summary of the following document content, no more than 100 words"
tasks["create_sql"] ="Create SQL queries for the given tables and columns."
tasks["sql2nl"] ="Create a question based on the given SQL statement."
tasks["create_query"] ="Create a query based on the given table and column names."
tasks["complex_nl2sql"] ="Use the intermediate representation and the database schema to generate the SQL queries for each of the questions"
tasks["correct_sql"] ="For the given question, use the provided tables, columns to fix the given SQL query for any issues. If there are any problems, fix them. If there are no issues, return SQL query as is."
tasks["term_definition"] ="Please provide definition for given term related to database table column names. "
tasks["term_alias"] ="Please provide the alias of the given term."

lang_mappings["zh_doc_assistant"]="你是一名文档助手，负责帮助用户创建、编辑和格式化各种类型的文档。"
lang_mappings["zh_sql_assistant"]="你是一名SQL程序员，能够将自然语言查询转换为 SQL语句 。"
lang_mappings["zh_summary"] ="请给下面的文档内容生成总结，不要超过100字。"