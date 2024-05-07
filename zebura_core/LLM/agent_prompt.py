# 典型prompt, 分为三层， roles最基本对应自我认知， tasks 对应指令， details 对应细节， shots对应实例
# 这里只存 roles 和 tasks

roles={}
tasks={}            # task description
lang_mappings={}    # prompt 对应的多语言翻译

# "You are a SQL programmer, you can generate SQL queries based on natural language input."

roles["sql_assistant"]="You are SQL programmer, you can understand the natural language input and converse it into SQL statement."
roles["doc_assistant"]="You are a document assistant responsible for helping users create, edit, and format various types of documents."

tasks["rewrite"]= "Please rewrite the following sentence to clearly express the query intent and remove irrelevant information. If you cannot rewrite it, please output the original sentence."
tasks["nl2sql"]="complete SQL query only and with no explaination"
tasks["summary"] ="Please generate a summary of the following document content, no more than 100 words"

lang_mappings["zh_doc_assistant"]="你是一名文档助手，负责帮助用户创建、编辑和格式化各种类型的文档。"
lang_mappings["zh_sql_assistant"]="你是一名SQL程序员，能够将自然语言查询转换为 SQL语句 。"
lang_mappings["zh_summary"] ="请给下面的文档内容生成总结，不要超过100字。"