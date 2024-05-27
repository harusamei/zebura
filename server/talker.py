import os
import subprocess
import chainlit as cl
import asyncio
import sys
#sys.path.append("E:/zebura")
sys.path.insert(0, os.getcwd().lower())
import settings
from zebura_core.query_parser.parser import Parser
import pymysql
table_name = 'product'
parser = Parser()
conn = pymysql.connect(
    host='localhost',		# 主机名（或IP地址）
    port=3306,				# 端口号，默认为3306
    user='root',			# 用户名
    password='zebura',	# 密码
    charset='utf8mb4'  		# 设置字符编码
)
print(conn.get_server_info())
cursor = conn.cursor()
conn.select_db("products")

secret = subprocess.check_output(["chainlit", "create-secret"], text=True)
os.environ["CHAINLIT_AUTH_SECRET"] = secret

@cl.password_auth_callback
def auth_callback(username: str, password: str):
    # Fetch the user matching username from your database
    # and compare the hashed password with the value stored in the database
    if (username, password) == ("admin", "eCrMT3h=peMlxZmGMwjr"):
        return cl.User(
            identifier="admin", metadata={"role": "admin", "provider": "credentials"}
        )
    else:
        return None

@cl.on_chat_start
def redefine_secret():
    secret = subprocess.check_output(["chainlit", "create-secret"], text=True)
    os.environ["CHAINLIT_AUTH_SECRET"] = secret

@cl.step
def tool(message):
    categoryList=[]
    # TODO:在这里返回消息之后将数据塞入content就可以返回
    try:
        print(table_name,message.content)
        result = asyncio.run(parser.apply(table_name, message.content))
        print(result)
        if result["status"] is True:
            # categoryList.append(result["msg"])
            cursor.execute(result["sql1"][0].lower())
            answer = cursor.fetchall()
            keys = [column[0] for column in cursor.description]
            result_dicts = []
            for row in answer:
                result_dict = dict(zip(keys, row))
                result_dicts.append(result_dict)
            for result_dict in result_dicts:
                categoryList.append(result_dict)
            return categoryList
        else:
            return "暂未生成sql语句，请尝试其他方式"
    except Exception as e:
        return "遇到如下错误："+str(e)+"\n请您稍后重试"


@cl.on_message
async def main(message: cl.Message):
    result = tool(message)
    if type(result) is list:
        answer="您所查找的数据如下:"+"\n"+str(result)
    else:
        answer="发生报错,具体请看以下报错原因:"+"\n"+result
    await cl.Message(content=answer).send()

if __name__ == "__main__":
    from chainlit.cli import run_chainlit
    run_chainlit(__file__)