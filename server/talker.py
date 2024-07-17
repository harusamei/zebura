import os
import sys
sys.path.insert(0, os.getcwd().lower())
import asyncio
import subprocess
import chainlit as cl
import chainlit.data as cl_data
from chainlit.types import ThreadDict
from chainlit.data.sql_alchemy import SQLAlchemyDataLayer
from msg_maker import make_a_req
from controller import apply

secret = subprocess.check_output(["chainlit", "create-secret"], text=True)
os.environ["CHAINLIT_AUTH_SECRET"] = secret
user_name = 'postgres'
pass_word = '123456'
host = 'localhost'
port = 5432
database = 'zebura'

cl_data._data_layer = SQLAlchemyDataLayer(
    conninfo = f'postgresql+asyncpg://{user_name}:{pass_word}@{host}:{port}/{database}',
    ssl_require = False,  # 如果需要SSL连接的话
    storage_provider = "credentials",  # 如果有的话
    user_thread_limit = 1000,  # 你的用户线程限制111
)

@cl.password_auth_callback
#用户名密码登录
def auth_callback(username: str):
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(cl_data._data_layer.get_user(identifier=username))
    if username==data.identifier:
        return cl.User(identifier=username,metadata={"role": "admin", "provider": "credentials"})
    return None

@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict):
    print("The user resumed a previous chat session!")

@cl.on_chat_start
def on_chat_start():
    print(f"A new chat session has started! {cl.user_session.get('id')}")
    cl.user_session.set("context", [])

@cl.on_message
async def main(message: cl.Message):
    context = cl.user_session.get("context")
    if context is None:
        context=[]
    request = make_a_req(message.content)
    if len(context) > 1:
        request['status'] = "hold"
    resp = asyncio.run(apply(request))
    start_index = resp['msg'].find('Note:')
    if start_index != -1:
        resp['msg'] = resp['msg'][:start_index]
    answer = f"**ANSWER**:\n{resp['msg']} \n\n**NOTE**:\n{resp['note']} \n\n**STEP**:\n{resp['step']}"
    await cl.Message(content=answer).send()

if __name__ == "__main__":
    from chainlit.cli import run_chainlit
    run_chainlit(__file__)