import chainlit as cl
import asyncio
from controller import apply
# 设置全局变量

# chainlit与 controller 之间的交互的request格式
# request={
#         "msg": content,
#         "context": context,
#         "type": "user/assistant", # 用户， controller
#         "format": "text/md/sql...", # content格式，与显示相关
#         "status": "new/hold/failed/succ", # 新对话,多轮继续；controller查询失败；查询成功
#     } 

@cl.on_chat_start
def on_chat_start():
    print(f"A new chat session has started! {cl.user_session.get('id')}")
    cl.user_session.set("context", [])

@cl.on_message
async def main(message: cl.Message):
    context = cl.user_session.get("context")
    request={
        "msg": message.content,
        "context": context,
        "type": "user", # 用户， 机器人，内部执行的中间结果
        "format": "text", # 与显示相关的格式信息
        "status": "new", # 任务状态
    }
    if len(context) > 0:
        request['status'] = "hold"
    answer = asyncio.run(apply(request))

    context.append(answer)
    cl.user_session.set("context", context)
    await cl.Message(content=answer['msg']).send()

@cl.on_chat_end
def on_chat_end():
    print("The user disconnected!")


if __name__ == "__main__":

    from chainlit.cli import run_chainlit
    run_chainlit(__file__)