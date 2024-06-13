import chainlit as cl
import asyncio
from controller import apply
from msg_maker import make_a_req
# 设置全局变量

@cl.on_chat_start
def on_chat_start():
    print(f"A new chat session has started! {cl.user_session.get('id')}")
    cl.user_session.set("context", [])

@cl.on_message
async def main(message: cl.Message):
    context = cl.user_session.get("context")
    request = make_a_req(message.content)
    context.append(request)

    request['context'] = context    
    if len(context) > 1:
        request['status'] = "hold"

    resp = asyncio.run(apply(request))
    context.append(resp)
    if resp['status'] == "failed":
        answer = f"ANSWER:\n{resp['msg']}\nHINT:\n{resp['hint']}"
    else:
        answer = f"ANSWER:\n{resp['msg']}\nNOTE:\n{resp['note']}"
    cl.user_session.set("context", context)
    await cl.Message(content=answer).send()

@cl.on_chat_end
def on_chat_end():
    print("The user disconnected!")


if __name__ == "__main__":

    from chainlit.cli import run_chainlit
    run_chainlit(__file__)