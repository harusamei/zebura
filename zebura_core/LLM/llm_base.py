import http.client
import json
import os
import sys
sys.path.insert(0, os.getcwd())
from settings import z_config
import openai

# openai 转发, https://peiqishop.cn/
# agentName： OPENAI, CHATANYWHERE
class LLMBase:

    def __init__(self,agentName:str,model="gpt-3.5-turbo"):

        self.agentName = agentName
        sk = z_config['LLM',f'{agentName}_KEY']
        
        self.model = model
        messages=[{'role': 'user', 'content': 'this is test, are you llama developed by metadata?'}]
        if agentName == 'OPENAI':
            openai.api_key=sk
            self.client = openai

        elif agentName == 'CHATANYWHERE':
            self.client = http.client.HTTPSConnection("api.chatanywhere.tech")
            self.headers = {
                    'Authorization': sk,
                    'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
                    'Content-Type': 'application/json'
                }
        try:
            print("test connection by Q: who are you to LLM\n A:"+self.postMessage(messages))
        except Exception as e:
            raise ValueError("LLM agent is not available",e)

    # 不同的agent有不同的处理方式
    # 在OpenAI的GPT-3聊天模型中，`messages` 是一个列表，用于表示一系列的对话消息。
    # `role`：这个字段表示消息的发送者。它可以是 `"system"`、`"user"` 或 `"assistant"`。
    # "system"` 通常用于设置对话的初始上下文，`"user"` 和 `"assistant"` 分别表示用户和助手的消息。
    # `content`：这个字段表示消息的内容，也就是实际的文本。

    def postMessage(self,messages:list):
        
        if self.agentName == 'CHATANYWHERE':
            payload = {"model": self.model}
            payload["messages"] = messages
            res = self.client.request("POST", "/v1/chat/completions", json.dumps(payload), self.headers)
            res = self.client.getresponse().read()
            res = json.loads(res.decode("utf-8"))
            data = res['choices'][0]['message']['content']
        elif self.agentName == 'OPENAI':
            res = self.client.ChatCompletion.create(
                                                        messages=messages,
                                                        model=self.model,
                                                        stop=["#;\n\n"]
                                                        )
            data = res.choices[0].message.content
        return data

# Example usage
if __name__ == '__main__':
    agent = LLMBase('OPENAI')
    print(agent.postMessage([{'role': 'user', 'content': 'Who won the world series in 2020?'}]))
    agent = LLMBase('CHATANYWHERE')
    print(agent.postMessage([{'role': 'user', 'content': 'Who won the world series in 2020?'}]))