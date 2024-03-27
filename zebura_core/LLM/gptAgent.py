import sys
import os
import requests
import json
import asyncio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from settings import load_config

async def askGPT_agent(queries,prompt):

    if not os.environ.get("GPT_AGENT_API_KEY"):
        load_config()

    url = "https://openai-lr-ai-platform-cv.openai.azure.com/openai/deployments/IntentTest/chat/completions?api-version=2023-07-01-preview"
    header = {
        "Content-Type": "application/json",
        "api-key": "d500066ba22d46a982d7db918b512707"  # Replace with your actual GPT-4 Turbo API key
    }
    messages = [{"role": "system", "content": prompt}]
    messages.append({"role": "user", "content": "\n".join(queries)})  # Convert the list of queries to a string with newlines between the]
    print(messages)
    post_dict = {
        "frequency_penalty": 0,
        "presence_penalty": 0,
        "top_p": 0.95,
        "temperature": 0.95,
        "messages": messages
    }

    try:
        response = requests.post(url, headers=header, data=json.dumps(post_dict))
        response.raise_for_status()  # Raise an error for bad responses
        result = response.json()

        # Simplify the output by extracting relevant information
        simplified_result = {
            "choices": result.get("choices", []),
            "usage": result.get("usage", {})
        }
        return simplified_result
    except requests.exceptions.RequestException as err:
        print(f"Error: {err}")
        return None


# Example usage
    
if __name__ == '__main__':

    querys = ["我有一个products表，如果下面句子可以转换为SQL查询products,请转为SQL,如果不能，请直接输出 not sql",
                "请问联想小新电脑多少钱",
                "联想小新电脑多少钱",
                "请问小新电脑是什么品牌的",
                "今天天气挺好的，你觉得呢？"]

    answers = asyncio.run(askGPT_agent(querys,"你是一个编程助手，可以将自然语言转化为SQL查询："))
    print(answers)