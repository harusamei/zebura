
import requests
import json

def request_chatgpt(query):
    url = "https://openai-lr-ai-platform-cv.openai.azure.com/openai/deployments/IntentTest/chat/completions?api-version=2023-07-01-preview"

    header = {
        "Content-Type": "application/json",
        "api-key": "d500066ba22d46a982d7db918b512707"  # Replace with your actual GPT-4 Turbo API key
    }

    post_dict = {
        "frequency_penalty": 0,
        "presence_penalty": 0,
        "top_p": 0.95,
        "temperature": 0.9,
        "messages": [
            {
                "role": "user",
                "content": "我有一个产品数据库，请生成下面句子对应的SQL\n"+query
            }
        ]
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
querys = []
querys.append("我爷爷奶奶结婚的时候为什么不邀请我")
querys.append("请问小新电脑是什么品牌的")
querys.append("今天天气挺好的，你觉得呢？")
for query in querys:
    result = request_chatgpt(query)
    print(result)

