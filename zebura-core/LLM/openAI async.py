# 使用异步方法调用openAI的GPT-3.5-turbo模型

import os
import pandas as pd
import asyncio
from openai import AsyncOpenAI

client = AsyncOpenAI(
    api_key="sk-xxxxxxxxxxxxxx"

)

# define the task of async
async def callGPT(query): 
    print(f'say {query} to GPT')
    chat_completion = await client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content":"你是一个编程助手，可以将自然语言的查询转换为SQL查询。"
            },
            {
                "role": "user",
                "content": query,
            }
        ],
        model="gpt-3.5-turbo",
    )
    print(chat_completion.choices[0].message.content)
    
    return chat_completion.choices[0].message.content

    
async def main() -> None:
    
    df_answer =  pd.read_excel("doublePara.xlsx")
    #append a new colum to df_answer
    df_answer['GPT'] = ''
    print(df_answer.shape)

    # create a task list
    tasks = []
    prompt = "请对下面句子进行改写，要求用中文并且保持原意不变\n"
    for i in range(df_answer.shape[0]):
        query= prompt + df_answer.loc[i, "指代问题的上一轮问题"] # + "\n" + df_answer.loc[i, "指代问题"]
        task = asyncio.create_task(callGPT(query))
        tasks = tasks + [task]

    print(len(tasks))
    
    # 每次只执行100个任务
    batch_size = 100
    for i in range(0, len(tasks), batch_size):
        await asyncio.gather(*tasks[i:i+batch_size])

    # get the result of the task
    for i, task in enumerate(tasks):
        
        new_answer = task.result()
        df_answer.loc[i, "GPT"] = new_answer

    # save the dataframe to excel file
    print(df_answer.shape)
    df_answer.to_excel("answer.xlsx", index=False)

    print("Done!")

asyncio.run(main())
