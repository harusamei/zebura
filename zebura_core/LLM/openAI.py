# 使用异步方法调用openAI的GPT-3.5-turbo模型
import sys
import os
import pandas as pd
import asyncio
from openai import AsyncOpenAI

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from settings import load_config

if not os.environ.get("OPENAI_API_KEY"):
   load_config()

client = AsyncOpenAI(
    api_key=os.environ["OPENAI_API_KEY"],
)

# define the task of async
async def askGPT_query(query): 
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

# 所有query存于csv第一列
# 格式 query, context
async def askGPT_file(fileName,prompt):
    
    df_answer =  pd.read_excel(fileName)
    #append a new colum to df_answer
    df_answer['answer'] = ''
    print(df_answer.shape)

    # create a task list
    tasks = []
    for i in range(df_answer.shape[0]):
        query= prompt + df_answer.loc[i, "query"] # + "\n" + df_answer.loc[i, "context"]
        task = asyncio.create_task(askGPT_query(query))
        tasks = tasks + [task]

    print(f"{fileName}: total {len(tasks)} queries")    
    
    # 每次只执行100个任务
    batch_size = 100
    for i in range(0, len(tasks), batch_size):
        await asyncio.gather(*tasks[i:i+batch_size])

    # get the result of the task
    for i, task in enumerate(tasks):
        
        new_answer = task.result()
        df_answer.loc[i, "answer"] = new_answer

    # save the dataframe to excel file
    print(df_answer.shape)
    df_answer.to_excel(f"{fileName}_gpt.xlsx", index=False)

    print("Done!")

asyncio.run(askGPT_query("联想智能插座多少钱一只？"))
