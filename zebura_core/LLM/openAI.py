# 使用异步方法调用openAI的GPT-3.5-turbo模型
import sys
import os
import pandas as pd
import asyncio
from openai import AsyncOpenAI
sys.path.insert(0, os.getcwd())
import settings
    
client = AsyncOpenAI(
    api_key=os.environ["OPENAI_KEY"],
)

# define the task of async
async def askGPT_query(query): 
    print(f'say {query} to GPT')
    chat_completion = await client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content":"你是一个编程助手，你能够将用户的自然语言查询转换为SQL"
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
    print(f"Start to process {fileName}")
    df_answer =  pd.read_csv(fileName,encoding='utf-8-sig')
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
    df_answer.to_csv(f"{fileName}_gpt.csv",encoding='utf-8-sig')

    print("Done!")

if __name__ == '__main__':
    
    prompt = "下面自然语言如果与查询产品相关，请转换为sql，如果不相关直接输出no sql\n"
    asyncio.run(askGPT_query(prompt + "请问联想的智能投影仪多少钱一台？"))
    # path = os.getcwd()
    # asyncio.run(askGPT_file(path+"\datasets\samples.csv",prompt))
