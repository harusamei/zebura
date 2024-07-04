import asyncio
import multiprocessing
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from zebura_core.LLM.llm_agent import LLMAgent

llm = LLMAgent("CHATANYWHERE","gpt-3.5-turbo")

async def async_task(name, delay, queue):
    print(f"Task {name} started")
    result = await llm.ask_query("how are you",f"the current work process is {name}") 
    print(result)
    
    print(f"Task {name} completed after {delay} seconds")
    queue.put(f"Task {name} completed")

def process_task(name, delay, queue):
    asyncio.run(async_task(name, delay, queue))

if __name__ == "__main__":
    # 创建一个队列
    queue = multiprocessing.Queue()

    # 创建两个子进程
    process1 = multiprocessing.Process(target=process_task, args=("Process 1", 2, queue))
    process2 = multiprocessing.Process(target=process_task, args=("Process 2", 4, queue))

    # 启动两个子进程
    process1.start()
    process2.start()

    # # 等待两个子进程完成
    process1.join()
    # process2.join()

    # 从队列中获取并打印消息
    while not queue.empty():
        print(queue.get())