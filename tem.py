import multiprocessing
from zebura_core.LLM.llm_agent import LLMAgent

class MyClass:
    instance_count = 0
    def __init__(self, data):
        MyClass.instance_count += 1
        #self.llm = LLMAgent("AZURE","gpt-3.5-turbo")

        print(f"MyClass {MyClass.instance_count}th init success")
        self.data = data

    def process_data(self, item):
        # 类的方法逻辑
        result = f"Processed {item} with data {self.data}"
        return result

def worker(instance, item):
    # 全局函数，调用类的方法
    return instance.process_data(item)

def run_concurrent_processes(instance, items):
    # 创建进程池
    for item in items:
        p = multiprocessing.Process(target=worker, args=(instance, item))
        p.start()
        p.join()
    
    return [1,2,3]

if __name__ == "__main__":
    instance = MyClass("example data")
    items_to_process = ["item1", "item2", "item3"]

    # 并发执行类的方法
    results = run_concurrent_processes(instance, items_to_process)

    for result in results:
        print(result)
