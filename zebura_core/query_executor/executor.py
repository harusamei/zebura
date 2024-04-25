import os
import sys
sys.path.insert(0, os.getcwd())
from settings import z_config
from tools.es_searcher import ESearcher
from validator import Validator
# 数据存放方式不同，需要重新设计
class Executor:

    def __init__(self):
        
        self.es = ESQuery()
        self.validator = Validator()

        funcs = [method for method in dir(self) if callable(getattr(self, method))]
        self.funcs = [func for func in funcs if not func.startswith('__')]
        self.results = None
        self.history = []
        self.tracks = []

    def run(self, action):
        if not self.check_action(action):
            return False
        if not self.assign_tasks(action):
            return False
        print('done')
        return True
    
    def check_action(self, action):
        flag = self.validator.check_action(action)
        if not flag:
            self.tracks.append(f'failed action check')
            return False
        
        funcList = self.validator.get_funcs(action)
        for func in funcList:
            if not func in self.funcs:
                self.tracks.append(f'failed action check, {func} is NOT an executor method')
                return False
        return True

    # 任务分发, action是一个字典，包含了需要执行的任务
    # 假设一个action中只有一个table，一个doFunc和一个showFunc
    # assume action 信息已经check，不存在低级错误
    def assign_tasks(self, action):
        
        index = action.get('table')
        do_func = action.get('doFunc')
        show_func = action.get('showFunc')
        
        funcName = do_func.get('func')
        kwargs = do_func.get('kwargs')
        kwargs['index'] = index
        # 通过getattr获取方法并调用
        flag = getattr(self, funcName)(**kwargs)
        if not flag:
            self.tracks.append(f'failed when call {funcName}')    
            return False
        
        if show_func is None:
            return True
        funcName = show_func.get('func')
        kwargs = show_func.get('kwargs')
        flag = getattr(self, funcName)(**kwargs)
        if not flag:
            self.tracks.append(f'failed when call {funcName}')
            return False
        
        return True
    
    # query records where field = value
    def search_by_field(self, **kwargs):
        # 实际函数需要的参数
        index = kwargs.get('index')
        field = kwargs.get('field')
        value = kwargs.get('value')
        if not (index and field and value):
            return False
        
        self.results = self.es.query_word(index,field,value)
        if not self.results:
            self.tracks.append(f'failed when call search_by_field')
            return False
        
        return True
    
    # 输出结果中指定的字段
    def output_fields(self, **kwargs):
        count = self.results.get('hits').get('total').get('value')
        if count == 0:
            print("No result found")
            return True
        allhits = self.results.get('hits').get('hits')
        fields = kwargs.get('fields')
        print(f"output_fields: {count} {allhits}, {fields}")
        output = []
        # 只输出前limit个结果
        for hit in allhits:
            temDict = {}
            for field in fields:
                temDict[field] = hit["_source"].get(field)
            output.append(temDict)  
        
        print(output)
        return True

   
# Example usage  
if __name__ == '__main__':
    ex = Executor()
    action = {'table':'leproducts',
              'doFunc': {
                  'func':'search_by_field', 
                  'kwargs':{
                      'field':'product_name',
                      'value':'小新'
                       }
                    },
                'showFunc': {
                    'func':'output_fields',
                    'kwargs':{
                        'fields':['brand','product_cate1']
                    }
                }       
             }
    ex.run(action)
