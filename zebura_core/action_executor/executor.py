import os
import sys
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from esQuery import ESQuery

# 数据存放方式不同，需要重新设计
class Executor:

    def __init__(self):
        
        self.es = ESQuery()

        self.results = None
        self.history = []
        self.tracks = []

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
        
        return True

    def output_fields(self, **kwargs):
        input = self.results
        fields = kwargs.get('fields')
        print(f"output_fields: {input}, {fields}")
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
                        'fields':['product_name','category']
                    }
                }       
             }
    #ex.assign_tasks(action)
    print(ex.es.cat.indices(format='json'))
    #ex.search_by_field(field='product_name',value=None,index='products')
