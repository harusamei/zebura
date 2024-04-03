import os
import sys
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from esQuery import ESQuery
from action_executor import constants

class Executor:

    def __init__(self):
        host = os.environ['ES_HOST']
        port = int(os.environ['ES_PORT'])
        self.es = ESQuery(host,port)
        self.history = []

    # SELECT Category FROM Products WHERE ProductName = '鼠标'; 
    # ['select','sql_from', 'where', 'distinct', 'limit', 'offset', 'group_by', 'order_by', 'as', 'like']
    def assign_action(self, action):

        do_func = action['do']
        output_func = action['output']
        result = []
        temStr = f'result =self.{do_func(action)})'
        exec(temStr)
        temStr = f'result = self.{output_func}(result)'
        return result
        
    def select_columns(self, action):
        self.history.append(action)
        index = action['sql_from']
        fields = action['select']
        return "select_columns"

    # def select_columns_where(self, index, field, value):
    #     return self.es.query_where(index, field, value)
    
    # def select_all(self, index):
    #     return self.es.query_all(index)
    
    # def get_range(self, index, field, upper, lower):
    #     return self.es.query_range(index, field, upper, lower)

    # def get_avg(self, index, field):
    #     return self.es.query_average(index, field)

    # def get_max_min(self, index, field, most):
    #     return self.es.query_max_min(index, field, most)

    # def get_table_info(self):
    #     return self.es.query_table_info()

    # def get_table_head(self):
    #     return self.es.query_table_head()

    # def get_table_column(self, column):
    #     return self.es.query_table_column(column)

    # def get_sql_pat(self, key):
    #     return self.es.query_sql_pat(key)
    
if __name__ == '__main__':
    executor = Executor()
