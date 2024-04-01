import os
import sys
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from info_loader import InfoLoader
from esQuery import ESQuery

class Executor:

    def __init__(self):

        self.db_info = InfoLoader()
        host = self.db_info['host']
        port = int(self.db_info['port'])
        if self.db_info['type'] == 'elasticsearch':
            print(host,port)
            self.es = ESQuery(host,port)
        

    def get_top(self, index, field):
        return self.es.query_top(index, field)

    def get_range(self, index, field, upper, lower):
        return self.es.query_range(index, field, upper, lower)

    def get_avg(self, index, field):
        return self.es.query_average(index, field)

    def get_max_min(self, index, field, most):
        return self.es.query_max_min(index, field, most)

    def get_table_info(self):
        return self.es.query_table_info()

    def get_table_head(self):
        return self.es.query_table_head()

    def get_table_column(self, column):
        return self.es.query_table_column(column)

    def get_sql_pat(self, key):
        return self.es.query_sql_pat(key)
    
if __name__ == '__main__':
    executor = Executor()
