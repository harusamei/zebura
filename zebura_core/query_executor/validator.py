# 基于es，检查 action格式，action内容是否合法
import os
import sys
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from esQuery import ESQuery

def find_key(one_dict, key):
        all_vals = []
        for k, v in one_dict.items():
            if k == key:
                if isinstance(v, list):
                    all_vals.extend(v)  
                else:
                    all_vals.append(v)
            elif isinstance(v, dict):
                all_vals.extend(find_key(v, key))

        return list(set(all_vals))

def find_all_keys(one_dict):
        all_keys = []
        for k, v in one_dict.items():
            all_keys.append(k)
            if isinstance(v, dict):
                all_keys.extend(find_all_keys(v))
        
        return list(set(all_keys))

class Validator:

    def __init__(self) -> None:
        
        self.es = ESQuery()
        indices = self.es.all_indices
        self.indices = [index.get('index') for index in indices]
        self.index_columns ={}
        for index in self.indices:
            columns = self.es.get_fields(index)
            self.index_columns[index] = columns

        self.critical_keys = ['table','doFunc','func','field','value','kwargs']
    
    def check_action(self, action):
        # check if all critical keys are in action
        all_keys = find_all_keys(action)
        
        for crit in self.critical_keys:
            if not crit in all_keys:
                return False
        
        index = action.get('table')
        do_func = action.get('doFunc')
        if not index in self.indices:
            return False
        if not do_func:
            return False
        
        fieldList = find_key(action, 'fields')+find_key(action, 'field')
        for field in fieldList:
            if not field in self.index_columns[index]:
                return False
        
        return True
    @property
    def all_indices(self):
        return self.indices
    
    def get_funcs(self,action):
        return find_key(action, 'func')
        
    def get_fields(self,index):
        return self.index_columns.get(index)

  # Example usage  
if __name__ == '__main__':
    v = Validator()
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
    if not v.check_action(action):
        print("Invalid action")
    else:
        print("Valid action")
        
   