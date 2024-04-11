# load 关于要查询的DB的信息，以及good cases
import yaml
import os
import sys
import json
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from typing import Dict

# load schema of tables
class SchemaLoader:
    def __init__(self,file=None):
        # default to load the tables to be queried 
        if file is None:
            cwd = os.getcwd()
            name = z_config['Tables','schema']  
            file = os.path.join(cwd, name)

        self._info = self.load_tableInfo(file)
        self.tables = self._info["tables"] # [{}],每个表一个dict
        
    def load_tableInfo(self,file) -> Dict[str,str]:
        
        # Load the config file
        with open(file, 'r',encoding='utf-8-sig') as file:
            info_dict = json.load(file)
        return info_dict
    
    def get_tableList(self):
        return [table["table"] for table in self.tables]
    
    # table 在ES中的index名可以不同
    def get_indexList(self):
        return [table["es_index"] for table in self.tables]
    
    def get_table(self,tableName) -> Dict[str,str]:
        tDict = next((table for table in self.tables if table["table"] == tableName), None)
        return tDict
    # 表信息或表头信息
    def get_column(self, tableName, columnName) -> Dict[str,str]:
        
        columns = self.get_table(tableName).get("columns")
        result = next((column for column in columns if column["column_en"] == columnName), None)
        return result
    
    def get_columnList(self, tableName):
        return  self.get_table(tableName).get("columns")
    
    def __getitem__(self, key):
        return self._info.get(key)

# Example usage
if __name__ == '__main__':
    # Load the SQL patterns
    cwd = os.getcwd()
    name = z_config['Tables','schema']  
    file = os.path.join(cwd, name)
    print(file)
    loader = SchemaLoader(file)
    print(loader.tables[0]["table"])
    print(loader.get_column('product','brand'))
    print(loader.get_table('product').get('alias_en'))

    