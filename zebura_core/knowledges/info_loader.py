# load 关于要查询的DB的信息，以及good cases
import yaml
import os
import sys
import json
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from typing import Dict

# 读metadata of table
class InfoLoader:
    def __init__(self):
        self._info = self.load_tableInfo()
        self.tables = self._info["tables"] # [{}],每个表一个dict
        
    def load_tableInfo(self) -> Dict[str,str]:
        cwd = os.getcwd()
        path = z_config['Paths','KnowledgePath']
        name = z_config['Info','table_info']  
        file = os.path.join(cwd, path, name)
        # Load the config file
        with open(file, 'r',encoding='utf-8-sig') as file:
            info_dict = json.load(file)
        return info_dict
    
    @property
    def tableList(self):
        return self._info.get("table_list")
    
    def get_table(self,tableName) -> Dict[str,str]:
        tDict = next((table for table in self.tables if table["table"] == tableName), None)
        return tDict
    # 表信息或表头信息
    def get_column(self, tableName, columnName) -> Dict[str,str]:
        
        columns = self.get_table(tableName).get("columns")
        result = next((column for column in columns if column["column_en"] == columnName), None)
        return result

    def __getitem__(self, key):
        return self._info.get(key)
    
# 读模板
class PatLoader:
    def __init__(self):
        
        self._sql_pats = self.load_pat()
    
    def load_pat(self) -> Dict[str,str]:
        cwd = os.getcwd()
        path = z_config['Paths','KnowledgePath']
        name = z_config['Info','sql_re']  
        sp_file = os.path.join(cwd, path, name)

        # Load the config file
        with open(sp_file, 'r',encoding='utf-8') as file:
            pats = yaml.safe_load(file)

        sql_pats = dict()
        # Save the environment variables in os
        for key, value in pats.items():
            sql_pats[key] = str(value)
        return sql_pats
    
    @property
    def sql_pats(self):
        return self._sql_pats
    
    def __getitem__(self, key):
        return self._sql_pats[key]

# Example usage
if __name__ == '__main__':
    # Load the SQL patterns
    loader = InfoLoader()
    print(loader.tableList)
    print(loader.tables[0]["table"])
    print(loader.get_column('product','brand'))
    print(loader.get_table('product').get('alias_en'))

    