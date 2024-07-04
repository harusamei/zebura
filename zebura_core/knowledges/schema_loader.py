# load 关于要查询的DB的schema信息, 这个信息做为一种知识用于schema linkage,validation and so on
import os
import json
from typing import Dict
import logging

class Loader:
    def __init__(self,file):
        
        # schema 必须存在，否则raise error
        self._info = self.load_schema(file)
        if "tables" not in self._info:
            logging.critical("tables not in schema file")
        self.tables = self._info["tables"] # [{}],每个表一个dict
        self.project = self._info.get("_project_code","")
        logging.debug("Loader init success")
        
    def load_schema(self,file) -> Dict[str,str]:
        
        # Load the config file
        try:
            with open(file, 'r',encoding='utf-8-sig') as file:
                info_dict = json.load(file)
        except Exception as e:
            raise ValueError(f"Cannot load the schema file{e}")            
        return info_dict
    
    def get_table_nameList(self):
        return [table["table_name"] for table in self.tables]
    
    def get_table_info(self,tableName) -> Dict[str,str]:
        tDict = next((table for table in self.tables if table["table_name"] == tableName), None)
        return tDict
    
    def get_all_columns(self,tableName): # [Dict]
        columns = self.get_table_info(tableName).get("columns")
        return columns
    
    # 表信息或表头信息
    def get_column(self, tableName, columnName) -> Dict[str,str]:
        table = self.get_table_info(tableName)
        columns = table.get("columns", None)
        result = next((column for column in columns if column["column_name"] == columnName), None)
        return result
    

# Example usage
if __name__ == '__main__':
    # Load the SQL patterns
    cwd = os.getcwd()
    name = 'training/amazon/amazon_meta.json'
    file = os.path.join(cwd, name)
    print(file)
    loader =Loader(file)
    print(loader.tables[0]["table_name"])
    print(loader.get_column('product','brand'))
    print(loader.get_table_info('product').get('desc'))

    