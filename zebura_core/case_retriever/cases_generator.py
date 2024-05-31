# 通过LLM生成DB的golden cases
import os
import sys
sys.path.insert(0, os.getcwd())
import re
from settings import z_config
import logging
from knowledges.schema_loader import Loader



class case_generator:
    def __init__(self):
        self.sch_loader = Loader(z_config['Training','db_schema'])
        logging.debug("case_generator init success")

    # 生成golden cases
    def gen_cases(self,table_name:str) -> dict:
        table_info = self.sch_loader.get_table_info(table_name)
        if table_info is None:
            return {   
                "zh":'',
                "en":''
            }
        
        desc = table_info.get("desc",'')
        columns = self.sch_loader.get_all_columns(table_name)
        columnInfo = [f"{c.get('name_zh','')}: {c.get('desc','')}" for c in columns]
        columnInfo = "\n".join(columnInfo)
        db_zh = (
                    f"表名是{table_info.get('name_zh','')}，用途是{desc}，包含的"
                    f"列及其含义如下\n{columnInfo}\n"
                )
        
        columnInfo = [f"{c.get('column_name','')}， {c.get('desc','')}" for c in columns]
        columnInfo = "\n".join(columnInfo)
        db_en = (
                    f"The table name is {table_info.get('name_en','')} and its purpose is {desc}. "
                    f"The columns and their meanings are as follows\n{columnInfo}\n"
                )
        return {"zh":db_zh,"en":db_en}
    
    # 生成golden cases
    def gen_cases(self,table_name:str) -> dict:
        table_info = self.sch_loader.get_table_info(table_name)
        if table_info is None:
            return {   
                "zh":'',
                "en":''
            }
        
        desc = table_info.get("desc",'')
        columns = self.sch_loader.get_all_columns(table_name)
        columnInfo = [f"{c.get('name_zh','')}: {c.get('desc','')}" for c in columns]
        columnInfo = "\n".join(columnInfo)
        db_zh = (
                    f"表名是{table_info.get('name_zh','')}，用途是{desc}，包含的"
                    f"列及其含义如下\n{columnInfo}\n"
                )
        
        columnInfo = [f"{c.get('column_name','
