# 扫描存放在csv里的table信息，生成schema           #
##################################################
import sys
import os
import asyncio
import re
sys.path.insert(0, os.getcwd())
from settings import z_config
from LLM.llm_agent import LLMAgent
import LLM.agent_prompt as ap
from utils.csv_processor import pcsv

class Scanner:
    def __init__(self):
        self.pcsv = pcsv()
        self.llm = LLMAgent()
        self.table_keys = ["table_name", "desc", "desc_zh", "name_zh", "name", "alias_zh", "alias", "columns"]
        self.column_keys = ["column_name", "name_zh", "name", "alias_zh", "alias","type", "length", "desc", "desc_zh"]
        self.tableInfo = {}
    
    async def scan_table_from_csv(self, csv_filename):
        csv_rows = self.pcsv.read_csv(csv_filename)
        if csv_rows[0].get("table_name") is None:
            print("Error: table_name not found in csv")
            return None

        self.tableInfo= csv_rows[0]
        for row in csv_rows[1:]:
            column_name = row.get("column_name")
            if row.get("desc") is None or len(row.get("desc")) == 0:
                row['desc'] = await self.complete_info(column_name, "desc")
            if row.get("alias") is None or len(row.get("alias")) == 0:
                row['alias'] = await self.complete_info(column_name, "alias")

        filename = os.path.basename(csv_filename)
        csv_out = csv_filename.replace(filename, f"new_{filename}")
        
        self.pcsv.write_csv(csv_rows,csv_out)
    
    async def complete_info(self, column_name, task):
        if task not in ["desc", "alias"]:
            return None
        
        if task == "desc":
            task = "please provide the defination of the given column name.\n" 
            task += f"data table:{self.tableInfo['table_name']}, is about {self.tableInfo['desc']}. \n"
            one_shot = 'Q: "column_name: brand"\n'
            one_shot += 'A: a product version of it that is made by one particular manufacturer.\n'
        if task == "alias":
            task = "please provide the alias of the given column name.\n" 
            task += f"data table:{self.tableInfo['table_name']}, is about {self.tableInfo['desc']}. \n"
            one_shot = 'Q: "column_name: brand"\n'
            one_shot += 'A: company; trademark; manufacturer\n'

        content = f"Q: {column_name}\n"
        prompt = task+one_shot
        result = await self.llm.ask_query(content, prompt)
        if result.startswith("A:"):
            result = result[3:]
        else:
            result = ""
        return result
    
    def generate_schema(self):
        pass
    
# example usage
if __name__ == '__main__':
    scanner = Scanner()
    asyncio.run(scanner.scan_table_from_csv("C:\something\zebura\\training\it\dbInfo\product_info.csv"))