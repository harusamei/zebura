import os
import sys
sys.path.insert(0, os.getcwd())
import settings
import logging
import re
from zebura_core.knowledges.schema_loader import Loader
from zebura_core.utils.compare import similarity

# schema linking, for table, column
# todo must match schema
class Sch_linking:

    def __init__(self,scha_file):
        self.similarity = similarity()
        self.info_loader = Loader(scha_file)
        logging.info("Schema linking init done")

    def link_table(self, term):
        name_list = self.info_loader.get_table_nameList()
        # 名称完美匹配
        if term in name_list:
            return term
        # 输出最可能的表名
        table_dict ={}
        tables = self.info_loader.tables
        for table in tables:
            temStr = f"{table['table_name']},{table.get('name_zh','')},{table.get('alias','')},{table.get('alias_zh','')}"
            temList = re.split(',+\s*|;+\s*',temStr) 
            table_dict[table['table_name']] = ','.join(temList)
        
        like_item = self.get_like_item(term,table_dict)    
        return like_item['name']
            
    def link_field(self, term, table_name=None):
        # 如果是数字或者符号，直接返回
        if re.match(r'^[^a-zA-Z\u4e00-\u9fa5]+$', term):
            return table_name, term
        
        column_dict = {}
        if table_name is not None:
            table = self.info_loader.get_table_info(table_name)
            tables = [table]
        else:
            tables = self.info_loader.tables

        for table in tables:
            columns = table['columns']
            table_name = table['table_name']
            for column in columns:
                temStr = f"{column['column_name']},{column.get('name_zh','')},{column.get('alias','')},{column.get('alias_zh','')}"
                temList = re.split(',+\s*|;+\s*',temStr) 
                column_dict[f"{table_name}==={column['column_name']}"] = ','.join(temList)
        like_item = self.get_like_item(term,column_dict)
        
        table_name, _, field_name = like_item['name'].partition('===')        
        return table_name, field_name
    
    def refine(self,slots1):
        if slots1 is None or slots1.get('from') is None:
            return None
        
        slots = slots1.copy()
        tableName = slots['from']
        st_table = self.link_table(tableName)
        tableName = slots['from'] = st_table
        
        columns = slots['columns']
        for idx, column in enumerate(columns):
            st_table, st_col = self.link_field(column, tableName)
            columns[idx] = st_col
            
        # conditions
        for cond in slots.get('conditions', []):
            if isinstance(cond, str):
                continue
            st_table, st_col = self.link_field(cond['column'], tableName)
            cond['column'] = st_col
            
        return slots
    
    def get_like_item(self, term, items_dict):
        like_item = {'score':-1,'name':''}
        for key in items_dict.keys():
            s = self.similar(term,items_dict[key].split(','))
            if s['score'] > like_item['score']:
                like_item['score'] = s['score']
                like_item['name'] = key
        return like_item

    def similar(self,term,candidates):
        lang = self.similarity.getLang(term)
        candidates = [c for c in candidates if self.similarity.getLang(c) == lang and c != '']
        matched={'score':-1}
        
        for ref in candidates:
            s = self.similarity.getUpperSimil(term,ref)
            if s > matched['score']:
                matched['score'] = s
                matched['name'] = ref
        return matched
    
# Example usage
if __name__ == '__main__':
    cwd = os.getcwd()
    name= 'training/ikura/ikura_meta.json'
    sch_linking = Sch_linking(os.path.join(cwd, name))
    slots = {'from': 'products', 'columns': ['brand name', 'item price'], 'conditions': [{'column': 'brand', 'op': '=', 'value': '联想'}]}
    slots = {
        'columns': ['COST','market time'],'from': 'sale_info'
        }
    print(slots)
    result = sch_linking.refine(slots)
    print(result)