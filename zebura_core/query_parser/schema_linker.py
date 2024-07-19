# SQL与数据库schema 对齐
# 基于 XXX_meta.json 文件，对SQL进行解析，将SQL中的表名、字段名与数据库schema对齐
# 不涉及具体值的修正
############################################
import os
import sys

sys.path.insert(0, os.getcwd())
import settings
import logging
import re
from zebura_core.knowledges.schema_loader import Loader
from zebura_core.utils.compare import similarity
import datetime


# schema linking, for table, column
class Sch_linking:

    def __init__(self, scha_file, scha_loader=None):
        self.similarity = similarity()
        if scha_loader is not None:
            self.info_loader = scha_loader
        elif scha_file is not None:
            self.info_loader = Loader(scha_file)
        else:
            raise ValueError("No schema file or schema loader")
        logging.info("Schema linking init done")

    def link_table(self, term):
        name_list = self.info_loader.get_table_nameList()
        # 名称完美匹配
        if term in name_list:
            return term
        # 输出最可能的表名
        table_dict = {}
        tables = self.info_loader.tables
        for table in tables:
            temStr = f"{table['table_name']},{table.get('name_zh', '')},{table.get('alias', '')},{table.get('alias_zh', '')}"
            temList = re.split(',+\s*|;+\s*', temStr)
            table_dict[table['table_name']] = ','.join(temList)

        like_item = self.get_like_item(term, table_dict)
        return like_item['name']

    def link_field(self, term, table_name=None):
        if '*' in term:
            return '*', '*'

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
                temStr = f"{column['column_name']},{column.get('name_zh', '')},{column.get('alias', '')},{column.get('alias_zh', '')}"
                temList = re.split(',+\s*|;+\s*', temStr)
                column_dict[f"{table_name}==={column['column_name']}"] = ','.join(temList)
        like_item = self.get_like_item(term, column_dict)

        table_name, _, field_name = like_item['name'].partition('===')
        return table_name, field_name

    def get_like_item(self, term, items_dict):
        like_item = {'score': -1, 'name': ''}
        for key in items_dict.keys():
            s = self.similar(term, items_dict[key].split(','))
            if s['score'] > like_item['score']:
                like_item['score'] = s['score']
                like_item['name'] = key
        return like_item

    def similar(self, term, candidates):
        lang = self.similarity.getLang(term)
        candidates = [c for c in candidates if self.similarity.getLang(c) == lang and c != '']
        matched = {'score': -1}

        for ref in candidates:
            s = self.similarity.getUpperSimil(term, ref)
            if s > matched['score']:
                matched['score'] = s
                matched['name'] = ref
        return matched


# Example usage
if __name__ == '__main__':
    cwd = os.getcwd()
    name = './training/amazon/amazon_meta.json'
    sch_linking = Sch_linking(os.path.join(cwd, name))
    slots = {'from': 'products', 'columns': ['brand name', 'item price'],
             'conditions': [{'column': 'brand', 'op': '=', 'value': '联想'}]}
    slots = {
        'columns': ['COST', 'market time'], 'from': 'sale_info'
    }
    print(slots)
