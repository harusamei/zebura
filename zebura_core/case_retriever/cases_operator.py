#####################
# golden cases operator, 包括创建，增删改
# golden cases 存放在ES中，通过ES查询
# golden cases schema 文件名为<project_code>_gcases.json
#####################
import os
import sys
import logging
sys.path.insert(0, os.getcwd())

from settings import z_config
from utils.es_operator import ESOps
from utils.es_creator import ESIndex

from utils.csv_processor import pcsv
from knowledges.schema_loader import Loader

must_columns = {"query", "sql", "gt", "updated_date"}
class CaseOps(ESOps):
        
    def __init__(self):
        # golden cases 用ES存储，以 ESOps 为基类
        super().__init__()
        self.creator = ESIndex()
        # load schema of goodcases
        project_code = z_config['Training','project_code'] # project code约定为db schema的上级目录名
        self.gcase_index = f"{project_code}_gcases"  # index name 名字强制为<project_code>_gcases

        name = z_config['Training','db_schema']  # 'Training\ikura\ikura_meta.json'
        # 获得<project_code>_gcases.json
        name = name.replace('_meta','_gcases')
        cwd = os.getcwd()
        self.schema_path = os.path.join(cwd, name)    # golden cases schema由project_code决定
        self.loader = Loader(self.schema_path)        # schema loader
        tableList = self.loader.get_table_nameList()
        if tableList[0] != self.gcase_index:
            raise ValueError(f"Table_name in {project_code}_gcases.json should be the same as index name.")
        logging.debug("CaseOps init success")

    # 将cases存入ES
    def store_cases(self, csv_filename):
        es_schema = self.create_index()
        mypcsv = pcsv()
        csv_rows = mypcsv.read_csv(csv_filename)
        if not csv_rows:
            return False
        
        new_docs=[]
        #TODO, 此处不能模糊查询
        for doc in csv_rows:
            # 已经在Index里面的数据不需要再次插入
            #result = self.term_search(self.gcase_index, field="query", value=query["query"])
            result = self.search(self.gcase_index, {"query":doc["query"]})
            count = result['hits']['total']['value']
            if count>0:
                hit_doc = result['hits']['hits'][0]['_source']
                if hit_doc["query"] == doc["query"]:
                    #print(f"Query '{doc['query']}' already exists in index '{self.gcase_index}'.")
                    continue
            self.creator.format_doc(doc,es_schema)
            new_docs.append(doc)
        # 为新文档生成embedding
        print(new_docs)
        es_docs = self.creator.complete_embs(new_docs,es_schema)
        self.insert_docs(self.gcase_index, es_docs)
        return
    
    #将csv文件中的cases从golden cases中删除
    def delete_cases(self, csv_filename):
        mypcsv = pcsv()
        csv_rows = mypcsv.read_csv(csv_filename)
        del_ids = []
        for doc in csv_rows:
            result = self.search(self.gcase_index, {"query":doc["query"]})
            count = result['hits']['total']['value']
            if count>0 and result['hits']['hits'][0]['_source']["query"] == doc["query"]:
                doc_id = result['hits']['hits'][0]['_id']
                del_ids.append(doc_id)
            else:
                print(f"Query '{doc['query']}' not found in index '{self.gcase_index}'.")
        self.delete_docs(self.gcase_index, del_ids)
        print(f"Delete {len(del_ids)} cases from index '{self.gcase_index}'.")
      
    # 创建初始索引
    def create_index(self):

        es_schema = self.gen_esSchema(analyzer=self.creator.analyzer)
        if self.is_index_exist(self.gcase_index):
            print(f"Index '{self.gcase_index}' already exists.")
        else:
            self.creator.create_index(self.gcase_index, es_schema)

        return es_schema

    def gen_esSchema(self, analyzer=None):

        index_name = self.gcase_index
        table_info = self.loader.get_table_info(index_name)
        columns = table_info.get('columns')
        if not columns:
            raise ValueError(f"Error: no columns in {self.schema_path}")
        
        es_schema = {}
        fields =set()
        for column in columns:
            field_name = column["column_name"]            
            field_type = column.get("type")
            if not field_type:
                field_type = "text"
            es_schema[field_name] = {"type": field_type}
            if field_type == "text" and analyzer != None:
                es_schema[field_name]["analyzer"] = analyzer

            fields.add(field_name)

        if not must_columns.issubset(fields):
            raise ValueError(f"Error: must columns {must_columns} not in {self.schema_path}")
        return es_schema
    
# Example usage
if __name__ == '__main__':
    caseOps = CaseOps()
    cwd = os.getcwd()
    caseOps.delete_cases(os.path.join(cwd,'training\\ikura\\dbInfo\\gcases_d.csv'))