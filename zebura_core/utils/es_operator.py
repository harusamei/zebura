# Index 的增删改
#########################################
import os
import sys
sys.path.insert(0, os.getcwd())
import settings
from elasticsearch.helpers import scan
import logging
from utils.csv_processor import pcsv

from utils.es_base import ES_BASE
from utils.es_searcher import ESearcher
from utils.es_creator import ESIndex
from constants import D_MAX_BATCH_SIZE as batch_size

class ESOps(ES_BASE):

    def __init__(self):
        
        super().__init__()
        self.se = ESearcher()
        logging.info('ESops is initial success')

    # 设置要创建索引的table名和数据文件
    # table的列信息，index name等已经放在schema中
    def store_table(self, csv_file, table_name, sch_file, batch_size=batch_size):
        
        creator = ESIndex()
        index_name,es_mapping = creator.json2mapping(sch_file, table_name) 
        # 创建索引
        if self.is_index_exist(index_name):
            print(f"Index '{index_name}' already exists.")
        else:
            creator.create_index(index_name, es_mapping)
        
        docs = pcsv().read_csv(csv_file)
        comp_fields = []
        for field in es_mapping.keys():
            if es_mapping[field]['type'] == 'keyword':
                comp_fields.append(field)
        
        for i in range(0, len(docs), batch_size):
            count = self.insert_new_docs(index_name, docs[i:i+batch_size],comp_fields)
            logging.info(f"inserted {count} docs,from {i} to {i+batch_size}")  

    def drop_duplicate_docs(self, docs, comp_fields):
        
        new_docs = []
        seen = set()
        for doc in docs:
            # 创建一个元组，包含我们想要比较的字段
            comp_tuple = tuple(doc.get(field) for field in comp_fields)
            if comp_tuple not in seen:
                new_docs.append(doc)
                seen.add(comp_tuple)

        return new_docs
    
    def insert_new_docs(self, index_name, docs, comp_fields)->int:

        docs = self.drop_duplicate_docs(docs, comp_fields)
        new_docs = []
        for doc in docs:
            if not self.is_doc_exist(index_name, doc, comp_fields):
                new_docs.append(doc)
        
        return self.insert_docs(index_name, new_docs)
        
    # 输出index中样本的field 列表项
    def write_scan(self, index_name, out_csv, fields=[], max_size=-1):
        if max_size < 0:
            max_size = self.get_doc_count(index_name)
        
        all_fields = self.get_all_fields(index_name)
        if len(fields) == 0:
            fields = list(all_fields.keys())
        else:
            fields = list(set(fields) & set(all_fields.keys()))

        scroller = scan(self.es, index=index_name, query={"query": {"match_all": {}}})
        dict = { field:None for field in fields}
        csv_rows = [dict]
        count = 0
        for res in scroller:
            hit = res['_source']
            dict ={field:hit.get(field) for field in fields}           
            csv_rows.append(dict)
            count += 1
            if count > max_size:
                break
        pcsv().write_csv(csv_rows, out_csv)
        logging.info(f"index: {index_name}, total: {count}\n")

    def is_doc_exist(self, index_name, doc, comp_fields):
        docs = self.exist_docs(index_name, doc, comp_fields)
        if docs is None:
            return False
        if len(docs) > 0:
            return True
        else:
            return False
    # 查找相同doc, comp_fields字段的值相等则认为是同一文档: 
    # 输出 doc
    def exist_docs(self, index, doc, comp_fields) -> dict:
        # exists = self.es.exists(index=index_name, id=doc_id, filter_path=['_source'])
        all_fields = self.get_all_fields(index)
        comp_fields = set(comp_fields) & set(all_fields.keys()) & set(doc.keys())
        if len(comp_fields) == 0:
            logging.error(f"no valid fields in {comp_fields}")
            return None
        
        terms= []
        matchs = []
        for field in comp_fields:
            t =self.get_field_type(index, field)
            if t == 'keyword':
                terms.append(field)
            else:
                matchs.append(field)
        
        term_body = {}
        for field in terms:
            term_body[field] = doc[field]
        match_body = {}
        for field in matchs:
            match_body[field] = doc[field]
        
        conds =[]
        if len(matchs) >0 :
            conds = [{"match": match_body}]
        if len(terms) >0:
            conds.append({"term": term_body})
        
        body = {
            "query": {
                "bool": {
                    "must": conds
                }
            }
        }
        response = self.es.search(index=index_name, body=body)
        return response['hits']['hits']

    def delete_doc(self, index_name, doc_id):    
        response = self.es.delete(index=index_name, id=doc_id,refresh=True)
        if response['result'] == 'deleted':
            logging.info(f"delete doc {doc_id} in {index_name}")
        else:
            logging.error(f"can not delete doc {doc_id} in {index_name}")
            
    def update_doc_field(self, index_name, doc_id, field, new_value):
        body = {
            "doc": {
                field: new_value
            }
        }
        res = self.es.update(index=index_name, id=doc_id, body=body,refresh=True)
        return res
    
    
# example usage
if __name__ == '__main__':

    esoper = ESOps()
    index_name = "goldencases"
    # query = {
    #             "product_name": {
    #                 "value": ".*小",
    #                 "flags" : "ALL"
    #             }   
    #         }
    # result = esoper.search_with_regexp(index_name, query)
    # query = {
    #             "product_name": "小新"
    #         }
    # result2 = esoper.search(index_name, query)
    # print(result)
    # print(result2)

    esoper.write_scan(index_name, "gcases.csv",['query','sql','action'])
   
   