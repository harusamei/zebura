#from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from csv_processor import pcsv
import os
import sys
sys.path.insert(0, os.getcwd())
import settings
from utils.es_base import ES_BASE
from constants import D_TOP_K as d_size
class ESOps(ES_BASE):

    def __init__(self):
        
        super().__init__()
        print(self.es_version)

    # docs是一个list，每个元素是一个dict
    def insert_docs(self, index_name, docs):
        if not docs or len(docs) == 0:
            print("No documents to insert.")
            return 
             
        print(f"Inserting {len(docs)} documents into index '{index_name}'")
        new_docs = list(map(lambda doc:[
                                        {"index": {"_index": index_name}},
                                        doc],
                            docs))
        new_docs = sum(new_docs,[])   
        response = self.es.bulk(index=index_name, body=new_docs) # 在索引中添加文档
        if response['errors']:
            print(f"Error inserting documents: {response}")
        # 刷新索引，使文档立即可用
        self.es.indices.refresh(index=index_name)

    # 从es中检索大量结果，并写入文件
    def write_scan(self, index_name, out_csv):
        import itertools

        scroller = scan(self.es, index=index_name, query={"query": {"match_all": {}}})
        scroller, scroller_copy = itertools.tee(scroller)

        keyset = set()
        for res in scroller:
            hit = res['_source']
            keyset.update(hit.keys())
        keyset = {key for key in keyset if not key.startswith('_') and not key == 'qembedding'}
        keyset = list(keyset)
        keyset.sort()
        print(f"keys: {keyset}")

        csv_rows = []
        count = 0
        for res in scroller_copy:
            hit = res['_source']
            dict ={key: hit.get(key) for key in keyset}
            csv_rows.append(dict)
            count += 1
        pcsv().write_csv(csv_rows, out_csv)

        print(f"index: {index_name}, total: {count}\n")
    

    def delete_doc(self, index_name, doc_id):    
        exists = self.es.exists(index=index_name, id=doc_id)
        if exists:
            self.es.delete(index=index_name, id=doc_id)
        self.es.indices.refresh(index=index_name)
        
    # remove multiple docs
    def delete_docs(self, index_name, doc_ids):
        for doc_id in doc_ids:
            self.delete_doc(index_name, doc_id)
        self.es.indices.refresh(index=index_name)
    
    def update_doc_field(self, index_name, doc_id, field, new_value):
        body = {
            "doc": {
                field: new_value
            }
        }
        res = self.es.update(index=index_name, id=doc_id, body=body)
        self.es.indices.refresh(index=index_name)
        return res


    def search(self, index_name, qbody, size=d_size):
        result = self.es.search(index=index_name, body={"query": {"match": qbody}},size=size)
        return result
    
    # 完全匹配查询，不对输入进行分析
    def term_search(self, index_name, qbody,max_size=d_size):
        result = self.es.search(index=index_name, body={"query": {"term": qbody}},size=max_size)
        return result
    
    # regexp_query = "小新.*本"  # 匹配小新开头的各种本
    # {
    #     "field_name": {
    #         "value": "regexp_query"
    #     }   
    # }
    def search_with_regexp(self, index_name, qbody):
        query ={
                    "query": {
                        "regexp": qbody
                        }
                }
         
        result = self.es.search(index=index_name, body=query)
        return result

    def write_search_result(result, csv_file):
        if result['hits']['total']['value'] == 0:
            print("No matching documents found.")
            return
        csv_rows=[]
        dict1 = {}
        for hit in result['hits']['hits']:
            csv_rows.append(hit['_source'])
            dict1.update(hit['_source'])
        dict1 = {key: None for key in dict1}
        csv_rows.insert(0,dict1)     
        pcsv().write_csv(csv_rows, csv_file)

    def get_docids(result):
        docids = []
        for hit in result['hits']['hits']:
            docids.append(hit['_id'])
        return docids

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

    esoper.write_scan(index_name, "gcases.csv")
   
   