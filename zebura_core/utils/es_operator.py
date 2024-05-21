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
from constants import D_TOP_K as d_size

class ESOps(ES_BASE):

    def __init__(self):
        
        super().__init__()
        self.se = ESearcher()
        logging.info('ESops is initial success')

    # docs是一个list，每个元素是一个dict
    def insert_docs(self, index_name, docs):
        try:
            new_docs = list(map(lambda doc:[
                                            {"index": {"_index": index_name}},
                                            doc],
                                docs))
            new_docs = sum(new_docs,[])
            # 在索引中添加文档, refresh=True 使得文档立即可见
            response = self.es.bulk(index=index_name, body=new_docs,refresh='true') 
        except Exception as e:
            logging.error(f"Error inserting documents: {e}")
        else:
            if response['errors']:
                logging.error(f"can not insert docs in {index_name}")
            else:
                logging.info(f'insert {len(new_docs)} in {index_name}')
        
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
            csv_rows.append(hit)
            count += 1
            if count > max_size:
                break
        pcsv().write_csv(csv_rows, out_csv)
        logging.info(f"index: {index_name}, total: {count}\n")
      
    # 查找相同doc, comp_fields字段的值相等则认为是同一文档: 
    # 输出 doc
    def exist_doc(self, index, doc, comp_fields) -> dict:
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
        
    # remove multiple docs
    def delete_docs(self, index_name, doc_ids):
        for doc_id in doc_ids:
            self.delete_doc(index_name, doc_id)
    
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
   
   