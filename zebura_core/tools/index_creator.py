from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from csv_processor import pcsv
from datetime import datetime
import sys
import os
import re
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from embedding import Embedding
from knowledges.info_loader import SchemaLoader

# 创建索引
class ESIndex:
    def __init__(self):
        
        host = z_config['Eleasticsearch','host']
        port = z_config['Eleasticsearch','port']
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port,'scheme': 'http'}])
        if not self.es.ping():
            raise ValueError("Connection failed")
        else:
            print("Connect Elasticsearch to create index")

        self.model = Embedding()   
        self.info_loader = None #SchemaLoader()
        self.schema = {}

    def is_index_exist(self,index_name):

        if self.es.indices.exists(index=index_name):
            print(f"Index '{index_name}' already exists.")
            return True
        else: 
            return False                              

#     schema = {
#             "category": {
#                 "type": "text",
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                         }
#                     }
#             },
#             "content": {
#                 "type": "text",
#                 "analyzer":"cn_analyzer"
#             },
#             "embedding": {
#                 "type": "dense_vector"
#             }
#     }
    # 设置要创建索引的table名和数据文件
    # table的列信息，index name等默认已经放在info_loader中
    def store_table(self, table_name, data_file):

        table = self.info_loader.get_table(table_name)
        index_name = table["es_index"]
        if self.is_index_exist(index_name):
            return False
        self.schema = {}
        columns = self.info_loader.get_columnList(table_name)
        for column in columns:
            field_name = column["column_en"]
            field_type = column.get("type")
            if not field_type:
                field_type = "text"
            self.schema[field_name] = {"type": field_type}
            if field_type == "text":
                self.schema[field_name]["analyzer"] = "cn_analyzer"
        
        self.create_index(index_name, self.schema)
        self.load_csv_data(index_name, data_file)

        return True

    # 创建索引
    def create_index(self,index_name,schema):
        body = schema
        # 定义索引主分片个数和分析器
        index_mapping = {
                "settings": {
                    "number_of_shards": 2,
                    "number_of_replicas": "1",
                    "analysis": {
                        "tokenizer": {
                            "smartcn_tokenizer": {
                            "type": "smartcn_tokenizer"
                            }
                        },
                        "analyzer": {
                            "cn_analyzer": {
                            "type": "custom",
                            "tokenizer": "smartcn_tokenizer",
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": body
                }
        }
        # 索引的每个field都可以设置不同的analyzer
        try:
            self.es.indices.create(index=index_name, body=index_mapping)
            print(f"Index '{index_name}' created successfully with 5 primary shards.")
        except RequestError as e:
            print(e)
            return False
        
        return True
    
    def load_csv_data(self,index_name, data_filename):

        mypcsv = pcsv()
        csv_rows = mypcsv.read_csv(data_filename)
        if not csv_rows:
            return False
        
        docs=[]
        for doc in csv_rows[3:]:
            result = self.search(self.es, index_name, {"uid": doc['uid']})
            if not result['hits']['hits']:
                self.format_doc(doc)
                docs.append(doc)

        # 计算文本的embedding
        denseSet = []
        for field in self.schema.keys():
            if self.schema[field]['type'] == 'dense_vector':
                denseSet.append(field)
        
        for doc in docs:
            texts = []
            # embedding之前该field存放原始文本
            for field in denseSet:
                texts.append(doc[field])
            embs = self.get_embedding(texts)
            for i, field in enumerate(denseSet):
                doc[field] = embs[i].tolist()
                
        if self.insert_docs(self.es, index_name, docs):
            print(f"Data loaded into index '{index_name}' successfully.")
        else:
            return False
        
    def insert_docs(self,index_name, docs):
        # 一个doc是dict，多个doc是list
        if isinstance(docs, dict):
            self.es.index(index=index_name, body=docs)
        else:
            print(f"Inserting {len(docs)} documents into index '{index_name}'")
            new_docs = list(map(lambda doc:[
                                            {"index": {"_index": index_name}},
                                            doc],
                                docs))
            new_docs = sum(new_docs,[])   
            response = self.es.bulk(index=index_name, body=new_docs)
        
        return response
    
     
    # ['鼠标属于哪个分类','鼠标的价格']
    def get_embedding(self, texts):
        # 获取文本的embedding
        embedding = self.model.encode(texts, normalize_embeddings=True)
        return embedding

    # 格式规范化
    def format_doc(self, doc):

        delkeys = []
        # clear empty fileds
        for key in doc.keys():
            if doc[key] == '':
                delkeys.append(key)
        for key in delkeys:
            del doc[key]
        # data type check, 日期，整数，浮点数
        dateSet= shortSet = NumSet = {}  
        for key in self.schema.keys():
            if self.schema[key]['type'] == 'date':
                dateSet.append(key)
            elif self.schema[key]['type'] == 'short':
                shortSet.append(key)
            elif self.schema[key]['type'] in ['float','integer','long','double','half_float','scaled_float','byte']:
                NumSet.append(key)
        # date format
        for key in dateSet:
            date_str = doc[key]
            date_obj = datetime.strptime(date_str, '%Y/%m/%d')
            doc[key] = date_obj.strftime('%Y-%m-%d')
        
        # digit format  
        for key in doc.keys():
            if key in shortSet:
                doc[key] = re.sub(r'\D', '', doc[key])
                doc[key] = min(int(doc[key]), 32767)
            elif key in NumSet:
                doc[key] = re.sub(r'[^\d.]', '', doc[key])
                doc[key] = float(doc[key])
        
        return
    
    def search(self, index_name, qbody):
        result = self.es.search(index=index_name, body={"query": {"match": qbody}})
        return result
    
# examples usage
if __name__ == '__main__':
    # Load the SQL patterns
    cwd = os.getcwd()
    name = 'datasets\\gcases_schema.json'
    file = os.path.join(cwd, name)
    
    loader = SchemaLoader()
