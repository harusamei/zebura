import sys
import os
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from embedding import Embedding
from knowledges.info_loader import Loader
from tools.es_base import ES_BASE
from csv_processor import pcsv
from datetime import datetime
import re
from elasticsearch.exceptions import RequestError

# 创建索引
class ESIndex(ES_BASE):
     
    def __init__(self):
        super().__init__()
        print(self.es_version)

        base_attrs = [attr for attr in dir(super()) if not attr.startswith('__')]
        base_methods = [method for method in dir(ES_BASE) if not method.startswith('__')]
        print("base attributes",base_attrs)
        print("base methods",base_methods)

        self.info_loader = None
        # 创建index的schema
        self.index_schema = {}
        self.analyzer = "cn_analyzer"

    def is_index_exist(self,index_name):

        if self.es.indices.exists(index=index_name):
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
    def set_schema(self, schema_file):
        self.info_loader = Loader(schema_file)

    def set_embedding(self):
        if not self.embedding:
            self.embedding = Embedding()
    # 设置要创建索引的table名和数据文件
    # table的列信息，index name等默认已经放在info_loader中
    # info_loader 已经加载了schema信息
    def store_table(self, table_name, data_file, size=100):

        if not self.info_loader:
            print("table info not found.")
            return False
        else:
            print("table info loaded.")
        
        table_info = self.info_loader.get_table_info(table_name)
        index_name = table_info["es_index"]
        
        # 从table的columns中抽取index的schema
        self.index_schema = {}
        columns = table_info.get('columns')
        if not columns:
            print(f"Table '{table_name}' not found in schema.")
            return False
        
        for column in columns:
            field_name = column["column_en"]
            field_type = column.get("type")
            if not field_type:
                field_type = "text"
            self.index_schema[field_name] = {"type": field_type}
            if field_type == "text":
                self.index_schema[field_name]["analyzer"] = self.analyzer
        
        # 创建索引
        if self.is_index_exist(index_name):
            print(f"Index '{index_name}' already exists.")
        else:
            self.create_index(index_name, self.index_schema)
        # 加载数据
        return self.load_csv_data(index_name, data_file,size)


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
                            "type": "smartcn_tokenizer"     # 使用ES内置中文分词器
                            }
                        },
                        "analyzer": {
                            self.analyzer: {
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
    
    # size=负数,None，表示全部加载
    def load_csv_data(self,index_name, data_filename, size=-1):

        mypcsv = pcsv()
        csv_rows = mypcsv.read_csv(data_filename,size)
        if not csv_rows:
            return False
        
        docs=[]
        for doc in csv_rows:
            # 已经在Index里面的数据不需要再次插入
            if doc.get('uid'):
                continue
            self.format_doc(doc)
            docs.append(doc)

        # 计算文本的embedding
        denseSet = []
        for field in self.index_schema.keys():
            if self.index_schema[field]['type'] == 'dense_vector':
                denseSet.append(field)

        # 如果有dense_vector字段，需要计算embedding
        if len(denseSet) >0:
            self.set_embedding()

        for doc in docs:
            texts = []
            # embedding之前该field存放原始文本
            for field in denseSet:
                texts.append(doc[field])
            embs = self.embedding.get_embedding(texts)
            for i, field in enumerate(denseSet):
                doc[field] = embs[i].tolist()
                
        if self.insert_docs(index_name, docs):
            print(f"Data loaded into index '{index_name}' successfully.")
            return True
        else:
            print(f"Failed to load data into index '{index_name}'.")
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
        dateSet=[]
        shortSet=[]
        NumSet=[]
        for key in self.index_schema.keys():
            if self.index_schema[key]['type'] == 'date':
                dateSet.append(key)
            elif self.index_schema[key]['type'] == 'short':
                shortSet.append(key)
            elif self.index_schema[key]['type'] in ['float','integer','long','double','half_float','scaled_float','byte']:
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

    cwd = os.getcwd()
    name = 'datasets\\gcases_schema.json'
    sch_file = os.path.join(cwd, name)
    
    escreator = ESIndex()
    # escreator.set_schema(sch_file)
    # 'gcases'是table名，'datasets\\goodcases.csv'是数据文件， index name是'goldencases'
    # escreator.store_table('gcases', 'datasets\\goodcases.csv')
    results = escreator.search('goldencases', {'query': '鼠标'})
    print(results)

