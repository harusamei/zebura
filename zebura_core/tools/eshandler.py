from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from elasticsearch.helpers import scan
from csv_processor import pcsv
from datetime import datetime
import sys
import os
import re
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from action_executor import constants
from embedding import Embedding

# 创建带有dense_vector类型的索引
class ESIndex:
    def __init__(self):
        
        host = os.environ['ES_HOST']
        port = int(os.environ['ES_PORT'])
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port,'scheme': 'http'}])
        if not self.es.ping():
            raise ValueError("Connection failed")
        else:
            print("Connect Elasticsearch to create index")

        self.model = Embedding()
        self.schema = {}        # 索引的schema

    def is_index_exist(self,index_name):

        if self.es.indices.exists(index=index_name):
            print(f"Index '{index_name}' already exists.")
            return True
        else: 
            return False                              

#
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

    def set_schema(self, csv_filename):
        pass

    def create_index(self,index_name):

        # 定义索引主分片个数和分析器
        body = self.schema

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
        dateSet= shortSet = floatSet = {}  
        for key in self.schema.keys():
            if self.schema[key]['type'] == 'date':
                dateSet.append(key)
            elif self.schema[key]['type'] == 'integer':
                shortSet.append(key)
            elif self.schema[key]['type'] == 'float':
                floatSet.append(key)
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
            elif key in floatSet:
                doc[key] = re.sub(r'[^\d.]', '', doc[key])
                doc[key] = float(doc[key])
        
        return
    
    def search(self, index_name, qbody):
        result = self.es.search(index=index_name, body={"query": {"match": qbody}})
        return result
    
    def load_csv(self,index_name, data_filename):

        mypcsv = pcsv()
        csv_rows = mypcsv.read_csv(data_filename)
        if not csv_rows:
            return False
        
        docs=[]
        for doc in csv_rows[3:]:
            result = self.search(es, index_name, {"uid": doc['uid']})
            if not result['hits']['hits']:
                self.format_doc(doc)
                docs.append(doc)

        if self.insert_docs(es, index_name, docs):
            print(f"Data loaded into index '{index_name}' successfully.")
        else:
            return False
    

class ESTester:
    
    def __init__(self):
        
        host = os.environ['ES_HOST']
        port = int(os.environ['ES_PORT'])
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port,'scheme': 'http'}])
        if not self.es.ping():
            raise ValueError("Connection failed")
        else:
            print("Connect Elasticsearch to create index")
        
    def get_indices(self):
        # 获取集群中的所有有别名的索引
        aliases = self.es.cat.aliases(format="json")
        # 过滤系统自动生成的index
        user_aliases = [alias for alias in aliases if not alias['alias'].startswith('.')]
        user_aliases = list( map(lambda alias: f"{alias['alias']}->{alias['index']}", user_aliases))
        print(f"alias -> index\n {user_aliases}")
    
        # 获取集群中的所有索引名称
        all_indices = self.es.cat.indices(h="index",format="json")
        print(all_indices)

    def test_index(self, index_name):
        if self.es.indices.exists(index=index_name):
            print(self.es.cat.indices(index=index_name, v=True))
        else:
            print(f"Index '{index_name}' does not exist.")

    def get_count(self, index_name):
        count = self.es.count(index=index_name)['count']
        return count

    def get_analyzers(self,index_name):
        # 获取映射到index的analyzer，不包括内置分析器
        settings = self.es.indices.get_settings(index=index_name).get(index_name, {})
        analysis = settings.get('settings', {}).get('index', {}).get('analysis', {})
        if analysis:
            print(f'Index: {index_name}')
            for analyzer_type, analyzers in analysis.items():
                for analyzer_name, analyzer_settings in analyzers.items():
                    print(f'  {analyzer_type}: {analyzer_name} - {analyzer_settings}')
        else:
            print(f'No custom analyzers found for index {index_name}')

    def test_analyzer(self, index_name):
        
        query = "联想智能插座多少钱一只？"  
        analysis_result = self.es.indices.analyze(index=index_name, body={"analyzer": "cn_html_analyzer", "text": query})

        # 提取分析结果中的分词列表
        tokens = [token_info["token"] for token_info in analysis_result["tokens"]]
        print("analyzing results", tokens)

    def test_field_analyzer(self, index_name, field_name,text):
            result = self.es.indices.analyze(index=index_name, body={"field": field_name, "text": text})
            tokens = [token_info["token"] for token_info in result["tokens"]]
            print("analyzing results", tokens)


        
class ESOps:

    def __init__(self):
        
        host = os.environ['ES_HOST']
        port = int(os.environ['ES_PORT'])
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port,'scheme': 'http'}])
        if not self.es.ping():
            raise ValueError("Connection failed")
        else:
            print("Connect Elasticsearch to create index")
    
    def insert_docs(self, index_name, docs):
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
            print(len(new_docs))
            response = self.es.bulk(index=index_name, body=new_docs) # 在索引中添加文档
            if response['errors']:
                print(f"Error inserting documents: {response}")
        # 刷新索引，使文档立即可用
        self.es.indices.refresh(index=index_name)

    def scan_all(self, index_name, output_filename):
        scroller = scan(self.es, index=index_name, query={"query": {"match_all": {}}})
        with open(output_filename, "w") as f:
            f.write(f"index: {index_name}, total: {get_count(self.es,index_name)}\n")
            for res in scroller:
                f.write(str(res))
                f.write("\n")

    def test_search(self, index_name):
        # 查询是否有doc
        result = self.es.search(index=index_name, body={"query": {"match_all": {}}})
        print(result)

    def delete_doc(self, index_name, doc_id):
        
        exists = self.es.exists(index=index_name, id=doc_id)
        if exists:
            self.es.delete(index=index_name, id=doc_id)
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


    def search(self, index_name, qbody):
        result = self.es.search(index=index_name, body={"query": {"match": qbody}})
        return result

    def search_with_regexp(self, index_name, qbody):
        result = self.es.search(index=index_name, body={"query": {"regexp": qbody}})
        return result

    def write_search_result(result, filename):
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
        pcsv().write_csv(csv_rows, filename)

    def get_docids(result):
        docids = []
        for hit in result['hits']['hits']:
            docids.append(hit['_id'])
        return docids

# example usage
if __name__ == '__main__':
    # 连接到本地的 Elasticsearch 实例
    es = Elasticsearch(['http://10.110.153.75:9200'],
                       basic_auth=('elastic', 'a3ghnRyzop2O1B2yOnqT'))
   
    if not es.ping():
        raise ValueError("Connection failed")
    else:
        print("Connected to Elasticsearch")

   
    index_name = "goldencases"
    data_filename = "data\pSch.csv"
    
    load_data(es, index_name, data_filename)
    
