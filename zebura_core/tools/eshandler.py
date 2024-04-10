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
        
        self.host = os.environ['ES_HOST']
        self.port = int(os.environ['ES_PORT'])
        self.es = Elasticsearch(hosts=[{'host': self.host, 'port': self.port,'scheme': 'http'}])
        if not self.es.ping():
            raise ValueError("Connection failed")
        else:
            print("Connect Elasticsearch to create index")

        self.model = Embedding()

    def is_index_exist(self,index_name):

        if self.es.indices.exists(index=index_name):
            print(f"Index '{index_name}' already exists.")
            return True
        else: 
            return False                              

    def create_index(self,index_name, body):
        body = {
                "category": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                            }
                        }
                },
                "content": {
                    "type": "text",
                    "analyzer":"cn_analyzer"
                },
                "embedding": {
                    "type": "dense_vector"
                }
        }

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
    
    # ['鼠标属于哪个分类','鼠标的价格']
    def get_embedding(self, texts):
        # 获取文本的embedding
        embedding = self.model.encode(texts, normalize_embeddings=True)
        return embedding
    
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
            print(len(new_docs))
            response = self.es.bulk(index=index_name, body=new_docs)
        
        return response
    
    # 格式规范化
    def format_doc(doc):

        delkeys = []
        # empty value
        for key in doc.keys():
            if doc[key] == '':
            delkeys.append(key)
        for key in delkeys:
            del doc[key]
        # date format  
        date_str = doc['time_to_market']
        date_obj = datetime.strptime(date_str, '%Y/%m/%d')
        doc['time_to_market'] = date_obj.strftime('%Y-%m-%d')
        # digits only
        shortSet = {'price', 'memory_capacity', 'cpu_core_number',
                    'stock_number','max_memory_capacity','memory_slot_number','disk_capacity'}
        floatSet = {'height','width','depth'}
        for key in doc.keys():
            if key in shortSet:
                doc[key] = re.sub(r'\D', '', doc[key])
                doc[key] = min(int(doc[key]), 32767)
            elif key in floatSet:
                doc[key] = re.sub(r'[^\d.]', '', doc[key])
                doc[key] = float(doc[key])
        
        return
    

    def load_csv(es,index_name, data_filename):

        mypcsv = pcsv()
        csv_rows = mypcsv.read_csv(data_filename)
        if not csv_rows:
            return False
        
        docs=[]
        for doc in csv_rows[3:]:
            result = search(es, index_name, {"uid": doc['uid']})
            if not result['hits']['hits']:
                format_doc(doc)
                docs.append(doc)

        if insert_docs(es, index_name, docs):
            print(f"Data loaded into index '{index_name}' successfully.")
        else:
            return False
    

class ESTester:
    
    def get_indices(es):
        # 获取集群中的所有有别名的索引
        aliases = es.cat.aliases(format="json")
        # 过滤系统自动生成的index
        user_aliases = [alias for alias in aliases if not alias['alias'].startswith('.')]
        user_aliases = list( map(lambda alias: f"{alias['alias']}->{alias['index']}", user_aliases))
        print(f"alias -> index\n {user_aliases}")
    
        # 获取集群中的所有索引名称
        all_indices = es.cat.indices(h="index",format="json")
        print(all_indices)

    def test_index(es, index_name):
        if es.indices.exists(index=index_name):
            print(es.cat.indices(index=index_name, v=True))
        else:
            print(f"Index '{index_name}' does not exist.")

    def get_analyzers(es,index_name):
        # 获取映射到index的analyzer，不包括内置分析器
        settings = es.indices.get_settings(index=index_name).get(index_name, {})
        analysis = settings.get('settings', {}).get('index', {}).get('analysis', {})
        if analysis:
            print(f'Index: {index_name}')
            for analyzer_type, analyzers in analysis.items():
                for analyzer_name, analyzer_settings in analyzers.items():
                    print(f'  {analyzer_type}: {analyzer_name} - {analyzer_settings}')
        else:
            print(f'No custom analyzers found for index {index_name}')

    def test_analyzer(es, index_name):
        
        query = "联想智能插座多少钱一只？"  
        analysis_result = es.indices.analyze(index=index_name, body={"analyzer": "cn_html_analyzer", "text": query})

        # 提取分析结果中的分词列表
        tokens = [token_info["token"] for token_info in analysis_result["tokens"]]
        print("analyzing results", tokens)

    def test_field_analyzer(es, index_name, field_name,text):
            result = es.indices.analyze(index=index_name, body={"field": field_name, "text": text})
            tokens = [token_info["token"] for token_info in result["tokens"]]
            print("analyzing results", tokens)


        

def insert_docs(es, index_name, docs):
    # 一个doc是dict，多个doc是list
    if isinstance(docs, dict):
        es.index(index=index_name, body=docs)
    else:
        print(f"Inserting {len(docs)} documents into index '{index_name}'")
        new_docs = list(map(lambda doc:[
                                        {"index": {"_index": index_name}},
                                        doc],
                            docs))
        new_docs = sum(new_docs,[])   
        print(len(new_docs))
        response = es.bulk(index=index_name, body=new_docs) # 在索引中添加文档
        if response['errors']:
            print(f"Error inserting documents: {response}")
    # 刷新索引，使文档立即可用
    es.indices.refresh(index=index_name)

def scan_all(es, index_name, output_filename):
    scroller = scan(es, index=index_name, query={"query": {"match_all": {}}})
    with open(output_filename, "w") as f:
        f.write(f"index: {index_name}, total: {get_count(es,index_name)}\n")
        for res in scroller:
            f.write(str(res))
            f.write("\n")

def test_search(es, index_name):
    # 查询是否有doc
    result = es.search(index=index_name, body={"query": {"match_all": {}}})
    print(result)

def delete_doc(es, index_name, doc_id):
    
    exists = es.exists(index=index_name, id=doc_id)
    if exists:
        es.delete(index=index_name, id=doc_id)
    es.indices.refresh(index=index_name)

def update_doc_field(es, index_name, doc_id, field, new_value):
    body = {
        "doc": {
            field: new_value
        }
    }
    res = es.update(index=index_name, id=doc_id, body=body)
    es.indices.refresh(index=index_name)
    return res

def get_count(es, index_name):
    count = es.count(index=index_name)['count']
    return count

def search(es, index_name, qbody):
    result = es.search(index=index_name, body={"query": {"match": qbody}})
    return result

def search_with_regexp(es, index_name, qbody):
    result = es.search(index=index_name, body={"query": {"regexp": qbody}})
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
    
