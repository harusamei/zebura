# ES 各种操作的基类
from elasticsearch import Elasticsearch
import os
import sys
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from tools.embedding import Embedding

class ES_BASE:

    def __init__(self):
        
        host = z_config['Eleasticsearch','host']
        port = int(z_config['Eleasticsearch','port'])

        self.es = Elasticsearch(hosts=[{'host': host, 'port': port,'scheme': 'http'}])
        self.embedding = None
        if not self.es.ping():
            raise ValueError("Connection failed")
        else:
            print("Connect Elasticsearch")
        self.es_version = f"es version: {self.es.info()['version']['number']}"

    @property
    def all_indices(self):
        return self.es.cat.indices(format='json')
    
    def get_fields(self,index):
        mapping= self.es.indices.get_mapping(index=index)
        fields = mapping[index]['mappings']['properties']
        return fields
    
    def search_word(self,index, field, word,size=5):

        if not self.is_exist_field(index, [field]):
            return None
        
        fields = self.get_fields(index=index)
        # vector search
        if fields.get(field).get('type') == 'dense_vector':
            if self.embedding is None:
                self.embedding = Embedding()
            embs = self.embedding.get_embedding(word)
            return self.search_vector(index, field, embs, size)

        # string search
        kw = {field: word}
        query = {
            "size": size,  # Return top3 results
            "query": {
                "match": kw
            }
        }
        # Execute the query
        try:
            return self.es.search(index=index, body=query)
        except Exception as e:
            print(e)
            return None

    def search_vector(self,index, field, embs, size=5):
        query = {
            "knn": {"field": field, "query_vector": embs, "k": 100, "num_candidates": 100, "boost": 1},
            "size": size
        }
        try:
            return self.es.search(index=index, body=query)
        except Exception as e:
            print(e)
            return None
    
    #查询是否存在一组fields
    def is_exist_field(self,index, fieldList):
        fields = self.get_fields(index=index)
        for field in fieldList:
            if not fields.get(field):
                print(f"Field {field} not found in index {index}")
                return False
        return True

    