# ES 各种操作的基类
# 1. 连接ES；2. 判断index是否存在；3. 判断字段是否存在；4. 获取所有字段；5. 获取所有index
###################################
import os
import sys
sys.path.insert(0, os.getcwd())
from settings import z_config
import logging

from elasticsearch import Elasticsearch

class ES_BASE:

    def __init__(self):
        
        host = z_config['Eleasticsearch','host']
        port = int(z_config['Eleasticsearch','port'])

        self.es = Elasticsearch(hosts=[{'host': host, 'port': port,'scheme': 'http'}])
        if not self.es.ping():
            raise ValueError("Connection failed")
        
        self.es_version = f"es version: {self.es.info()['version']['number']}"   
        logging.debug("ES_BASE init success")
        logging.info(self.es_version)

    @property
    def get_all_indices(self):
        return self.es.cat.indices(format='json')
       
    def get_all_fields(self,index):
        mapping= self.es.indices.get_mapping(index=index)
        fields = mapping[index]['mappings']['properties']
        return fields
    
    def get_field_type(self, index_name, field_name):
        
        properties = self.get_all_fields(index_name)
        if field_name in properties:
            return properties[field_name]['type']
        else:
            return None

    def get_doc_count(self, index_name):
        return self.es.count(index=index_name)['count']
 
    #是否存在index
    def is_index_exist(self,index_name):

        if self.es.indices.exists(index=index_name):
            return True
        else: 
            return False
     
    #是否缺失需要的字段
    def is_fields_exist(self,index, must_fields):
        if isinstance(must_fields, str):
            must_fields = [must_fields]
            
        fields = self.get_all_fields(index=index)
        tSet = set(fields.keys())
        mSet = set(must_fields)
        missing = mSet - tSet
        if missing:
            print(f"Fields {missing} not found in index {index}")
            return False
        else:
            return True
        

# Example usage
if __name__ == '__main__':

    es = ES_BASE()
    index_name ="leproducts"
    es.is_fields_exist(index_name, ['brand','price','desc_uk'])