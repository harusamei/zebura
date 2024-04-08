import os
import sys
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from elasticsearch import Elasticsearch
from action_executor import constants

class ESQuery:

    def __init__(self):

        self.host = os.environ['ES_HOST']
        self.port = int(os.environ['ES_PORT'])
        self.es = Elasticsearch(hosts=[{'host': self.host, 'port': self.port,'scheme': 'http'}])
        if not self.es.ping():
            raise ValueError("Connection failed")
        else:
            print("Connected to Elasticsearch")

    def get_all_indices(self):
        return self.es.cat.indices(format='json')
    
    #field-query = {product_name: "小新"}   
    def query_word(self,index, field, word):
        kw = {field: word}

        query = {
            "size": 3,  # Return top3 results
            "query": {
                "match": kw
            }
        }
        # Execute the query
        return self.es.search(index=index, body=query)

    #可使用"fields": ["*"], 表示所有字段
    def query_multi_fields(self,index,word,fieldList):
        query = {
            "size": 3,
            "query": {
                "multi_match": {
                    "query": word,
                    "fields": fieldList
                }
            }
        }
        return self.es.search(index=index, body=query)

    def query_either_should_must(self,index, fqList, opt="should"):
        if opt != "should":
            opt = "must"
        
        query = {
            "size": 3,
            "query": {
                "bool": {
                    opt: [{"match": fq} for fq in fqList]
                }
            }
        }
        return self.es.search(index=index, body=query)

    # 返回满足所有must和至少一个should的文档
    def query_both_should_must(self,index, must_list, should_list):
        query = {
            "query": {
                "bool": {
                    "must": [{"match": fq} for fq in must_list],
                    "should": [{"match": fq} for fq in should_list],
                    "minimum_should_match": 1
                }
            }
        }
        return self.es.search(index=index, body=query)

    # 基于field的聚合
    def aggregate(self,index, field):
        query = {
            "size": 0,
            "aggs": {
                "properties": {
                    "terms": {
                        "field": field
                    }
                }
            }
        }
        response = self.es.search(index=index, body=query)
        return response['aggregations']['properties']['buckets']

    #满足某查询条件下的基于field的聚合
    def query_agg(self,index, fqlist, field):
        query = {
            "query": {
                "bool": {
                    "should": [{"match": fq} for fq in fqlist]
                }
            },
            "size": 0,
            "aggs": {
                "properties": { #聚合依据
                        "terms": {
                            "field": field,
                            "size": 10
                        }
                }
            }
        }
        response = self.es.search(index=index, body=query)
        return response['aggregations']['properties']['buckets']
    # 数值统计
    def query_range(self, index, field, upper, lower):
        query = {
            "size": 3,
            "query": {
                "range": {
                    field: {
                        "gte": lower, #大于等于low
                        "lte": upper
                    }
                }
            }
        }
        return self.es.search(index=index, body=query)

    def query_average(self,index, field):
        query = {
            "size": 0,
            "aggs": {
                "average": {
                    "avg": {
                        "field": field
                    }
                }
            }
        }

        response = self.es.search(index=index, body=query)
        avg_value = response['aggregations']['average']['value']
        return avg_value

    #返回最大或最小值
    def query_max_min(self, index, field, most):
        if most == "max":
            m_value="max_value"
        else:
            m_value="min_value"
            most = "min"   
        query = {
            "size": 0,
            "aggs": {
                m_value: {
                    most: {
                        "field": field
                    }
                }
            }
        }

        response = self.es.search(index=index, body=query)
        return response['aggregations'][m_value]['value']
    
    @staticmethod
    def list_result(response):
        #满足查询条件的文档数
        print("Got %d Hits:" % response['hits']['total']['value'])
        for hit in response['hits']['hits']:
            print(hit["_source"]) 
            
    @staticmethod
    def field_result(response, field):
        for hit in response['hits']['hits']:
            print(hit["_source"].get(field))

    #用户的问题是“请问是否有叫小新的产品”， 请根据下面的查询结果回复用户的问题
    @staticmethod
    def generate_answer(query,response):
        print("about %s, Got %d Hits:" % (query,response['hits']['total']['value']))
        print(response)

# Example usage
def main():

    # ES_HOST = "10.110.153.75"
    # ES_PORT = 9200
    # Call the query_es_index function
    es = ESQuery()
    # index="leproducts"
    # result = es.query_word(index, "product_name","小新")
    # es.list_result(result)
    print(es.get_all_indices())

if __name__ == '__main__':
    main()