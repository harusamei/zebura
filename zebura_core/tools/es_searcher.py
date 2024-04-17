import os
import sys
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from tools.es_base import ES_BASE

class ESearcher(ES_BASE):

    def __init__(self):
       super().__init__()
       print(self.es_version)
       base_attrs = [attr for attr in dir(super()) if not attr.startswith('__')]
       base_methods = [method for method in dir(ES_BASE) if not method.startswith('__')]
       print("base attributes",base_attrs)
       print("base methods",base_methods)
       
    # 使用"fields": ["*"], 表示所有字段
    def search_fields(self,index,word,fieldList):
        if not self.is_exist_field(index, fieldList):
            return None
            
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

    #fqList = [{"product_name": "小新"}, {"goods_status": "下架"}]
    def search_either_should_must(self,index, fqList, opt="should"):
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
    def search_both_should_must(self,index, must_list, should_list):
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
    def search_agg(self,index, fqlist, field):
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
    def search_range(self, index, field, upper, lower):
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

    def search_average(self,index, field):
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
    def search_max_min(self, index, field, most):
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
    def list_results(response):
        #满足查询条件的文档数
        print("Got %d Hits:" % response['hits']['total']['value'])
        for hit in response['hits']['hits']:
            print(hit["_source"]) 
            
    @staticmethod
    def filter_results(response, field):
        for hit in response['hits']['hits']:
            print(hit["_source"].get(field))

    @staticmethod
    def asctable_results(response, separator='\t'):
        dict_array = [hit['_source'] for hit in response['hits']['hits']]   
        # 获取所有的键（列名）
        headers = dict_array[0].keys()
        # 创建表头行
        header_row = separator.join(headers)
        # 创建数据行
        data_rows = [separator.join(str(d.get(k)) for k in headers) for d in dict_array]
        # 将所有行合并成一个字符串
        table = '\n'.join([header_row] + data_rows)
        print(table)
    
    #用户的问题是“请问是否有叫小新的产品”， 请根据下面的查询结果回复用户的问题
    @staticmethod
    def generate_answer(query,response):
        print("about %s, Got %d Hits:" % (query,response['hits']['total']['value']))
        print(response)

    

# Example usage
if __name__ == '__main__':

    es = ESearcher()
    index="goldencases"
    fields = es.get_fields(index)
    print(fields.keys())
    result = es.search_word(index, "query","小新")
    es.filter_results(result,'query')
    
    fqList = [{"product_name": "小新"}, {"goods_status": "下架"}]
    result = es.search_either_should_must(index, fqList)
    if not result:
        es.asctable_results(result)

    result = es.search_word(index, "qembedding","有哪些新上市的产品")
    es.filter_results(result,'query')

