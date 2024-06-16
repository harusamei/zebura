# 搜索以及结果的基本输出格式
################################
import sys
import os
sys.path.insert(0, os.getcwd())
import settings
import logging

from zebura_core.utils.es_base import ES_BASE
from zebura_core.utils.embedding import Embedding
from zebura_core.constants import D_TOP_K

class ESearcher(ES_BASE):

    def __init__(self):
       super().__init__()
       self.embedding = None
       logging.debug("ESearcher init success")
    
    def try_search(self,index, query):
        try:
            return self.es.search(index=index, body=query)
        except Exception as e:
            logging.error(e)
        return None
    
    def search(self,index, field, word, size=D_TOP_K):

        ty = self.get_field_type(index, field)
        # vector search
        if ty is not None and ty == 'dense_vector':
            if self.embedding is None:
                self.embedding = Embedding()
            embs = self.embedding.get_embedding(word)
            return self.search_vector(index, field, embs, size)

        # string search
        query = {
            "size": size,  # Return top3 results
            "query": {
                "match": {field: word}
            }
        }
        # Execute the query
        return self.try_search(index, query)
        
    def search_term(self,index, field, word, size=D_TOP_K):
        query = {
            "size": size,
            "query": {
                "term": {field: word}
            }
        }
        return self.try_search(index, query)
        
    def search_vector(self,index, field, embs, size=D_TOP_K):

        if not self.is_fields_exist(index, field):
            logging.error(f"Field {field} not found in index {index}")
            return None
        
        query = self.generate_knn_query(field, embs, size)
        return self.try_search(index, query)
        
        
    @staticmethod
    def generate_knn_query(field_name, vec, size):
        
        return {
                "knn": {
                    "field": field_name, 
                    "query_vector": vec, 
                    "k": 100, 
                    "num_candidates": 100, 
                    "boost": 1
                    },
                "size": size
            }
    @staticmethod
    def generate_cosine_query(field_name, vec, size):
        
        return {
                "query": {
                        "script_score": {
                                        "query": { "match_all": {} },
                                        "script": {
                                            "source": f"cosineSimilarity(params.queryVector, '{field_name}') + 1.0",
                                            "params": {
                                                "queryVector": vec
                                            }
                                        }
                        }
                    },
                "size": size
            }

    #  not including some fields,即该字段为空
    def search_ni_fields(self, index_name, ni_fields,size=D_TOP_K):
        must_not = [{"exists": {"field": field}} for field in ni_fields]
        body = {
            "size": size,
            "query": {
                "bool": {
                    "must_not": must_not
                }
            }
        }
        response = self.es.search(index=index_name, body=body)
        return response['hits']['hits']
  
    # 多字段，多值条件下，必须满足或至少满足一个
    # kvList = [{"product_name": "小新"}, {"goods_status": "下架"}]
    # opt = "should"==OR, "must"==AND
    def search_kvs(self,index, kvList, opt="should",size=D_TOP_K):
        if opt != "should":
            opt = "must"
        query = {
            "size": size,
            "query": {
                "bool": {
                    opt: [{"match": kv} for kv in kvList]
                }
            }
        }
        return self.try_search(index, query)
    
    # 同一个word, 在多个字段中搜索，至少一个字段匹配
    def search_multimatch(self,index, fields, word, size=D_TOP_K):
        query = {
            "size": size,
            "query": {
                "multi_match": {
                    "query": word,
                    "fields": fields
                }
            }
        }
        return self.try_search(index, query)
    # 返回满足所有must和至少一个should的文档
    # must表示 and, should表示 or
    def search_and_or(self,index, must_list, should_list):
        query = {
            "query": {
                "bool": {
                    "must": [{"match": fq} for fq in must_list],
                    "should": [{"match": fq} for fq in should_list],
                    "minimum_should_match": 1
                }
            }
        }
        return self.try_search(index, query)

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
        response = self.try_search(index, query)
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
        response = self.try_search(index, query)
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
        return self.try_search(index, query)

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

        response = self.try_search(index, query)
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
        response = self.try_search(index, query)
        return response['aggregations'][m_value]['value']
    
    @staticmethod
    # filter 不需要的字段s， 默认全部保留
    def filter_results(response, filters=[]) -> list:
        #满足查询条件的文档数
        logging.info("Got %d Hits:" % response['hits']['total']['value'])
        if isinstance(filters, str):
            fset=set([filters])
        else:
            fset = set(filters)
        hits = []
        for hit in response['hits']['hits']:
            t_hit = hit["_source"]
            tset =set(t_hit.keys())
            for k in tset&fset:
                t_hit.pop(k,None)
            hits.append(t_hit)
        return hits
     
    @staticmethod
    # 只保留需要的字段
    def keep_results(response, fields):
        logging.info("Got %d Hits:" % response['hits']['total']['value'])
        if isinstance(fields, str):
            fset=set(fields)
        else:
            fset = set(fields)
        hits = []
        for hit in response['hits']['hits']:
            t_hit = hit["_source"]
            tset =set(t_hit.keys())
            for k in tset-fset:
                t_hit.pop(k,None)       
            hits.append(t_hit)
        return hits

    @staticmethod
    def asctable_results(response, separator='\t'): # simple data visualization
        dict_array = [hit['_source'] for hit in response['hits']['hits']]   
        # 获取所有的键（列名）
        headers = dict_array[0].keys()
        # 创建表头行
        header_row = separator.join(headers)
        # 创建数据行
        data_rows = [separator.join(str(d.get(k)) for k in headers) for d in dict_array]
        # 将所有行合并成一个字符串
        table = '\n'.join([header_row] + data_rows) 
        return table
    
    @staticmethod 
    def csv_results(response, out_csv):
        import utils.csv_processor as pcsv
        if response['hits']['total']['value'] == 0:
            logging.info("No matching documents found.")
            return
        csv_rows=[]
        dict1 = {}
        for hit in response['hits']['hits']:
            csv_rows.append(hit['_source'])
            dict1.update(hit['_source'].keys())
        dict1 = {key: None for key in dict1}
        csv_rows.insert(0,dict1)     
        pcsv().write_csv(csv_rows, out_csv)

    @staticmethod
    def docid_results(response):
        docids = []
        for hit in response['hits']['hits']:
            docids.append(hit['_id'])
        return docids
    

# Example usage
if __name__ == '__main__':

    es = ESearcher()
    index="goldencases"
    fields = es.get_all_fields(index)
    print(fields.keys())
   
    fqList = [{"query": "多少钱"}, {"sql": "products"}]
    result = es.search_kvs(index, fqList)
    print(es.filter_results(result,['qembedding']))

    result = es.search(index, "qembedding","请从产品表里查一下联想小新电脑的价格")
    print(es.keep_results(result,['query','sql']))

