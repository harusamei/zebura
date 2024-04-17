from elasticsearch import Elasticsearch
import sys
import os
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from tools.es_base import ES_BASE
from tools.embedding import Embedding

# base methods ['all_indices', 'get_fields', 'is_exist_field', 'search_vector', 'search_word']
class ESTester(ES_BASE):
    
    def __init__(self):
        super().__init__()
        print(self.es_version)

        
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

    def test_search(self, index_name):
        # 查询是否有doc
        result = self.es.search(index=index_name, body={"query": {"match_all": {}}})
        print(result)

    def search_embedding(self,index_name, field_name, emb):
        # 默认cosine距离
        query = {
                    "query": {
                        "script_score": {
                            "query": {
                                "match_all": {}
                            },
                            "script": {
                                "source": f"cosineSimilarity(params.queryVector, '{field_name}') + 1.0",
                                "params": {
                                    "queryVector": emb
                                }
                            }
                        }
                    },
                    "size": 10
                }
        print(f'{len(emb)}: {emb[0:5]}')
        result = self.es.search(index=index_name, body=query)
        return result
    

    def test_field_search(self, index_name, field_name,query):
        fields = self.get_fields(index_name)
        
        if not fields.get(field_name):
            print(f"Field {field_name} not found in index {index_name}")
            return False
        
        if fields.get(field_name).get('type') == 'dense_vector':
            if not self.embedding:
                self.embedding = Embedding()
                print("Embedding model loaded.")
            embedding = self.embedding.get_embedding(query)
            result = self.search_embedding(index_name, field_name, embedding)
        else:
            result = self.es.search(index=index_name, body={"query": {"match": {field_name: query}}})
        total = result['hits']['total']['value']
        print(f'total hits: {total}')
        return result
    
    def output_result(self, index_name, result, size=3):

        fields = self.get_fields(index_name)
        dv_fields = []
        for field_name in fields.keys():
            if fields[field_name].get('type') == 'dense_vector':
                dv_fields.append(field_name)

        for hit in result['hits']['hits'][:size]:
            one_hit = hit['_source']
            for key in one_hit:
                if key in dv_fields:
                    embs = one_hit[key]
                    print(f'{key}: dims {len(embs)}-> {embs[0:5]} ...')
                else:
                   print(f"{key}: {one_hit[key]}")

 
# examples usage
if __name__ == '__main__':

    tester = ESTester()
    tester.get_indices()
    fields = tester.get_fields('goldencases')
    print(fields)

    result = tester.test_field_search('goldencases', 'query', '列出所有电子产品分类下的产品')   
    tester.output_result('goldencases',result,2)
    
    embs = result['hits']['hits'][0]['_source']['qembedding']
    result = tester.search_embedding('goldencases', 'qembedding', embs)
    tester.output_result('goldencases',result,2)
