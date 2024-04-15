from elasticsearch import Elasticsearch
import sys
import os
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from tools.embedding import Embedding

class ESTester:
    
    def __init__(self):
        
        host = z_config['Eleasticsearch','host']
        port = int(z_config['Eleasticsearch','port'])
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port,'scheme': 'http'}])
        if not self.es.ping():
            raise ValueError("Connection failed")
        else:
            print("Connect Elasticsearch")

        
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

    def test_dense_search(self, index_name, field_name, embedding):
        result = self.es.search(index=index_name, body={"query": {"dense_vector": {field_name: embedding}}})
        print(result)
 
# examples usage
if __name__ == '__main__':
    tester = ESTester()
    tester.get_indices()
    tester.test_search('goldencases')
