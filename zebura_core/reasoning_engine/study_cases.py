# case study 模式，找相似的案例
import os
import sys
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
import settings
from tools.es_searcher import ESearcher
from knowledges.schema_loader import Loader
import constants

class CaseStudy:
        
        def __init__(self):
            # good cases 存放在ES中，通过ES查询
            self.es = ESearcher()
            # load schema of goodcases db
            cwd = os.getcwd()
            name = os.environ.get('CASE_STUDY_SCHEMA')  # 'datasets\gcases_schema.json'
            self.loader = Loader(os.path.join(cwd, name))

            self.gcase_index = self.loader.get_index_nameList()[0]  # 'gcases'
            table = self.loader.get_table_nameList()[0]
            self.columns = self.loader.get_all_columns(table)
            # check necessary columns
            necessary_columns = ['query', 'qembedding', 'sql', 'action']
            for col in necessary_columns:
                if col not in [c['column_en'] for c in self.columns]:
                    raise ValueError(f"Column {col} not found in table {table}")
            
        
        def find_similar_query(self, query, topk=5):
            index = self.gcase_index
            results = self.es.search_word(index, "query", query, topk)
            return results
        
        def find_similar_sql(self, sql, topk=5):
            index = self.gcase_index
            results = self.es.search_word(index, "sql", sql, topk)
            return results
        
        def find_similar_vector(self, query, topk=5):
            index = self.gcase_index
            results = self.es.search_word(index, "qembedding", query, topk)
            return results
        
        def assemble_find(self, query, sql, topk=5):
            resp1 = self.find_similar_query(query, topk)
            resp2 = self.find_similar_sql(sql, topk)
            resp3 = self.find_similar_vector(query, topk)
            
            cands = {}
            all_resp = resp1['hits']['hits']+resp2['hits']['hits']+resp3['hits']['hits']
            for resp in all_resp:
                if not cands.get(resp['_id']):
                    cands[resp['_id']] = resp['_score']
                else:
                    cands[resp['_id']] += resp['_score']
            print(cands)
            sorted_ids = sorted(cands, key=lambda x: cands[x],reverse=True)
            results = {}
            for i, id in enumerate(sorted_ids):
                results[id] = {'rank':i, 'score':cands[id],'doc':None}
            
            for resp in all_resp:
                if resp['_id'] in results:
                    results[resp['_id']]['doc'] = resp['_source']

            return results
        

# Example usage
if __name__ == '__main__':
    cs = CaseStudy()
    query = 'what the difference between desktop and laptop?'
    sql = 'select * from product where product_name = "联想小新电脑"'
    results = cs.assemble_find(query, sql)
    
    for id, result in results.items():
        print(f"Rank:{result['rank']}, Score:{result['score']}")
        print(result['doc'].get('query'))
   