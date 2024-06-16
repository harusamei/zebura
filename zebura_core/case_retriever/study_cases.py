# case study 模式，找相似的案例
import os
import sys
sys.path.insert(0, os.getcwd())
import logging
from settings import z_config
from zebura_core.utils.es_searcher import ESearcher
from zebura_core.knowledges.schema_loader import Loader

dtk = 5     # default top k for search

class CaseStudy:
        
        def __init__(self):
            # good cases 存放在ES中，通过ES查询
            self.es = ESearcher()
            # load schema of goodcases
            name = z_config['Training','db_schema']  # 'Training\ikura\ikura_meta.json'
            # 获得<project_code>_gcases.json
            name = name.replace('_meta','_gcases')
            cwd = os.getcwd()
            self.loader = Loader(os.path.join(cwd, name))

            project_code = os.path.basename(name).split('_')[0]
            self.gcase_index = f"{project_code}_gcases"  # 'gcases'
            table = self.loader.get_table_nameList()[0]
            self.columns = self.loader.get_all_columns(table)

            logging.debug("CaseStudy init success")
            
            
        # 欧氏距离或manhattan distance, _score 越小越相似，区间是[0, +∞)
        def find_similar_query(self, query, topk=dtk):
            index = self.gcase_index
            results = self.es.search(index, "query", query, topk)
            return results
        
        def find_similar_sql(self, sql, topk=dtk):
            index = self.gcase_index
            results = self.es.search(index, "sql", sql, topk)
            return results
        # ES为了保证所有的得分为正，实际使用（1 + 余弦相似度）/ 2，_score [0，1]。得分越接近1，表示两个向量越相似  
        def find_similar_vector(self, query, topk=dtk):
            index = self.gcase_index
            results = self.es.search(index, "qembedding", query, topk)
            return results
        
        """
            Compute the weight using Reciprocal Rank Fusion (RRF) for a list of rank lists.
            ranks_lists (list of lists): List of rank lists to be fused.
            Returns: list: Weighted rank list.
        """
        @staticmethod
        def rrf_weighted(ranks_lists):
            docs={}
            # k是平滑因子，这里取最大的rank长度
            k =max(len(rank) for rank in ranks_lists)
            # 未出现在rank中的doc score为0
            for rank in ranks_lists:
                for i,doc in enumerate(rank):
                    if not docs.get(doc):
                        docs[doc] = 1/(i+k)
                    else:
                        docs[doc] += 1/(i+k)
            
            sorted_docs = sorted(docs.items(), key=lambda item: item[1], reverse=True)
            return sorted_docs
        
        # hybrid search
        def assemble_find(self, query, sql=None, topk=dtk) -> list:

            resps = [None]*3
            resps[0] = self.find_similar_query(query, topk)
            if sql:
                resps[1] = self.find_similar_sql(sql, topk)
            else:
                resps[1] = None
            resps[2] = self.find_similar_vector(query, topk)
            
            rank_list=[None]*3
            for i in range(3):
                if resps[i] is not None:
                    rank_list[i] = [hit['_id'] for hit in resps[i]['hits']['hits']]
                else:
                    rank_list[i] = []
            sorted_ids = self.rrf_weighted(rank_list)
            # print("sorted_ids:",sorted_ids)
            docs = {}
            for i in range(3):
                if resps[i] is None:
                    continue
                for hit in resps[i]['hits']['hits']:
                    docs[hit['_id']] = hit['_source']
            
            results = []
            for i, id in enumerate(sorted_ids):
                results.append({'doc':docs[id[0]], 'rank':i+1, 'score':id[1]})
            
            return results
        

# Example usage
if __name__ == '__main__':

    cs = CaseStudy()
    query = 'what the difference between desktop and laptop?'
    sql = 'select * from product where product_name = "联想小新电脑"'
    results = cs.assemble_find(query)
    
    for result in results:
        print(f"rank:{result['rank']}, score:{result['score']}")
        print(result['doc'].get('query'))

   