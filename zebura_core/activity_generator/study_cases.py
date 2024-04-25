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
            must_columns = ['query', 'qembedding', 'sql', 'action']
            for col in must_columns:
                if col not in [c['column_en'] for c in self.columns]:
                    raise ValueError(f"Column {col} not found in table {table}")
            
        # 欧氏距离或manhattan distance, _score 越小越相似，区间是[0, +∞)
        def find_similar_query(self, query, topk=5):
            index = self.gcase_index
            results = self.es.search_word(index, "query", query, topk)
            return results
        
        def find_similar_sql(self, sql, topk=5):
            index = self.gcase_index
            results = self.es.search_word(index, "sql", sql, topk)
            return results
        # ES为了保证所有的得分为正，实际使用（1 + 余弦相似度）/ 2，_score [0，1]。得分越接近1，表示两个向量越相似  
        def find_similar_vector(self, query, topk=5):
            index = self.gcase_index
            results = self.es.search_word(index, "qembedding", query, topk)
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
            print("k:",k)
            # 未出现在rank中的doc score为0
            for rank in ranks_lists:
                for i,doc in enumerate(rank):
                    if not docs.get(doc):
                        docs[doc] = 1/(i+k)
                    else:
                        docs[doc] += 1/(i+k)
            
            sorted_docs = sorted(docs.items(), key=lambda item: item[1], reverse=True)
            return sorted_docs

        def assemble_find(self, query, sql=None, topk=5) -> list:

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

    # Example usage:
    rank_list1 = [1,2,3,4,5,6]
    rank_list2 = [5,4,3,2,1,6]
    rank_list3 = [1, 2, 6]
    ranks_lists = [rank_list1, rank_list2, rank_list3]

    weighted_rank_list = cs.rrf_weighted(ranks_lists)
    print("Weighted Rank List:", weighted_rank_list)
   