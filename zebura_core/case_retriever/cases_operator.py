#####################
# 创建，增删改 golden cases
# golden cases 存放在ES中，通过ES查询
# golden cases schema 文件名为<project_code>_gcases.json
#####################
import os
import sys
import logging
sys.path.insert(0, os.getcwd())

from settings import z_config
from utils.es_operator import ESOps
from utils.es_creator import ESIndex
from case_retriever.study_cases import CaseStudy
from utils.csv_processor import pcsv
import json
import datetime

class CaseOps(ESOps):
        
    def __init__(self):
        # golden cases 用ES存储，以 ESOps 为基类
        super().__init__()

        self.creator = ESIndex()
        self.gc = CaseStudy()
        self.text_analyzer = "cn_analyzer"       # 分词器名称

        # load schema of goodcases
        project_code = z_config['Training','project_code'] # project code约定为db schema的上级目录名
        self.gcase_index = f"{project_code}_gcases"  # index name 名字强制为<project_code>_gcases
        self.es_schema = self.gen_gcase_schema()

        logging.debug("CaseOps init success")

    # 生成golden cases的ES index schema
    # golden cases schema 固定，所有项目的golden case  schema一样
    def gen_gcase_schema(self):
        
        fields =[
            ('no','integer'),               # id, 流水号, 从1开始
            ('query','text'),               # query, 用户输入的query
            ('qemb','dense_vector'),        # query embedding, 用于计算相似度
            ('sql','text'),                 # sql, NL2sql
            ('action','text'),              # 非sql的操作, 如call function
            ('action_input','text'),        # 非sql的操作的输入
            ('category','keyword'),         # 类别，sql, action, chat, info(提供信息)
            ('updated_date','date'),        # 更新日期
            ('next_msg','text'),            # 下一步的提示
            ('topic','text'),               # 该query涉及的主题,场景
            ('comment','text'),             # 备注
        ]
        es_schema = {}
        for field in fields:
            field_name, field_type = field              
            es_schema[field_name] = {"type": field_type}
            if field_type == "text":
                es_schema[field_name]["analyzer"] = self.text_analyzer
        return es_schema
    
    # 将cases存入ES
    def store_cases(self, csv_filename):

        mypcsv = pcsv()
        csv_rows = mypcsv.read_csv(csv_filename)
        if not csv_rows:
            return False
        new_docs=[]
        for doc in csv_rows:
            doc = self.preprocess(doc)
            query = doc['query']
            sql = doc['sql']

            results = self.gc.assemble_find(query,sql, topk=3)
            max_score = 0
            for result in results:
                if result['score'] > max_score:
                    max_score = result['score']
            print(f"max score: {max_score}, query: '{query}'")
            if max_score < 0.8:
                new_docs.append(doc)
            else:
                print(f"similar Doc with '{results[0]['doc']['query']}' already exists")
                continue  
            es_docs = self.creator.complete_embs([doc],self.es_schema)
            self.creator.insert_docs(self.gcase_index, es_docs)
        
        # 为新文档生成embedding
        print(f'total docs: {len(csv_rows)}, insert docs: {len(new_docs)}')
        return
    
    def get_doc_count(self):
        return super().get_doc_count(self.gcase_index)
    
    def preprocess(self, doc, no=-1):
        doc['qemb'] = doc['query']
        if doc['updated_date'] == '': 
                doc['updated_date'] = datetime.date.today().strftime('%Y-%m-%d')
        if no < 0:
            doc['no'] = self.get_doc_count() + 1
        else:
            doc['no'] = no+1
        # 删除空字段
        delKeys =[key for key in doc.keys() if doc[key] == '']
        {doc.pop(key, None) for key in delKeys}

        while True:
            result = self.gc.es.search(self.gcase_index, "no", doc['no'],1)
            count = result['hits']['total']['value']
            if count == 0:
                break
            doc['no'] += 1
        
        return doc
    #将csv文件中的cases从golden cases中删除
    def delete_cases(self, doc_nos):
        del_uids = []
        for no in doc_nos:
            result = self.gc.es.search(self.gcase_index, "no", no,1)
            count = result['hits']['total']['value']
            if count >1 :
                print(f"more than one doc with id {id} found.")
            for item in result['hits']['hits']:
                hit = item['_source']
                if hit['no'] == no:
                    del_uids.append(item['_id'])
        
        for uid in del_uids:
            self.delete_doc(self.gcase_index, uid)
        print(f"Delete {len(del_uids)} cases from index '{self.gcase_index}'.")
      
    # 创建初始索引
    def create_index(self):

        if self.is_index_exist(self.gcase_index):
            print(f"Index '{self.gcase_index}' already exists.")
        else:
            self.creator.create_index(self.gcase_index, self.es_schema)
        return
   
    @staticmethod
    def write_json(dict, out_path):
        from collections import OrderedDict
        ordered_dict = OrderedDict(sorted(dict.items()))
        with open(out_path, 'w', encoding='utf-8-sig') as json_file:
            json_file.write(json.dumps(ordered_dict, indent=4, ensure_ascii=False))

def use_case():
    schema = CaseOps().gen_gcase_schema()
    CaseOps.write_json(schema, 'gcases.json')
    print('schema written to gcases.json')

def use_case2():
    caseOps = CaseOps()
    cwd = os.getcwd()
    file_path = os.path.join(cwd,'training\\amazon\\dbInfo\\gcases.csv')
    caseOps.store_cases(file_path)

# Example usage
if __name__ == '__main__':
    caseOps = CaseOps()
    # caseOps.create_index()
    # print(caseOps.get_doc_count())
    use_case2()
    