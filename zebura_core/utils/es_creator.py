# Description: 创建ES索引，加载数据
#######################################
import sys
import os

sys.path.insert(0, os.getcwd())
sys.path.append("D:/zebura")
import logging
from zebura_core.utils.embedding import Embedding
from zebura_core.utils.es_base import ES_BASE
from zebura_core.knowledges.schema_loader import Loader
from zebura_core.utils.csv_processor import pcsv
from datetime import datetime
import re
from elasticsearch.exceptions import RequestError  # type: ignore
from zebura_core.constants import C_ES_SHORT_MAX


# 创建索引
class ESIndex(ES_BASE):

    def __init__(self):
        super().__init__()

        self.embedding = None
        self.text_analyzer = "cn_analyzer"  # 分词器名称
        self.keyword_analyzer = "keyword_analyzer"  # 关键词分词器名称

        logging.debug("ESIndex init success")

    def set_embedding(self):
        if not self.embedding:
            self.embedding = Embedding()

    def json2mapping(self, json_file, table_name='') -> tuple:

        loader = Loader(json_file)
        if table_name is None or table_name == '':
            table_name = loader.get_table_nameList()[0]
        table_info = loader.get_table_info(table_name)
        if not table_info:
            logging.error(f"Table '{table_name}' not found in schema.")
            return False

        # 从table的schema中抽取index的mapping
        # 默认index name为table name
        index_name = table_name
        es_mapping = {}  # es mapping

        columns = table_info.get('columns')
        if not columns:
            logging.warning(f"Table '{table_name}' not found in schema.")
            return False

        for column in columns:
            field_name = column["column_name"]
            field_type = column.get("type")
            if not field_type:
                field_type = "text"
            es_mapping[field_name] = {"type": field_type}
            if field_type == "text":
                es_mapping[field_name]["analyzer"] = self.text_analyzer
            # elif field_type == "keyword":
            #     es_mapping[field_name]["analyzer"] = self.keyword_analyzer

        return index_name, es_mapping

    # 创建索引
    def create_index(self, index_name, es_mapping):
        body = es_mapping
        # 定义索引主分片个数和分析器
        index_mapping = {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": "1",
                "analysis": {
                    "tokenizer": {
                        "smartcn_tokenizer": {
                            "type": "smartcn_tokenizer"  # 使用ES内置中文分词器
                        }
                    },
                    "analyzer": {
                        "cn_analyzer": {
                            "type": "custom",
                            "tokenizer": "smartcn_tokenizer",
                        },
                        "keyword_analyzer": {
                            "type": "keyword",
                            "tokenizer": "keyword",
                            "filter": ["lowercase"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": body
            }
        }
        # 索引的每个field都可以设置不同的analyzer
        try:
            self.es.indices.create(index=index_name, body=index_mapping)
            logging.info(f"Index '{index_name}' created successfully with 5 primary shards.")
        except RequestError as e:
            logging.error(e)
            return False

        return True

    # 补全embedding
    def complete_embs(self, docs, es_mapping):
        # 计算文本的embedding
        denseSet = []
        for field in es_mapping.keys():
            if es_mapping[field]['type'] == 'dense_vector':
                denseSet.append(field)

        # 如果有dense_vector字段，需要计算embedding
        if len(denseSet) > 0:
            self.set_embedding()
        else:  # 没有dense_vector字段，不需要计算embedding
            return docs

        for doc in docs:
            texts = []
            # embedding之前该field存放原始文本
            for field in denseSet:
                texts.append(doc[field])
            embs = self.embedding.get_embedding(texts)
            for i, field in enumerate(denseSet):
                doc[field] = embs[i].tolist()

        return docs

    # 返回ES操作文档数量
    # docs是一个list，每个元素是一个dict
    def insert_docs(self, index_name, docs):
        if not self.is_index_exist(index_name):
            logging.error(f"index {index_name} not exist")
            return 0
        if docs is None or len(docs) == 0:
            logging.error(f"no docs to insert")
            return 0
        try:
            count = len(docs)
            new_docs = list(map(lambda doc: [
                {"index": {"_index": index_name}},
                doc],
                                docs))
            new_docs = sum(new_docs, [])
            # 在索引中添加文档, refresh=True 使得文档立即可见
            response = self.es.bulk(index=index_name, body=new_docs, refresh='true')
        except Exception as e:
            logging.error(f"Error inserting documents: {e}")
        else:
            if response['errors']:
                logging.error(f"can not insert docs in {index_name}")
                count = 0
            else:
                logging.info(f'insert {count} in {index_name}')

        return count

    # 格式规范化
    def format_doc(self, doc, es_mapping) -> dict:
        delkeys = []
        # clear empty fileds
        for key in doc.keys():
            if doc[key] == '':
                delkeys.append(key)
        for key in delkeys:
            del doc[key]
        # data type check, 日期，整数，浮点数
        dateSet = []
        shortSet = []
        NumSet = []
        for key in es_mapping.keys():
            if es_mapping[key]['type'] == 'date':
                dateSet.append(key)
            elif es_mapping[key]['type'] == 'short':
                shortSet.append(key)
            elif es_mapping[key]['type'] in ['float', 'integer', 'long', 'double', 'half_float', 'scaled_float',
                                             'byte']:
                NumSet.append(key)
        # date format
        now = datetime.now()
        for key in dateSet:
            date_str = doc.get(key)
            if date_str is None or date_str == '':
                date_str = now.strftime('%Y/%m/%d')
            # 日期格式统一为yyyy-mm-dd
            if re.match(r'\d{4}/\d{2}/\d{2}', date_str):
                date_obj = datetime.strptime(date_str, '%Y/%m/%d')
            elif re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            else:
                date_str = now.strftime('%Y/%m/%d')
                date_obj = datetime.strptime(date_str, '%Y/%m/%d')

            doc[key] = date_obj.strftime('%Y-%m-%d')

        # digit format
        for key in doc.keys():
            if key in shortSet:
                doc[key] = re.sub(r'\D', '', doc[key])
                doc[key] = min(int(doc[key]), C_ES_SHORT_MAX)
            elif key in NumSet:
                doc[key] = re.sub(r'[^\d.]', '', doc[key])
                doc[key] = float(doc[key])

        return doc

    ########用于测试的methods
    # 简单加载CSV数据，没有去重
    def load_csv(self, index_name, csv_file, sch_file):
        mypcsv = pcsv()
        csv_rows = mypcsv.read_csv(csv_file)
        if not csv_rows:
            return False

        index_name, es_mapping = self.json2mapping(sch_file, index_name)
        if self.is_index_exist(index_name):
            print(f"Index '{index_name}' already exists.")
        else:
            self.create_index(index_name, es_mapping)

        docs = []
        for doc in csv_rows:
            doc = self.format_doc(doc, es_mapping)
            docs.append(doc)

        # 计算文本的embedding
        docs = self.complete_embs(docs, es_mapping)
        if self.insert_docs(index_name, docs):
            print(f"Data loaded into index '{index_name}' successfully.")
            return True
        else:
            print(f"Failed to load data into index '{index_name}'.")
            return False

    def test(self, index, field, word, size=5):
        query = {
            "size": size,  # Return top3 results
            "query": {
                "match": {field: word}
            }
        }
        return self.es.search(index=index, body=query)


# examples usage
if __name__ == '__main__':
    cwd = os.getcwd()

    # name = "D:/zebura/training/ikura/ikura_gcases.json"
    name = 'E:/zebura/training/amazon/amazon_gcases.json'
    sch_file = os.path.join(cwd, name)
    print("文件地址", sch_file)
    creator = ESIndex()
    creator.load_csv('amazon_gcases', "E:/zebura/training/amazon/dbInfo/amazon.csv", sch_file)
    # print(creator.test('ikura_gcases','product_name','开关'))