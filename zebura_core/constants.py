# 非全局相关，子模块内部使用的常量
import os

# 输出查询结果的数量
os.environ['DEFAULT_OUTPUT_SIZE'] = '20'
os.environ['DEFAULT_SEARCH_SIZE'] = '20'
# case study的好样本信息
os.environ['CASE_STUDY_SCHEMA'] = 'datasets\gcases_schema.json'