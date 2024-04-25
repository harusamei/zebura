# 非全局，子模块内部使用的常量
import os
import logging
# DEBUG or RELEASE
os.environ['ENV'] = 'RELEASE'

# 输出查询结果的数量
os.environ['DEFAULT_OUTPUT_SIZE'] = '20'
os.environ['DEFAULT_SEARCH_SIZE'] = '20'

# case study的好样本信息
os.environ['CASE_STUDY_SCHEMA'] = 'datasets\gcases_schema.json'
os.environ['GOLDEN_CASES_INDEX'] = 'goldencases'

if os.getenv('ENV') == 'DEBUG':
    logging.basicConfig(level=logging.DEBUG)        #输出debug信息
else:
    logging.basicConfig(level=logging.INFO)