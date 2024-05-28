################################################
# 非全局，子模块内部使用的default or constant values
# D, default; C, constant
################################################

# Zebura中logging 分为5级 debug，info，warning，error，critical
# 最严重的错误是 raise ValueError， 直接退出

D_TOP_K = 5   # default top k for search
D_TOP_GOODCASES = 3            # default top k for good cases
D_MAX_BATCH_SIZE = 1000        # default max batch size for insert_docs
D_MAX_ROWS = 1000000           # default max rows for read_csv
C_PROJECT_SHEET = 'project'     # sheet name of project info in metadata.xlsx
C_ES_SHORT_MAX = 32767          # default max value of short type in ES
