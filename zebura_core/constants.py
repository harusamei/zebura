################################################
# 非全局，子模块内部使用的default or constant values
# D, default; C, constant
################################################

# Zebura中logging 分为5级 debug，info，warning，error，critical
# 最严重的错误是 raise ValueError， 直接退出

D_TOP_K = 5   # default top k for search
C_PROJECT_SHEET = 'project'  # sheet name of project info in metadata.xlsx
