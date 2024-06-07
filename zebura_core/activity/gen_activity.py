# 确定当前query的activity
import query_parser.parser as parser
class GenActivity:
    def __init__(self):
        pass

    def gen_activity(self, slots):
        # 1. 生成activity
        activity = {}
        activity['activity'] = 'query'
        activity['table_name'] = slots['from']
        activity['columns'] = slots['columns']
        activity['conditions'] = slots['conditions']
        return activity
