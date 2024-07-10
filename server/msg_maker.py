# 在各模块间传递的共享信息的结构定义

# request={
#         "msg": content,
#         "context": context,
#         "type": "user/assistant/transaction", # 用户， controller, 增加 action间切换, reset 到某一action
#         "format": "text/md/sql/dict...", # content格式，与显示相关
#         "status": "new/hold/failed/succ", # 新对话,多轮继续；执行失败；执行成
#         
# only log      "from": "nl2sql/sql4db/interpret/polish" # 当前模块
# only log      "others": 当前步骤产生的次要信息 # 下一个任务
#     }
# request为用户发出的请求, log为后台各模块传递的中间结果 
def make_a_log(funcName):
        return {
            'msg': '',          # 当前步骤产生的主要信息
            'note': '',         # 记录出错类型, 格式 ERR_TAGS , details
            'status': 'succ',
            'from': funcName,    # 当前模块
            'type': 'transaction',
            'format': 'text',
            'others': {},        # 当前步骤产生的次要信息
            'hint': ''           # 当前步骤产生的提示信息
        }

def make_a_req(content:str):
    return {
        "msg": content,         # 给用户的主信息
        "context": [],
        "type": "user",
        "format": "text",
        "status": "new",
        "note": "",             # 补充，或出错类型
        'hint': ''              # 当前步骤产生的提示信息
    }