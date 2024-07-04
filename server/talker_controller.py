from zebura_core.query_parser.parser import Parser
import pymysql
import asyncio

parser = Parser()

conn = pymysql.connect(
    host='localhost',		# 主机名（或IP地址）
    port=3306,				# 端口号，默认为3306
    user='root',			# 用户名
    password='123456',	    # 密码
    charset='utf8mb4'  		# 设置字符编码
)
cursor = conn.cursor()
conn.select_db("ikura")
class Controller:
    def __init__(self):
        pass
    async def apply(self,query):
        try:
            #任务分发
            categoryList=[]
            if query["type"]=="user":  #暂定用户
                print("控制层的传输数据",query["content"])
                result = asyncio.run(parser.apply(query["content"]))
                if result["status"] is True:
                    print(result["msg"][0])
                    cursor.execute(result["sql1"][0].lower())
                    answer = cursor.fetchall()
                    keys = [column[0] for column in cursor.description]
                    result_dicts = []
                    for row in answer:
                        result_dict = dict(zip(keys, row))
                        result_dicts.append(result_dict)
                    for result_dict in result_dicts:
                        categoryList.append(result_dict)
                    return categoryList
                else:
                    return f"{result['msg']}"
            elif query["type"]=="assistant":
                return "暂未开发assistant模式"
            elif query["type"]=="transition":
                return "暂未开发transition模式"
            else:
                return query
        except Exception as e:
            return str(e)