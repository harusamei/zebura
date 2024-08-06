# 执行活动
import sys
import os

sys.path.insert(0, os.getcwd().lower())
from settings import z_config
import pymysql
import logging
from tabulate import tabulate
from server.msg_maker import make_a_log


class ExeActivity:
    # db_type: 数据库类型，sch_loader: 项目的schema
    def __init__(self, sch_loader):

        self.sch_loader = sch_loader
        self.db_name = z_config['Training', 'project_code']
        db_type = z_config['TrainingDB', 'db_type']
        self.cnx = self.connect(db_type)
        if self.cnx is None:
            raise ValueError("Database connection failed")
        logging.info(f"ExeActivity init success")

    @staticmethod
    def connect(type='mysql'):

        section = 'TrainingDB'
        host = z_config[section, 'host']
        port = int(z_config[section, 'port'])
        user = z_config[section, 'user']
        pwd = z_config[section, 'pwd']

        if type == 'mysql':
            cnx = pymysql.connect(
                host=host,  # 主机名（或IP地址）
                port=port,  # 端口号，默认为3306
                user=user,  # 用户名
                password=pwd,  # 密码
                charset='utf8mb4',  # 设置字符编码
                cursorclass=pymysql.cursors.DictCursor  # 返回字典类型的游标
            )
            return cnx
        else:
            raise ValueError(f"ERR_cursor: {type} not supported")

    def checkDB(self) -> str:  # failed, succ
        cursor = self.cnx.cursor()
        cursor.execute(f"SHOW DATABASES")

        databases = [db['Database'] for db in cursor.fetchall()]
        # check if the database exists
        if self.db_name not in databases:
            print(f"{self.db_name} not found, create it first")
            return "failed"
        cursor.close()
        return "succ"

    def exeSQL(self, sql):

        answer = make_a_log("exeSQL")
        answer["format"] = "dict"
        try:
            cursor = self.cnx.cursor()
            cursor.execute(f"USE {self.db_name}")
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            if len(result) > 0:
                answer["msg"] = result
            else:
                answer['status'] = "failed"
                answer['note'] = "ERR_noresult"
        except Exception as e:
            print(f"Error: {e}")
            answer["note"] = f"ERR_cursor, {e}"
            answer["status"] = "failed"

        return answer

    @staticmethod
    def toMD_format(results):
        markdown = tabulate(results, headers="keys", tablefmt="pipe")
        print(markdown)


if __name__ == "__main__":
    from zebura_core.knowledges.schema_loader import Loader

    cwd = os.getcwd()
    name = z_config['Training', 'db_schema']  # 'training\ikura\ikura_meta.json'
    sch_loader = Loader(os.path.join(cwd, name))
    exr = ExeActivity(sch_loader)
    exr.checkDB()
    results = exr.exeSQL("SELECT * FROM product WHERE actual_price = 1000 AND brand = 'Pear';")
    exr.toMD_format(results['msg'])
    print(results)
