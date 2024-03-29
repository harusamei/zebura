# load 关于要查询的DB的信息，以及good cases
import yaml
import os
import sys
if os.getcwd().lower() not in sys.path:
    sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from typing import Dict

# 读yaml文件保存为dict
def load_sql_re() -> Dict[str,str]:

    cwd = os.getcwd()    
    path = z_config['Paths', 'KnowledgePath']
    name = z_config['Patterns', 'sql_re']
    sp_file = os.path.join(cwd, path, name)

    # Load the config file
    with open(sp_file, 'r',encoding='utf-8') as file:
        patterns = yaml.safe_load(file)

    sql_re = dict()
    # Save the environment variables in os
    for key, value in patterns.items():
       sql_re[key] = str(value)
    return sql_re


# Example usage
if __name__ == '__main__':
    info_dict= load_sql_re()
    print(info_dict)
