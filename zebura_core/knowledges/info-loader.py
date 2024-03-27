# load 关于要查询的DB的信息，以及good cases
import yaml
import os
FILE_PATH = 'zebura-core\knowledges\sql-patterns.yml'
# 读yaml文件保存为dict
def load_sql_re():
    print(os.getcwd())
    sp_file = FILE_PATH 
    # Load the config file
    with open(sp_file, 'r',encoding='utf-8') as file:
        patterns = yaml.safe_load(file)

    sql_re = {}
    # Save the environment variables in os
    for key, value in patterns.items():
       sql_re[key] = str(value)
    return sql_re



def load_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

# Example usage
info_dict= load_sql_re()
print(info_dict)
