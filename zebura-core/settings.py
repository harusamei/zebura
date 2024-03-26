import os
import yaml

def load_config():
    # Path to the config.yml file
    # 当前文件的上级目录
    os.environ["root_path"]= os.path.dirname(os.path.dirname(__file__))
    config_file = os.environ["root_path"]+'\config.yml'

    # Load the config file
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)

    # Save the environment variables in os
    for key, value in config.items():
        os.environ[key] = str(value)

# Call the function to load the config and save environment variables

if __name__ == '__main__':
    env_vars = os.environ
    load_config()
    for key, value in os.environ.items():
        print(f"{key}: {value}")
    print(f"Environment variables:{len(env_vars)}")



