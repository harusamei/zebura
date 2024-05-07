import configparser
import sys
import os

class Settings:

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini',encoding='utf-8')
        self.settings()

    def settings(self):
        # insert paths
        cwd = os.getcwd()
        module_path = self.config.get('Paths', 'ModulePath')
        sys.path.insert(0, cwd)
        sys.path.insert(0, os.path.join(cwd,module_path))

        # insert model path 所有子目录
        model_path = os.path.join(cwd,module_path)
        for root, dirs, files in os.walk(model_path):
            for dir in dirs:
                if dir[0] != '.' and dir[0] != '_':    
                    sys.path.insert(0, os.path.join(root, dir))
        # remove duplicates
        sys.path= list(dict.fromkeys(path.lower() for path in sys.path))
       
    def __getitem__(self, keys):
        return self.config.get(keys[0], keys[1])
    
        
# Zebura settings
z_config = Settings()

# Example usage
if __name__ == '__main__':

    Settings()
    print("\n".join(sys.path))
    print(z_config['LLM','OPENAI_KEY'])

