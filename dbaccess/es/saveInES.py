# 例子：用户数据放在ES中
#######################################
import sys
import os
sys.path.insert(0, os.getcwd())
import settings
import logging
from utils.es_creator import ESIndex
class ESAccess:
        
        def __init__(self):
            self.creator = ESIndex()    
            logging.debug("ESAccess init success")
            
        def create_index(self,schema):
           pass
        
