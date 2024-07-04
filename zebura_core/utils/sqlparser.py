##########################################################
# 对SQL进行解析，提取基本元素, check the SQL syntax
# 基于sqlparse库的扩展，解析SQL语句，提取SQL中的列名，表名，条件等信息
##########################################################
import sqlparse
from sqlparse.tokens import Keyword, DML, Whitespace, Wildcard, Name,Punctuation, Token, Literal
from sqlparse.sql import IdentifierList, Identifier,Where,Function
import logging
import re

class ParseSQL:
    def __init__(self):
        logging.debug("ParseSQL init done")
    
    @staticmethod
    def get_columns(tokens):
        columns = {'all_cols':{},'distinct':False}
        # column 在select, from 之间
        begflag = False
        all_cols = columns['all_cols']
        for token in tokens:
           
            if token.ttype is DML and token.value.upper() == 'SELECT':
                begflag = True
                continue
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                break
            if not begflag:
                continue
            if isinstance(token, IdentifierList):
                for item in token:
                    if isinstance(item, Identifier):
                        all_cols[item.get_real_name()] = item.tokens
                    elif isinstance(item, Function):
                        all_cols[item.get_real_name()] ={'name':item.value,'ttype':'Function'}    
                break
            if isinstance(token, Identifier):
                all_cols[token.get_real_name()] = token.tokens
                
            if token.ttype is Wildcard:
                all_cols['*'] = {'ttype':'Wildcard'}

            if token.ttype is Keyword and token.value.upper() == 'DISTINCT':
                columns['distinct'] = True
        # 解析 identifier
        asFlag = False   
        for k,v in all_cols.items():
            if not isinstance(v, list):
                continue
            all_cols[k] = {'ttype':'Name','name':'','as':''}
            for token in v:
                if token.is_whitespace:
                    continue
                if token.ttype is Name:
                    full_name = all_cols[k].get('full_name','')+token.value 
                    all_cols[k]['full_name'] = full_name
                    all_cols[k]['name'] = token.value
                # full_name保留表名前缀
                if token.ttype is Punctuation:
                    full_name = all_cols[k].get('full_name','')+token.value
                    all_cols[k]['full_name'] = full_name
                if token.ttype is Wildcard:
                    all_cols[k]['ttype'] ='Wildcard'
                    all_cols[k]['name'] = '*'
                if isinstance(token, Function):
                    all_cols[k]['ttype'] ='Function'
                    all_cols[k]['name'] = token.value
                if token.ttype is Keyword and token.value.upper() == 'AS':
                    asFlag = True
                    continue
                if isinstance(token, Identifier) and asFlag:
                    all_cols[k]['as']=  token.get_real_name()
                    asFlag = False
        return columns
    
    # 表只有一个
    @staticmethod
    def get_table(tokens):
        table = {'name':'', 'order by':'','group by':'',
                  'limit':'','offset':'','join':[]}
        table_names = []
        begFlag = False
        for token in tokens:
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                begFlag=True
                continue
            if isinstance(token,Where):
                continue
            if not begFlag:
                continue
            if isinstance(token, Identifier) or isinstance(token, IdentifierList):
                table_names.append(token)
            if token.ttype is Keyword:          #        print(tokens)
                table_names.append(token.value)
            if token.ttype in Token.Literal:
                table_names.append(token.value)
            # if token.ttype in Token.Literal.String:
            #     table_names.append(token.value)
        # 解析 identifier
        processed = []
        some_keys = ['LIMIT','OFFSET','ORDER BY','GROUP BY']
        for key in some_keys:
            indx = table_names.index(key) if key in table_names else -1
            if indx > -1:
                processed.extend([indx,indx+1])
                key_token = table_names[indx+1]
                if isinstance(key_token, Identifier):
                    table[key.lower()] = key_token.get_real_name()
                elif isinstance(key_token, IdentifierList):
                    table[key.lower()] = [(x.get_real_name()) for x in key_token if isinstance(x, Identifier)]
                else:
                    table[key.lower()] = key_token
        table_names = [x for i,x in enumerate(table_names) if i not in processed]
        processed = []
        for i, token in enumerate(table_names):
            if isinstance(token, Identifier):
                continue
            if 'join' in token.lower():
                processed.extend([i,i+1])
            if token.lower() == 'on' and len(processed) > 0:
                processed.append(i)
        table['join'] = [x for i,x in enumerate(table_names) if i in processed]
        if isinstance(table_names[0], Identifier):
            table['name'] = table_names[0].get_real_name()
        else:
            table['name']= table_names[0]
        
        return table
    
    @staticmethod
    def get_conditions(tokens):
        conditions = []
        for token in tokens:
            if isinstance(token, Where):
                for item in token.tokens:
                    if item.is_whitespace or item.value in ['WHERE',';']:
                        continue
                    conditions.append(item.value)
        return conditions
   
    def parse_sql(self,sql):
        parsed = sqlparse.parse(sql)
        if parsed and parsed[0].get_type() != 'SELECT':
            print("Can't handle this SQL")
            return None
        elif len(parsed) < 1:
            print("no SQL to parse")
            return None
        
        tokens = filter(lambda x: not x.is_whitespace, parsed[0].tokens)
        tokens = list(tokens)
        
        slots = self.make_a_slots()

        slots['columns'] = self.get_columns(tokens)
        slots['table'] = self.get_table(tokens)
        slots['conditions'] = self.get_conditions(tokens)

        return slots
    
    @staticmethod
    def formate(sql):
        tStr = sqlparse.format(sql, reindent=True, keyword_case='upper')
        tStr = re.sub(r'\n|\t',' ', tStr)
        tStr = re.sub(' +', ' ', tStr)
        return tStr
    
    def make_a_slots(self):
        return {
            'columns': {'all_cols':{},'distinct':False},
            'table': {},
            'conditions': [],
        }

       
        
# example usage    
if __name__ == '__main__':
    sparser = ParseSQL()
    sql_querys ="""
    SELECT DISTINCT customer_id AS ID,  first_name AS FirstName,  last_name AS LastName, city AS City FROM  customers ORDER BY  City ASC, LastName DESC;
    select * from table_name;
    select column1 FROM table_name;
    SELECT DISTINCT column1 FROM table_name;
    SELECT column1 FROM table_name WHERE column2 = 'value' AND column3 = 'value' OR column4 = 'value';
    SELECT column1, COUNT(*) FROM table_name GROUP BY column1;
    SELECT column1, COUNT(*) FROM table_name GROUP BY column1 HAVING COUNT(*) > 1;
    SELECT column1 FROM table_name ORDER BY column1 ASC;
    SELECT column1 FROM table_name LIMIT 10;
    SELECT column1 FROM table_name LIMIT 10 OFFSET 20;
    SELECT column1 FROM table_name ORDER BY column1 FETCH FIRST 10 ROWS ONLY;
    SELECT column1 AS renamed_column1, column2 AS renamed_column2 FROM table_name;
    UPDATE table_name SET column1 = value1, column2 = value2 WHERE condition;
    SELECT * FROM employees WHERE (department = 'Sales' or salary > 10000) AND department = 'Marketing';
    SELECT * FROM products  WHERE product_name LIKE "%apple%" AND price > 1000;
    SELECT d.department_name AS Department,COUNT(e.employee_id) AS NumberOfEmployees FROM departments d  LEFT JOIN employees e ON d.department_id = e.department_id  GROUP BY d.department_name  ORDER BY  NumberOfEmployees DESC;
    SELECT order_id, customer_id, order_date, total_amount FROM  orders  WHERE  order_date BETWEEN '2024-01-01' AND '2024-12-31'  ORDER BY  order_date ASC;
    """
    sql_querys = sql_querys.split("\n")[1:-1]

    for sql in sql_querys:
        sql =sparser.formate(sql)
        slots = sparser.parse_sql(sql)
        all_checks = sparser.get_checkPoints(slots)
        if slots is not None:
            print(sql)
            print(all_checks)
            print(slots)


   