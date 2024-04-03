import sqlparse
from sqlparse.sql import Where

def parse_sql(sql):

    formatted_sql = sqlparse.format(sql, keyword_case='upper')
    print("sql : "+formatted_sql)

    parsed = sqlparse.parse(formatted_sql)
    if not parsed:
        print("Can't parse this SQL")
        return None
    # 可以解析的信息
    slots = dict.fromkeys(['columns','from', 'conditions', 'distinct', 'limit', 'offset','order by','group by'])

    tokens = parsed[0].tokens
    kwords = []
    for token in tokens:
        if token.is_whitespace:
            continue
        if token.value == "DISTINCT":
            slots['distinct'] = True
            continue
        if len(kwords)>1 and kwords[-1] in ['LIMIT','OFFSET','ORDER BY','GROUP BY']:
            slots[kwords[-1].lower()] = token.value
            kwords.pop()
            continue
        kwords.append(token.value)
    # 切分为select...from...where几部分
    print(kwords)
    # 只处理查询sql, 包含以下keyword 暂时不能处理的SQL
    if kwords[0]!="SELECT":
        print("Can't handle this SQL")
        return None
    undo = {'HAVING','FETCH FIRST'}
    if undo & set(kwords):
        print("Can't handle this SQL")
        return None
    
    select_index = kwords.index("SELECT")
    from_index = kwords.index("FROM")
    slots['columns'] = kwords[select_index+1:from_index]
    slots['from'] = kwords[from_index+1]

    # 切分where条件
    slots['conditions']= []
    for token in tokens:
        if isinstance(token, Where):
            for item in token.tokens:
                if item.is_whitespace or item.value in ['WHERE',';']:
                    continue
                slots['conditions'].append(item.value)
    return slots   
       
        
# example usage    
if __name__ == '__main__':
    sql_querys ="""
    select * from table_name;
    select column1, column2 FROM table_name;
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
    """.split(";")
    for sql in sql_querys:
        parse_sql(sql)
    sql ="""
        SELECT * 
        FROM employees
        WHERE (department = 'Sales' or salary > 10000)
        AND department = 'Marketing';
    """
    sql= """
        SELECT * 
        FROM products
        WHERE product_name LIKE "%apple%" AND price > 1000;
    """
    parse_sql(sql)
        
        
