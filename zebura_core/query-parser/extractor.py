import re
from zebura-core import knowledges.info_loader as load_sql_re
class Extractor:
    def __init__(self):
        self.pattern = r"SELECT\s+(.*?)\s+FROM\s+(.*?)\s*;"
        self.slots = ['select','table','condition','limit','distinct','group_by','order_by','as','like','offset']
        # select, group_by, order_by 为多值用,分隔
        self.infoDict = {slot:None for slot in self.slots}

    def extract_info(self,sql):
        # Regular expression pattern to match SELECT statements
        pattern = r"FROM\s+(.*?)\s*(WHERE|;)"

        # Extract the table name and columns using regex
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            return match.group(1)
        else:
            return None

import re

# 示例SQL语句
sql_query = """
SELECT column1, column2 FROM table_name;
SELECT DISTINCT column1 FROM table_name;
SELECT column1 FROM table_name WHERE column2 = 'value';
SELECT column1, COUNT(*) FROM table_name GROUP BY column1;
SELECT column1, COUNT(*) FROM table_name GROUP BY column1 HAVING COUNT(*) > 1;
SELECT column1 FROM table_name ORDER BY column1 ASC;
SELECT column1 FROM table_name LIMIT 10;
SELECT column1 FROM table_name LIMIT 10 OFFSET 20;
SELECT column1 FROM table_name ORDER BY column1 FETCH FIRST 10 ROWS ONLY;
"""

# Example usage
extractor = Extractor()
sql_statement = "SELECT column1, column2 FROM table_name ;"
result = extractor.extract_info(sql_statement)
print(result)
