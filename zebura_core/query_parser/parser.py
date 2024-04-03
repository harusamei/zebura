# query parser，可利用的信息包含 slots from extractor, good cases, schema of db, ask db, gpt
class parser:
        
        def __init__(self):
            self.table_name = table_name
            self.shema = shema
            self.loader = info_loader.PatLoader()
            self.normalizer = normalizer(table_name,shema)
            self.extractor = Extractor()
            self.prompt = f"有一张表名为{table_name}，下面句子如果是关于查询{table_name}请转换为SQL查询，如果不是，请直接输出not sql"
        
        def parse(self,query):
            # 1. Extract the slots from the query
            slots = self.extractor.extract_info(query)
            # 2. Normalize the query
            sql_query = self.normalizer.convert_to_sql(query)
            # 3. Use the good cases to check the query
            if sql_query:
                return sql_query
            else:
                return "not sql"