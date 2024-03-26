import asyncio
from openai import AsyncOpenAI
from openai import OpenAIError
from esQuery import ESQuery
import re

# find typical cases to reference
class caseFinder:

    def __init__(self, gpt_api_key):
        self.sk = gpt_api_key
        self.client = AsyncOpenAI(api_key=gpt_api_key)
        self.es = ESQuery('10.110.153.75', 9200)
        if not self.es.ping():
            raise ValueError("Connection failed")
        else:
            print("Connected to Elasticsearch")
        self.casesIndex = "typicalCases"

        self.querys = []
        # 占解析的槽位
        parsedTerms = ["query","isSql","selectField","tableName","whereField","whereValue","gptReply"]
           

    def findSamples(self,query):
        parsedDict = {term: None for term in self.parsedTerms}
        parsedDict["query"] = query
        self.enhanceDict(parsedDict)
        samples = self.multi_search(parsedDict)


    def enhanceDict(self,parsedDict):
        prompt = "如果下面句子是查询数据库，请生成对应的SQL，如果不是请生成回复\n"
        answer = asyncio.run(self.callGPT(prompt, query))
        parsedDict["gptReply"] = answer
        
        self.parseSql(parsedDict)

    def parseSql(self,parsedInfo):
        
        sql_template = "SELECT * FROM * WHERE * = '*';"
        sql = parsedInfo["gptReply"]
        matches = re.findall(sql_template, sql)
        parsedInfo["isSql"] = False
           
        if len(matches) > 0:
            parsedInfo["isSql"] = True
            parsedInfo["tableName"] = matches[0][1]
            parsedInfo["selectField"] = matches[0][0]
            parsedInfo["whereField"] = matches[0][2]
            parsedInfo["whereValue"] = matches[0][3]
       
    
    def request_generation(self,query):
        pass

    def multi_search(self,query):
        pass

    def refine_query(self,query):
        pass
    # await 只能在async 函数内部使用，如在非异步函数中调用异步函数，阻塞等待结果
    # res =ayncio.run(self.callGPT(text))
    async def callGPT(self,prompt, query): 
       
        # try:
        #     chat_completion = await self.client.chat.completions.create(
        #         messages=[
        #             {
        #                 "role": "user",
        #                 "content": prompt+query,
        #             }
        #         ],
        #         model="gpt-3.5-turbo",
        #         )
        # except OpenAIError as e:
        #     print(e)
        #     return None
        # answer = chat_completion.choices[0].message.content
        answer = "SELECT brand FROM computers WHERE name = '小新电脑';"
        return answer

if __name__ == '__main__':
    analyzer = QueryAnalyzer("sk-xxxxxxxxxxxxxx")
    query = "请对下面句子进行改写，要求用中文并且保持原意不变\n"
    resp = asyncio.run(analyzer.callGPT(query))
    print(resp)

