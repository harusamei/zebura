# 解析LLM输出的结果，提取最终信息
# taskname须与prompt.txt中的taskname一致
# 解析方法遵循prompt中output格式要求
# NOTE： prompt.txt 中taskname, output格式如有修改，需同步修改此文件
###########################################
import re
class AnsExtractor:

    def __init__(self):
        self.result = {'status': 'succ', 'msg': ''}
        # "rewrite","nl2sql","sql_revise","term_expansion","db2nl","db2sql"
        self.tasks = {
            'term_expansion': self.parse_expansion1,
            'nl2sql': self.parse_sql,
            'sql_revise': self.parse_revised_sql,
            'pattern': self.parse_llm_output
        }
        
    def output_extr(self, taskname, output):
        if self.tasks.get(taskname) is not None:
            return self.tasks[taskname](output)
        else:
            return None

    def parse_revised_sql(self, output) -> dict:

        result = self.result

        patn = "```sql\n(.*?)\n```"
        matches = re.findall(patn, output, re.DOTALL | re.IGNORECASE)
        if matches:
            result['msg'] = matches[0]
        else:
            result['status'] = 'failed'
            result['msg'] = 'can not match pattern'
        
        return result
    
    def parse_expansion1(self, llm_answer) -> dict:
        result = self.result
        if 'ERR' in llm_answer:
            result['status'] = 'failed'
            result['msg'] = None
            return result
        
        data = self.parse_table(llm_answer)
        new_terms = {}
        for row in data[1:]:
            tList = list(set(row[1].split(',')))
            tList = [t.strip() for t in tList]
            new_terms[row[0]] = tList
        result['msg'] = new_terms
        return result
      
    # 解析term_expansion from LLM's answer
    def parse_expansion(self, llm_answer) -> dict:

        result = self.result
        if 'ERR' in llm_answer:
            result['status'] = 'failed'
            result['msg'] = None
            return result

        tlist = llm_answer.split('\n')
        tem_dic = {}
        # LLM 存在格式问题，## 代表非对应到keyword的部分
        kword = '##'
        tem_dic[kword] = ''
        for temstr in tlist:
            if '[Keyword:' in temstr:
                kword = temstr.split(':')[1]
                kword = re.sub(r'\s*]\s*', '', kword).strip()
                tem_dic[kword] = ''
            else:
                temstr = re.sub(r'-.*:', '', temstr).strip()
                tem_dic[kword] += temstr + '\n'
        del tem_dic['##']

        new_terms = {}
        for k, v in tem_dic.items():
            v = v.replace('OR', '\n')
            v = v.replace('(', '\n')
            v = v.replace(')', '\n')
            v = re.sub('[ ]+', ' ', v)
            v = re.sub(r'(\s*\n\s*)+', '\n', v)
            v = v.strip()
            new_terms[k] = list(set(v.split('\n')))
        result['msg'] = new_terms
        return result
    
    # Extract the SQL code from the LLM result
    # 提取SQL代码, 提取sql 全部小写
    def parse_sql(self, output: str) -> dict:
       
        result = self.result
        
        if output.lower().startswith("```sql"):
            code_pa = "```sql\n(.*?)\n```"  # 标准code输出
        elif 'select' in output.lower():
            output = re.sub('\n|\t', ' ', output)
            output = re.sub(' +', ' ', output)
            code_pa = "(select.*?from[^;]+;)"  # 不一定有where
        else:
            print("ERR: no sql found in result")
            result['status'] = 'failed'
            result['msg'] = "ERR: NOSQL"
            return result
        matches = re.findall(code_pa, output, re.DOTALL | re.IGNORECASE)
        if len(matches) == 0:
            print("ERR: no sql found in result")
            result['status'] = 'failed'
            result['msg'] = "ERR: NOSQL"
        else:
            result['msg']=matches[0]
        
        return result
    
    def parse_llm_output(self,output):
        # Define regular expressions to extract relevant information
        pattern1 = r"Pattern 1: (.+)"
        pattern2 = r"Pattern 2: (.+)"
        # Add more patterns as needed

        # Extract information using regular expressions
        match1 = re.search(pattern1, output)
        match2 = re.search(pattern2, output)
        # Add more matches as needed

        # Process the extracted information
        if match1:
            pattern1_result = match1.group(1)
            # Process pattern 1 result

        if match2:
            pattern2_result = match2.group(1)
            # Process pattern 2 result

        # Return the processed information
        return pattern1_result, pattern2_result
    
    #解析ascii表格为matric
    """ 
    | Keyword    | Label        |
    |------------|--------------|
    | apple      | Brand        |
    | mouse      | Product_name |
    | innovation | Business     | """
    # [['Keyword', 'Label'], ['apple', 'Brand'], ['mouse', 'Product_name'], ['innovation', 'Business']]
    @staticmethod
    def parse_table(table):
        # 去掉表格的边框和分隔线
        lines = table.strip().split('\n')
        lines = [line for line in lines if not re.match(r'^\|.*[-]+', line)]
        
        # 解析每一行
        data = []
        for line in lines:
            # 去掉行首和行尾的竖线，并按竖线分割
            row = [cell.strip() for cell in line.strip('|').split('|')]
            data.append(row)
        
        return data
    
if __name__ == "__main__":
    ans_extr = AnsExtractor()
    llm_output = "Pattern 1: This is the first pattern. Pattern 2: This is the second pattern."
    result1, result2 = ans_extr.output_extr('pattern',llm_output)
    print(result1)
    print(result2)