# Function: Cut text into sentences by punctuation marks.
# 英文切句使用nltk库，中文切句使用正则表达式。
# nltk支持很多种语言，但没有中文和日文，其它语言暂时也没用，所以只用了英文。
# nltk库需要下载punkt模块，下载路径可以指定，例如
# nltk.download('punkt', download_dir='C:\\something\\nltk_data')， 下载死慢
import nltk
from nltk.tokenize import sent_tokenize
import unicodedata
import re

class SentenceCutter:

    def __init__(self):
        self.end_punct = ['。', '！', '？','…','：']+ ['.', '!', '?',':']

        pair_punct = ['“”', '‘’', '【】', '（）']+ ['""', "''", '[]', '()', '{}']
        self.left_punct = [punct[0] for punct in pair_punct]
        self.right_punct = [punct[1] for punct in pair_punct]
    # 目前只支持中文和英文
    def cut_sentences(self, text, lang='zh'):
        if len(text) <= 1:
            return [text]
        # 
        if lang != 'zh':
            lang='english'
            return sent_tokenize(text, language=lang)

        paras = text.split('\n')
        regex = r'(?<=['+''.join(self.end_punct)+'])'
        all_sents = []
        for para in paras:
            sents = re.split(regex, para)
            all_sents.extend(sents)
        avg_len = len(text) / len(all_sents)
        
        new_sents = [all_sents[0]]
        for sent in all_sents[1:]:
            flag = False
            last_sent = new_sents[-1]
            if self.is_left_part(last_sent):
                flag = True
            if len(sent) <= 1:
                flag = True
            # 太长也不能合并
            if flag and len(last_sent)< 2*avg_len:
                new_sents[-1] += sent
            else:
                new_sents.append(sent)

        return new_sents
    
    def is_left_part(self, text):
        if len(text) == 0:
            return False
        
        left_count = 0
        for char in text:
            if char in self.left_punct:
                left_count+=1
            if char in self.right_punct:
                left_count-=1

        if left_count > 0:
            return True    
        return False
    
    @staticmethod
    def to_full_width(text):
        """将半角字符转换为全角字符"""
        return ''.join([unicodedata.normalize('NFKC', char) \
                        if unicodedata.east_asian_width(char) != 'Na' else char for char in text])
    @staticmethod
    def to_half_width(text):
        """将全角字符转换为半角字符"""
        return ''.join([unicodedata.normalize('NFKD', char) \
                        if unicodedata.east_asian_width(char) != 'Na' else char for char in text])
    @staticmethod
    def is_full_width(char):
        """判断字符是否为全角字符"""
        return unicodedata.east_asian_width(char) != 'Na'
    
# Example usage
if __name__ == '__main__':
    sc = SentenceCutter()
    print(sc.left_punct)
    print(sc.to_full_width(''.join(sc.left_punct)))
    txt = '【请问是否有叫小新的产品？有的。等等…[]{}\n[有的。等等。。。[]{}\n\nthis is book.'
    print(sc.cut_sentences(txt))
