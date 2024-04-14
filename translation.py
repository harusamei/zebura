from nltk.translate import IBMModel1
from nltk.translate.api import AlignedSent

# 准备数据
src_sentences = [['klein', 'ist', 'das', 'Haus'], ['das', 'Haus', 'ist', 'grün'], ['ich', 'liebe', 'dieses', 'Haus']]
tgt_sentences = [['the', 'house', 'is', 'small'], ['the', 'house', 'is', 'green'], ['I', 'love', 'this', 'house']]

# 创建AlignedSent对象
aligned_sents = [AlignedSent(src, tgt) for src, tgt in zip(src_sentences, tgt_sentences)]

# 训练模型
ibm1 = IBMModel1(aligned_sents, 5)

# 翻译
src_sentence = ['das', 'Haus']
translation = [max(ibm1.translation_table[word], key=ibm1.translation_table[word].get) for word in src_sentence]
print(translation)  # 输出: "the house"