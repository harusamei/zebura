# 用jieba 的paddle模式分词
# 需要 pip install paddlepaddle
import jieba

# 设置jieba分词模式
jieba.enable_paddle()  # 启用paddle模式，更准确的切分

# paddle模式分词
paragraph = "今天天气不错。我打算出去散步，然后去购物。"
sentences = jieba.cut(paragraph, use_paddle=True, cut_all=False)

# 普通模式分词
sentences2 = jieba.cut(paragraph,cut_all=False)
# 打印切分后的句子
for sentence in sentences:
    print(sentence)

for sentence in sentences2:
    print(sentence)
