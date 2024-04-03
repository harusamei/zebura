from rouge import Rouge
from nltk.translate.chrf_score import chrf_precision_recall_fscore_support
#from evaluate import load
from nltk.translate import meteor
from nltk.translate.bleu_score import sentence_bleu
from nltk import word_tokenize
import re
#from bleurt import score


def getRouge(ref_sent, gen_sent,
                  rouge_n='l'):
    """
    基于word级别的召回率评估，使用召回率直接作为分数
    :param reference_sentence: 传入一个str字符串
    :param generated_sentence: 传入一个str字符串
    :param rouge_n: 可选 '1','2','l',设置算法中的 n_gram=？ ,默认使用 rouge-l 的 lcs最长公共子序列的算法
    :param lang: 设置语言， 可选'en','zh'
    :return: 返回一个分数

    :demo
        ref_sent = "我是一个男孩"
        gen_sent = "你是一个男孩"
        print(getRouge(ref_sent, gen_sent))
    """
    rouge = Rouge()
    return rouge.get_scores(ref_sent,gen_sent)


def getChrf(ref_sent, gen_sent,
                 n_gram=3,
                 beta=2):
    """
    基于字符级别的召回率和精准率，beta控制的是召回率和精准率对最后分数比重的影响，现已根据论文设置n_gram=3，beta=2 为最优,

    :param reference_sentence: 传入一个str字符串
    :param generated_sentence: 传入一个str字符串
    :param n_gram: 默认为3
    :param beta: 用于调节 recall和 precise 作用于分数的权重
    :return: 返回一个分数

    :demo
        reference_sentence = '你是小猫'
        generated_sentence = '你是小狗'
        result =get_chrf_precision_recall_fscore_support(reference_sentence,generated_sentence)
        print(result)
    """
    precision, recall, fscore, tp = chrf_precision_recall_fscore_support(
            ref_sent, gen_sent, n=n_gram, epsilon=0., beta=beta
        )
    return fscore

def dealData(sent, lang='en'):  # 简单切分
    if lang == 'zh':
        sent = " ".join(sent)
    if lang == 'en':
        sent = re.sub(r"([a-z])([A-Z])", r"\1 \2", sent)
        sent = sent.replace('_',' ')
        sent = sent.sub(' +', ' ',sent)
        sent = sent.lower()
    return sent
   
def getBleu(ref_sent,gen_sent, weights=(1,)):
    '''
    计算Bleu分数
    :param references: 参考译文，字符串列表
    :param candidates: 候选译文，字符串列表
    :param weights: n-gram所占权重。如(0.5, 0.5)表示1-gram和2-gram的w均为0.5，(1,)表示1-gram的w为1，（0, 1)表示2-gram的w为1。
    :return: 两个句子的Bleu分数

    :demo
        references = 'This is a easy test'
        candidates = 'This is a test'
        print('Bleu:', getBleu(references, candidates))    # 默认weights=(1,)
    '''
    return sentence_bleu(ref_sent,gen_sent, weights)
      

def getMeteor(ref_sent, gen_sent, alpha=0.9, beta=3.0,
              gamma=0.5):  # gamma=0可使两个一样的句子得分为1
    '''
    计算meteor分数
    :param references: 参考译文，字符串列表
    :param candidates: 候选译文，字符串列表
    :param language: 语言，可选'en'，'zh'。已弃用，在dealData中判定语言。
    :param alpha: 参数
    :param beta: 参数
    :param gamma: 参数
    :return: 两个句子间的Bleu分数

    :demo
        references = ['This is a easy test', 'Today is a good day', 'Time is up']
        candidates = ['This is a test', 'Today is a bad day', 'Time is up']
        print('Meteor gamma=0.5:', getMeteor(references, candidates))    # 对于两个一样的句子，默认情况下gamma!=0，Meteor得分接近但不为1
        print('Meteor gamma=0:', getMeteor(references, candidates), gamma=0)    # gamma=0
    '''
    #return meteor_score([ref_sent], gen_sent, alpha=alpha, beta=beta, gamma=gamma)
    
    round(meteor([word_tokenize('The cat sat on the mat')],word_tokenize('The cat was sat on the mat')), 4)
   


def getScores(metrics: list, ref_sent, gen_sent):
    # ALLmetrics = ['rouge','chrf','bertscore',]
    result = {}
    for metric in metrics:
        if metric == 'rouge':
            result['rouge'] = getRouge(ref_sent, gen_sent)
        elif metric == 'chrf':
            result['chrf'] = getChrf(ref_sent, gen_sent)
        elif metric == 'bleu':
            result['bleu'] = getBleu(ref_sent, gen_sent)
        elif metric == 'meteor':
            result['meteor'] = getMeteor(ref_sent, gen_sent)
        else:
            raise Exception('no such metric like {}'.format(metric))
    return result

# examples usage
if __name__ == '__main__':
    refSents = ['Product Name', '产品库', 'Product Name', 'Product Name']
    candSents = ['product name', '产品表', 'Product Title','Merchandise Name']
    score = getScores(['rouge', 'chrf','meteor'], 'Product Name', 'product name')
    print(score)
    