from sentence_transformers import SentenceTransformer
import time
class Embedding:
    # 这个是北京数元灵科技有限公司开源的语义向量模型，在中文 STS上当时榜单TOP1
    # 首次使用通过默认model_name安装在C:\Users\<Your Username>\.cache
    # 智源的BGE模型在https://www.modelscope.cn 有，不需要翻墙
    # dmeta, bge
    def __init__(self, model_name='dmeta'):
        if model_name == 'dmeta':
            model_name = 'E:/zebura/transformer/DMetaSoul-embedding'    # 这里是你的模型路径
        else:
            model_name = 'E:/zebura/transformer/bge-base-zh-v1.5'
        self.model = SentenceTransformer(model_name)

    def get_embedding(self, texts):
        embs = self.model.encode(texts, normalize_embeddings=True)
        return embs
    
    @staticmethod
    def calc_similarity(emb1, emb2):
        return emb1 @ emb2.T
    
    def get_similar(self, texts1, texts2):
        embs1 = self.get_embedding(texts1)
        embs2 = self.get_embedding(texts2)
        similarity = self.calc_similarity(embs1, embs2)
        scores = []
        for i in range(len(embs1)):
            scores.append([])
            for j in range(len(embs2)):
                scores[-1].append([j, similarity[i][j]])
            scores[-1] = sorted(scores[-1], key=lambda x:x[1], reverse=True)
            print(f"查询文本：{texts1[i]}")
            similar_loc = scores[i][0][0]
            similar_sent= texts2[similar_loc]           
            print(f"相似文本：{similar_sent}，打分：{scores[i][0][1]}")

        return scores
        
    
# Example usage
if __name__ == '__main__':
    texts1 = ["鼠标多少钱？", "新上市的产品有哪些","what the difference between desktop and laptop?"]
    texts2 = ["鼠标属于哪个分类","远程数据恢复属于哪个分类","计算机属于哪个分类",
              "新上市的产品有哪些","价格在1000~15000的电脑有哪些","价格低于50的鼠标有哪些","台式机与笔记本有什么区别"]
    
    start_time = time.time()
    model = Embedding()
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")
    embs1 = model.get_embedding(texts1)
    embs2 = model.get_embedding(texts2)
    embs3 = model.get_embedding(["what the difference between desktop and laptop?"])
    print(embs1.shape, embs2.shape, embs3.shape)
    print(model.calc_similarity(embs1, embs2))

    scores = model.get_similar(texts1, texts2)
    print(scores)

