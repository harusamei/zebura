from sentence_transformers import SentenceTransformer

class Embedding:
    def __init__(self, model_name='DMetaSoul/Dmeta-embedding'):
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
    #texts1 = ["胡子长得太快怎么办？", "在香港哪里买手表好"]
    #texts2 = ["胡子长得快怎么办？", "怎样使胡子不浓密！", "香港买手表哪里好", "在杭州手机到哪里买"]
    texts1=['products','产品']
    texts2=['product_name', '产品名', 'Item Name', 'Product Title', 'Merchandise Name', '商品名称', ' 产品名称', ' 产品型号', ' 商品型号']
    model = Embedding()
    embs1 = model.get_embedding(texts1)
    embs2 = model.get_embedding(texts2)
    embs3 = model.get_embedding(["在香港哪里买手表好"])
    print(embs1.shape, embs2.shape, embs3.shape)
    print(model.calc_similarity(embs1, embs2))

    scores = model.get_similar(texts1, texts2)
    print(scores)

