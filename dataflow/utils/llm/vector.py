from sentence_transformers import SentenceTransformer
from sentence_transformers import CrossEncoder
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import PyPDFLoader
from typing import List, Optional
from dataflow.utils.log import Logger
import threading
from functools import lru_cache  # noqa: F401
import os # noqa: F401
import numpy as np


_logger = Logger('dataflow.utils.llm.vector')


# os.environ['HF_ENDPOINT']='http://hf-mirror.com'
# os.environ['HF_HOME']='D:/HF_HOME'
# os.environ['TRANSFORMERS_CACHE'] ='D:/HF_HOME'

# ---------- 全局锁 & 缓存 ----------
_lock = threading.Lock()
_MODEL_CACHE: dict[str, SentenceTransformer] = {}
_MODEL_CACHE_2: dict[str, SentenceTransformer] = {}

def Get_sentence_transformer(name: str) -> SentenceTransformer:
    """
    线程安全的 SentenceTransformer 单例工厂
    :param name: 模型名（如 'google-bert/bert-base-chinese'）
    :return: 单例模型实例
    """
    if name in _MODEL_CACHE:               # 快速路径无锁
        return _MODEL_CACHE[name]

    with _lock:                            # 并发加载保护
        if name not in _MODEL_CACHE:       # 二次检查
            _MODEL_CACHE[name] = SentenceTransformer(name)
        return _MODEL_CACHE[name]
    
def Get_CrossEncoder(name: str, **args) -> CrossEncoder:
    """
    线程安全的 CrossEncoder 单例工厂
    :param name: 模型名（如 'BAAI/bge-reranker-base'）
    :return: 单例模型实例
    """
    _logger.DEBUG(f'Get_CrossEncoder[{name}] with {args}')
    if name in _MODEL_CACHE_2:               # 快速路径无锁
        return _MODEL_CACHE_2[name]

    with _lock:                            # 并发加载保护
        if name not in _MODEL_CACHE_2:       # 二次检查
            _MODEL_CACHE_2[name] = CrossEncoder(name, **args)
        return _MODEL_CACHE_2[name]    

def load_pdf_text(file_path:str)->List[any]:
    # loader = PyPDFLoader("./RAG/pdf/健康档案.pdf")
    loader = PyPDFLoader(file_path)
    docs = loader.load()    
    _logger.DEBUG(f'docs[{len(docs)}]={docs}')
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=400, chunk_overlap=80)
    chunks = text_splitter.split_documents(docs)
    _logger.DEBUG(f'chunks[{len(chunks)}]={chunks}')
    text_lines = [chunk.page_content for chunk in chunks]
    _logger.DEBUG(f'text_lines[{len(text_lines)}]={text_lines}')
    return text_lines

def emb_text(embedding_model:SentenceTransformer, text)->List:
    return embedding_model.encode([text], normalize_embeddings=True).tolist()[0]

def simliar_cosine(embedding_model:SentenceTransformer, query:str, doc:str)->float:    
    query_embedding = embedding_model.encode(query)
    doc_embeddings = embedding_model.encode([doc])
    
    _logger.DEBUG(f'query_embedding.shape={query_embedding.shape} doc_embeddings.shape={doc_embeddings.shape}')    
    scores = np.dot(doc_embeddings, query_embedding) / (np.linalg.norm(doc_embeddings, axis=1) * np.linalg.norm(query_embedding))
    return scores[0]

def crossencoder_token_set(c :CrossEncoder):
    c.tokenizer.pad_token = c.tokenizer.eos_token
    c.config.pad_token_id = c.tokenizer.eos_token_id
    
    # if crossEncoder.tokenizer.pad_token is None and crossEncoder.tokenizer.eos_token is not None:
    #     crossEncoder.tokenizer.pad_token = crossEncoder.tokenizer.eos_token        
    # if crossEncoder.tokenizer.pad_token_id is None and crossEncoder.tokenizer.eos_token_id is not None:
    #     crossEncoder.tokenizer.pad_token_id = crossEncoder.tokenizer.eos_token_id
    

def rerank(reranker_model:CrossEncoder, query:str, candidates:List[str],rerank_top:Optional[int]=10)->List:
    if rerank_top is None:
        rerank_top = len(candidates)
        
    scores = reranker_model.predict([(query, doc) for doc in candidates])
    ranked = sorted(zip(candidates, scores), key=lambda x: x[1], reverse=True)
    return ranked[:rerank_top]

def similar_ranker(reranker_model:CrossEncoder, src: str, dest:str)->float:
    return reranker_model.predict([(src, dest)])[0]


# d:\HF_HOME>set HF_ENDPOINT=https://alpha.hf-mirror.com/
# d:\HF_HOME>huggingface-cli download google-bert/bert-base-chinese  --local-dir ./

if __name__ == "__main__":
    # print(f"os.environ['HF_ENDPOINT']={os.environ['HF_ENDPOINT']} os.environ['HF_HOME']={os.environ['HF_HOME']}")
    
    file_path = 'E:/WORK/PROJECT/python/AI/DS/RAG/pdf/健康档案.pdf'
    
    texts = load_pdf_text(file_path)
    print(f'texts({len(texts)})={texts}')
    
    
    
    model_name = "google-bert/bert-base-chinese"
    # model_name = 'D:\\HF_HOME\\download\\models--google-bert--bert-base-chinese'
    t = Get_sentence_transformer(model_name)
    
    test_embedding = emb_text(t, texts[0])
    embedding_dim = len(test_embedding)
    print(f'======= {embedding_dim} test_embedding={test_embedding}')
    print(test_embedding[:10])
    
    # 我们的数据
    query = "什么是人工智能？"
    documents = [
        "人工智能是研究、开发用于模拟、延伸和扩展人的智能的理论、方法、技术及应用系统的一门新的技术科学。",
        "熊猫是一种生活在中国的珍稀动物，主要以竹子为食。"
    ]
    print("=== SentenceTransformer (双塔模型) ===")
    # 编码所有句子
    query_embedding = t.encode(query)
    print(query_embedding.shape)
    doc_embeddings = t.encode(documents)
    print(doc_embeddings.shape)
    # 计算余弦相似度
    scores = np.dot(doc_embeddings, query_embedding) / (np.linalg.norm(doc_embeddings, axis=1) * np.linalg.norm(query_embedding))
    for i, score in enumerate(scores):
        print(f"文档 {i+1} 相似度: {score:.4f}")
        
    for i, doc in enumerate(documents):    
        score = simliar_cosine(t, query, doc)
        print(f"文档 {i+1} 相似度: {score:.4f}")
    
    # model_name = "Qwen/Qwen3-Reranker-0.6B"
    # model_name = "BAAI/bge-reranker-base"
    model_name = "BAAI/bge-reranker-v2-m3"
    c = Get_CrossEncoder(model_name)
    
    # 2. 使用 CrossEncoder (交叉编码器) 计算相似度
    print("\n=== CrossEncoder (交叉编码器) ===")
    print(f"使用模型：{model_name}") 
    # 组合句子对
    pairs = [[query, doc] for doc in documents]
    # 直接得到分数
    scores = c.predict(pairs)
    for i, score in enumerate(scores):
        print(f"文档 {i+1} 相似度: {score:.4f}")
        
    for i, doc in enumerate(documents):
        score = similar_ranker(c, query, doc)
        print(f"文档 {i+1} 相似度: {score:.4f}")
        
    scores = rerank(c, query, documents)
    
    for i, score in enumerate(scores):
        print(f"文档 {i+1} {score[0]} 相似度: {score[1]:.4f}")
    
    reranker = Get_CrossEncoder(model_name,
                            max_length=8192)      
    # 2. 使用 CrossEncoder (交叉编码器) 计算相似度
    print("\n=== CrossEncoder (交叉编码器) ===")
    print(f"使用模型：{model_name}") 
      
    # reranker.eval()
    query = "今年中秋节是哪天？"
    docs = ["2025 年中秋节是 10 月 6 日。",
            "2025 年国庆节是 10 月 1 日。",
            "2025 年端午节是 6 月 2 日。"]
    
    scores = reranker.predict([(query, d) for d in docs])  
    sorted_docs = [(score, d) for score, d in sorted(zip(scores, docs), reverse=True)]
    print("重排结果：", sorted_docs)
    
    
    model_name = "BAAI/bge-reranker-base"
    
    c = Get_CrossEncoder(model_name,
                            max_length=8192)
    # 2. 使用 CrossEncoder (交叉编码器) 计算相似度
    print("\n=== CrossEncoder (交叉编码器) ===")
    print(f"使用模型：{model_name}") 
      
        # 同样先补 pad_token
    # c.tokenizer.pad_token = c.tokenizer.eos_token
    # c.config.pad_token_id = c.tokenizer.eos_token_id
    # c.eval()
    # crossencoder_token_set(c)
    
    scores = rerank(c, query, docs)
    for i, score in enumerate(scores):
        print(f"文档 {i+1} {score[0]} 相似度: {score[1]:.4f}")
        
    for i, doc in enumerate(docs):
        score = similar_ranker(c, query, doc)
        print(f"文档 {i+1} 相似度: {score:.4f}")
    