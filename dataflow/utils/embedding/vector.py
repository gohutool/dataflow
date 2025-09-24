from sentence_transformers import SentenceTransformer
from sentence_transformers import CrossEncoder
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import PyPDFLoader
from typing import List
from dataflow.utils.log import Logger
import threading
from functools import lru_cache

_logger = Logger('utils.embedding.vector')

# ---------- 全局锁 & 缓存 ----------
_lock = threading.Lock()
_MODEL_CACHE: dict[str, SentenceTransformer] = {}


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

def emb_text(embedding_model:SentenceTransformer, text):
    return embedding_model.encode([text], normalize_embeddings=True).tolist()[0]

def rerank(reranker_model:CrossEncoder, query:str, candidates:List[str],rerank_top:int=10)->List:
    scores = reranker_model.predict([(query, doc) for doc in candidates])
    ranked = sorted(zip(candidates, scores), key=lambda x: x[1], reverse=True)
    return ranked[:rerank_top]


if __name__ == "__main__":
    file_path = 'E:/WORK/PROJECT/python/AI/DS/RAG/pdf/健康档案.pdf'
    texts = load_pdf_text(file_path)
    print(f'texts({len(texts)})={texts}')
    
    model_name = "google-bert/bert-base-chinese"
    t = Get_sentence_transformer(model_name)
    
    test_embedding = emb_text(t, texts[0])
    embedding_dim = len(test_embedding)
    print(f'======= {embedding_dim} test_embedding={test_embedding}')
    print(test_embedding[:10])
    