from pymilvus import MilvusClient

from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
    CollectionSchema, Function, FunctionType
)
from dataflow.utils.log import Logger
from contextlib import contextmanager
import yaml
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

# ------------------ 类型 ------------------
class SearchResult(BaseModel):
    id: str
    score: float
    fields: Dict[str, Any]

_logger = Logger('dataflow.utils.dbtools.milvus')

# milvus_client = MilvusClient(uri='http://milvus.ginghan.com:22000',token="root:Milvus",db_name="default")
class MilvusTools:
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)) 
#     参数	                                                含义
#     stop=stop_after_attempt(3)	                        最多重试 3 次（含首次）
#     wait=wait_exponential(multiplier=1, min=2, max=10)	每次等待时间 = min(max(2, 1 * 2ⁿ), 10)，单位秒；n 是已重试次数
#     举例序列	第 1 次异常后等 2 秒 → 第 2 次等 4 秒 → 第 3 次等 8 秒 → 达到 3 次仍失败就抛出最后一次异常
# 
#     需求	                                                写法
#     只在特定异常重试	                                    @retry(retry=retry_if_exception_type(ConnectionError))
#     重试前后加日志	                                    @retry(before_sleep=before_sleep_log(logger, logging.INFO))
#     返回 None 也重试	                                    @retry(retry=retry_if_result(lambda r: r is None))
#     无限重试	                                            stop=stop_never （慎用）
    def __init__(self, **kwargs):        
        self.__config__ = kwargs
        self.__client = MilvusClient(    
            uri = self.__config__['uri'] if 'uri' in self.__config__ else 'http://localhost:19530',
            user = self.__config__['user'] if 'user' in self.__config__ else '',
            password = self.__config__['password'] if 'password' in self.__config__ else '',
            db_name = self.__config__['db_name'] if 'db_name' in self.__config__ else '',
            token = self.__config__['token'] if 'token' in self.__config__ else '',
            timeout = self.__config__['timeout'] if 'timeout' in self.__config__ else None,
        )
                
        _logger.DEBUG(f"MilvusClient {self.__client}")
    
    def getConfig(self):
        return self.__config__
    
    def getClient(self)->MilvusClient:
        return self.__client
    
    # ---------- 集合 ----------
    def create_collection(
        self,
        name: str,
        dim: int,
        metric_type: str = "COSINE",
        tokenizer: str = "english",
        drop_if_exist: bool = False,
    ) -> None:
        """创建支持 稠密向量 + BM25 全文 的集合"""
        if drop_if_exist and self.__client.has_collection(name):
            self.__client.drop_collection(name)

        schema = self.__client.create_schema(auto_id=True)
        schema.add_field("pk", DataType.INT64, is_primary=True)
        schema.add_field("text", DataType.VARCHAR, max_length=65535, 
                         enable_analyzer=True, 
                         analyzer_params={"type": tokenizer})
        schema.add_field("dense_vec", DataType.FLOAT_VECTOR, dim=dim)
        # 1. 先声明稀疏向量字段（关键）
        schema.add_field("sparse_vec", DataType.SPARSE_FLOAT_VECTOR)  

        # BM25 函数
        schema.add_function(
            Function(
                name="text_bm25",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names=["sparse_vec"],
            )
        )

        idx = self.__client.prepare_index_params()
        idx.add_index("dense_vec", index_type="AUTOINDEX", metric_type=metric_type)
        idx.add_index("sparse_vec", index_type="AUTOINDEX", metric_type="BM25")
        self.__client.create_collection(name, schema=schema, index_params=idx)
        self.__client.load_collection(name)

    # ---------- 插入 ----------
    def insert(self, collection: str, data: List[Dict[str, Any]]) -> List[int]:
        return self.__client.insert(collection, data)["primary_keys"]

    # ---------- 稠密向量搜索 ----------
    def search_dense(
        self,
        collection: str,
        vec: List[float],
        top_k: int = 10,
        offset: int = 0,
        output_fields: Optional[List[str]] = None,
    ) -> List[SearchResult]:
        res = self.__client.search(
            collection,
            data=[vec],
            anns_field="dense_vec",
            limit=top_k,
            offset=offset,
            output_fields=output_fields or [],
        )[0]
        return [SearchResult(id=r["id"], score=r["score"], fields=r["entity"]) for r in res]

     # ---------- 全文检索 ----------
    def search_text(
        self,
        collection: str,
        query: str,
        top_k: int = 10,
        output_fields: Optional[List[str]] = None,
    ) -> List[SearchResult]:
        res = self.__client.search(
            collection,
            data=[query],
            anns_field="sparse_vec",
            limit=top_k,
            search_params={"metric_type": "BM25"},
            output_fields=output_fields or [],
        )[0]
        return [SearchResult(id=r["id"], score=r["score"], fields=r["entity"]) for r in res]

    
    # ---------- 混合检索（RRF 归并） ----------
    def hybrid_search(
        self,
        collection: str,
        vec: List[float],
        text: str,
        top_k: int = 10,
        rrf_k: int = 60,
    ) -> List[SearchResult]:
        req = [
            {"data": [vec], "anns_field": "dense_vec", "limit": top_k, "metric_type": "COSINE"},
            {"data": [text], "anns_field": "sparse_vec", "limit": top_k, "metric_type": "BM25"},
        ]
        res = self.__client.hybrid_search(
            collection, req, output_fields=[], rerank_strategy={"strategy": "rrf", "k": rrf_k}
        )[0]
        return [SearchResult(id=r["id"], score=r["score"], fields=r["entity"]) for r in res]
        
    # ---------- 删除 ----------
    def delete(self, collection: str, pks: List[int]) -> int:
        return self.__client.delete(collection, pks).delete_count
    
    # ---------- 释放 ----------
    def release(self, collection: str) -> None:
        self.__client.release_collection(collection)
        
    # ---------- 加载 ----------
    def load(self, collection: str) -> None:
        self.__client.load_collection(collection)




def initMilvusWithConfig(config)->MilvusTools:
    if config is None:
        DB_CONFIG = {}
    else:
        if hasattr(config, '__dict__'):
            DB_CONFIG = vars(config)
        else:
            if isinstance(config, dict):
                DB_CONFIG = dict(config)
            else:
                DB_CONFIG = config
                            
    _logger.DEBUG(f'数据库Milvus初始化 {DB_CONFIG}')
    
    dbtools = MilvusTools(**DB_CONFIG)
    
    db_name="default"
    
    if 'db_name' in DB_CONFIG:
        db_name = DB_CONFIG['db_name']
        
    data_meta = dbtools.getClient().describe_database( db_name = db_name)
    
    if data_meta is None:
        raise Exception(f'数据库Milvus不能访问 {DB_CONFIG}')
    else:
        _logger.INFO(f'{db_name}: {data_meta}')
    
    return dbtools

def initMilvusWithYaml(config_file='milvus.yaml')->MilvusTools:
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            DB_CONFIG = yaml.safe_load(f)['milvus']
    except Exception as e:
        _logger.ERROR('配置错误，使用默认配置', e)
        DB_CONFIG = {
            'uri': 'http://localhost:19530',
            'db_name': 'default'
        }
    
    return initMilvusWithConfig(DB_CONFIG)

    

    
    