from pymilvus import MilvusClient
from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
)
from dataflow.utils.log import Logger
from contextlib import contextmanager
import yaml


_logger = Logger('utils.dbtools.milvus')

# milvus_client = MilvusClient(uri='http://milvus.ginghan.com:22000',token="root:Milvus",db_name="default")
class MilvusTools:
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
        
        # self.__client =  MilvusClient('http://milvus.ginghan.com:22000')
        
        _logger.DEBUG(f"MilvusClient {self.__client}")
    
    def getConfig(self):
        return self.__config__
    
    def getClient(self)->MilvusClient:
        return self.__client
    
    # @contextmanager
    # def connect_database(self):
    #     # conn = pymysql.connect(**DB_CONFIG)
    #     conn = self.Connect_Mysql(**self.__config__)        
    #     try:
    #         yield conn
    #     finally:
    #         self.closeConnection(conn)


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

    

    
    