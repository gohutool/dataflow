import pymysql
from contextlib import contextmanager
from dataflow.utils.log import Logger
from dbutils.pooled_db import PooledDB
from dataflow.utils.utils import PageResult
import yaml

_logger = Logger('dataflow.utils.dbtools.mysql')

# DB_CONFIG = {
#     'host': '192.168.18.145',
#     'port': 3306,
#     'user': 'stock_agent',
#     'password': '1qaz2wsx',
#     'db': 'stock_agent',
#     'charset': 'utf8',
#     'autocommit': True,
#     'cursorclass': pymysql.cursors.DictCursor
# }


# DB_CONFIG = {
#         # 'host': '192.168.18.145',
#         # 'port': 3306,
#         'host': 'localhost',
#         'port': 60306,
#         'user': 'stock_agent',
#         'password': '1qaz2wsx',
#         'db': 'stock_agent',
#         'charset': 'utf8',
#         'autocommit': True,
#         'cursorclass': pymysql.cursors.DictCursor
#     }

class MysqlTools:
    def __init__(self, **kwargs):        
        self.__config__ = kwargs
        self.__dbpool = PooledDB(
            creator=pymysql,       # 使用 PyMySQL
            maxconnections=self.__config__['maxconnections'] if 'maxconnections' in self.__config__ else 20,     # 最大并发连接
            mincached=self.__config__['mincached'] if 'mincached' in self.__config__ else 2 ,           # 启动时空闲连接数
            maxcached=self.__config__['maxcached'] if 'maxcached' in self.__config__ else 10 ,          # 最大空闲连接数
            blocking=True,         # 连接用完是否阻塞等待
            ping=4,                # 每次取连接时自动 ping MySQL（防止超时）
            host=self.__config__['host'],
            port=self.__config__['port'],
            user=self.__config__['user'],
            password=self.__config__['password'],
            autocommit=self.__config__['autocommi'] if 'autocommi' in self.__config__ else True,
            database=self.__config__['db'] if 'db' in self.__config__ else self.__config__['database'],
            charset=self.__config__['charset'] if 'charset' in self.__config__ else 'utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        _logger.DEBUG(f"Connect {self.__dbpool}")
    
    def getConfig(self):
        return self.__config__
    
    @contextmanager
    def connect_database(self):
        # conn = pymysql.connect(**DB_CONFIG)
        conn = self.Connect_Mysql(**self.__config__)        
        try:
            yield conn
        finally:
            self.closeConnection(conn)

    def Connect_Mysql(self, **kwargs):
        try:
            # conn =  pymysql.connect(**kwargs)
            conn = self.__dbpool.connection()
            _logger.DEBUG(f"Connect {conn}")
        except Exception as e:
            raise e
        return conn
    
    # def getConnect(host, user, password, database):
    #     # 连接数据库
    #     connection = pymysql.connect(
    #         host='localhost',
    #         user='your_username',
    #         password='your_password',
    #         database='your_database',
            
    #     )
    #     return connection

    def queryMany(self, sql, params=None):
        _logger.DEBUG(f"[SQL]:{sql}")
        _logger.DEBUG(f"[Parameter]:{params}")
        try:
            with self.connect_database() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)  # 参数为元组
                    # 4. 获取结果
                    results = cursor.fetchall()  # 获取所有行
                    return results
        except Exception as e:
            _logger.ERROR("[Exception]", e)
            raise e

    def queryOne(self, sql, params=None):
        _logger.DEBUG(f"[SQL]:{sql}")
        _logger.DEBUG(f"[Parameter]:{params}")
        try:
            with self.connect_database() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)  # 参数为元组
                    # 4. 获取结果
                    result = cursor.fetchone()  # 获取行
                    return result
        except Exception as e:
            _logger.ERROR("[Exception]", e)
            raise e

    def queryCount(self, sql, params=None):
        result = self.queryOne(f'select count(1) cnt from ( {sql} ) a', params)  # 获取行
        return result['cnt']
            
    def queryPage(self, sql, params=None, page=1, pagesize=10) -> PageResult:
        total = self.queryCount(sql, params)
        if pagesize <= 0:
            list = self.queryMany(sql, params)
            return PageResult(total, pagesize, 1, 1 if total>0 else 0, list)            
        else:
            if page <= 0:
                page = 1
            if total <= 0:
                return PageResult(total, pagesize, 1, (total + pagesize - 1)//pagesize, None)
            else:
                offset = (page - 1) * pagesize
                if params is None:
                    params = (offset, pagesize)
                else:
                    params = params + (offset, pagesize)
                list = self.queryMany(sql + ' limit %s, %s', params)
                return PageResult(total, pagesize, 1, (total + pagesize - 1)//pagesize, list)

    def update(self, sql, params=None, commit=True):
        _logger.DEBUG(f"[SQL]:{sql}")
        _logger.DEBUG(f"[Parameter]:{params}")
        with self.connect_database() as connection:
            with connection.cursor() as cursor:
                try:
                    results = cursor.execute(sql, params)  # 参数为元组
                    if commit :
                        connection.commit()
                    return results
                except Exception as e:
                    connection.rollback()
                    _logger.ERROR("[Exception]", e)
                    raise e

    def insert(self, sql, params=None, commit=True):
        self.update(sql, params, commit)

    def delete(self, sql, params=None, commit=True):
        self.update(sql, params, commit)

    def batch(self, sql, paramsList=None, batchsize:int=100, commit=True):
        _logger.DEBUG(f"[SQL]:{sql}")
        _logger.DEBUG(f"[Parameters]:{paramsList}")
        results = 0
        
        if paramsList is None or len(paramsList)==0:
            return 0
        
        with self.connect_database() as connection:
            with connection.cursor() as cursor:
                try:
                    datas = []
                    for params in paramsList:                                            
                        datas.append(params)
                        if len(datas) >= batchsize:
                            count = cursor.executemany(sql, datas)  # 参数为元组    
                            results += count
                            _logger.DEBUG(f'批处理执行{len(datas)}条记录，更新数据{count}')                            
                            if commit :
                                # connection.commit()
                                self.commit(connection)
                                
                            datas.clear()
                    if len(datas) > 0:
                        count = cursor.executemany(sql, datas)  # 参数为元组    
                        results += count
                        _logger.DEBUG(f'批处理执行{len(datas)}条记录，更新数据{count}')                        
                        if commit :
                            # connection.commit()
                            self.commit(connection)
                        
                    return results
                except Exception as e:
                    # connection.rollback()
                    self.rollback(connection)
                    _logger.ERROR("[Exception]", e)
                    raise e

    def commit(self, connection):
        _logger.DEBUG("Commit")
        connection.commit()  # 提交事务

    def rollback(self, connection):
        _logger.DEBUG("Rollback")
        connection.rollback()  # 提交事务
        
    def closeConnection(self, connection):
        _logger.DEBUG(f"Close {connection}")
        connection.close()


def initMysqlWithConfig(config)->MysqlTools:
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
                            
    DB_CONFIG['cursorclass'] = pymysql.cursors.DictCursor
    
    _logger.DEBUG(f'数据库初始化 {DB_CONFIG}')
    
    dbtools = MysqlTools(**DB_CONFIG)
    
    if 'test' in DB_CONFIG:
        test = dbtools.queryOne(DB_CONFIG['test'])
    else:
        test = dbtools.queryOne('select 1')
        
    if test is None:
        raise Exception(f'数据库不能访问 {DB_CONFIG}')
    
    return dbtools

def initMysqlWithYaml(config_file='mysql.yaml')->MysqlTools:
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            DB_CONFIG = yaml.safe_load(f)['db']
    except Exception as e:
        _logger.ERROR('配置错误，使用默认配置', e)
        DB_CONFIG = {
            'host': '192.168.18.145',
            'port': 3306,
            # 'host': 'localhost',
            # 'port': 60306,
            'user': 'stock_agent',
            'password': '1qaz2wsx',
            'db': 'stock_agent',
            'charset': 'utf8',
            'autocommit': True
        }
    DB_CONFIG['cursorclass'] = pymysql.cursors.DictCursor
    return initMysqlWithConfig(DB_CONFIG)


if __name__ == "__main__":
    mt : MysqlTools = initMysqlWithConfig({      
        'host': 'localhost',
        'port': 61306,
        'user': 'stock_agent',
        'password': '1qaz2wsx',
        'db': 'stock_agent',
        'charset': 'utf8',
        'autocommit': True,
        'maxconnections': 20,
        'mincached': 3,
        'maxcached': 10
    })
    
    rtn = mt.queryMany('select * from dataflow_test.sa_security_realtime_daily where 1=1 AND tradedate in %(p_1759450992799135563776)s limit 10',{
        'p_1759450992799135563776':['2025-01-05','2025-01-06','2025-09-30']
    })
    print(f'Result={rtn}')
    
    rtn = mt.queryMany('select * from dataflow_test.sa_security_realtime_daily where 1=1 AND tradedate in %(one)s limit 10', {
        'one':['2025-01-05','2025-01-06']
    })
    print(f'Result={rtn}')
    


