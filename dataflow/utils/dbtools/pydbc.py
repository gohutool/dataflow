from sqlalchemy import create_engine, Engine, text, event, make_url, inspect
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from dataflow.utils.log import Logger
from dataflow.utils.utils import PageResult
from dataflow.utils.utils import json_to_str
from typing import Any, Dict
from cachetools import TTLCache

_logger = Logger('utils.dbtools.pydbc')


def _setup_monitoring(engine:Engine):
    """设置连接池监控"""        
    # 连接创建和关闭    
    @event.listens_for(engine, "connect")
    def on_connect(dbapi_conn, connection_record):
        _logger.DEBUG(f"🆕 CONNECT - 新建连接: {id(dbapi_conn)}")
    
    @event.listens_for(engine, "close")
    def on_close(dbapi_conn, connection_record):
        _logger.DEBUG(f"❌ CLOSE - 关闭连接: {id(dbapi_conn)}")
    
    # 连接取出和放回
    @event.listens_for(engine, "checkout")
    def on_checkout(dbapi_conn, connection_record, connection_proxy):
        _logger.DEBUG(f"📥 CHECKOUT - 取出连接: {id(dbapi_conn)}")
    
    @event.listens_for(engine, "checkin")
    def on_checkin(dbapi_conn, connection_record):
        _logger.DEBUG(f"📤 CHECKIN - 放回连接: {id(dbapi_conn)}")
    
    # 连接验证和失效
    @event.listens_for(engine, "checkout")
    def on_checkout_validate(dbapi_conn, connection_record, connection_proxy):
        _logger.DEBUG(f"🔍 VALIDATE - 验证连接: {id(dbapi_conn)}")
    
    @event.listens_for(engine, "invalidate")
    def on_invalidate(dbapi_conn, connection_record, exception):
        _logger.DEBUG(f"🚫 INVALIDATE - 连接失效: {id(dbapi_conn)}, 错误: {exception}")
    
    # 连接池调整
    @event.listens_for(engine, "first_connect")
    def on_first_connect(dbapi_conn, connection_record):
        _logger.DEBUG(f"🌟 FIRST_CONNECT - 首次连接: {id(dbapi_conn)}")
    
    @event.listens_for(engine, "soft_invalidate")
    def on_soft_invalidate(dbapi_conn, connection_record, exception):
        _logger.DEBUG(f"⚠️ SOFT_INVALIDATE - 软失效: {id(dbapi_conn)}")

class PydbcTools:
    def __init__(self, **kwargs):
        self._table_cache = TTLCache(maxsize=128, ttl=60)
        self.__config__ = kwargs
        self.__url = make_url(
                self.__config__['url']
            )
        if 'username' in self.__config__:
            self.__url = self.__url.set(username=self.__config__['username'])
        if 'password' in self.__config__:
            self.__url = self.__url.set(password=self.__config__['password'])
        
        self.engine = create_engine(
            url=self.__url,
            poolclass=QueuePool,            
            pool_size=self.__config__['pool_size'] if 'pool_size' in self.__config__ else 20,      # 常驻连接数
            max_overflow=self.__config__['max_overflow'] if 'max_overflow' in self.__config__ else 10 ,          # 超出池后可再建多少连接
            pool_timeout=self.__config__['pool_timeout'] if 'pool_timeout' in self.__config__ else 30 ,          # 获取连接最大等待秒数
            pool_recycle=self.__config__['pool_recycle'] if 'pool_recycle' in self.__config__ else 3600 ,        # 连接回收时间（防 MySQL 8h 断开）
            pool_pre_ping=self.__config__['ping'] if 'pool_pre_ping' in self.__config__ else True ,       # 使用前 ping，防“连接已死”
            
            future=True            
        )
        _setup_monitoring(self.engine)        
        # _logger.INFO(f'创建数据库连接:{self.__url}')               
        if 'test' in self.__config__:
            test = self.__config__['test']
            if test.strip() != '':
                self.queryOne(test)
        _logger.INFO(f'创建数据库连接:{self.__url}成功')
    
    def getConfig(self):
        return self.__config__
    
    def getEnginee(self)->Engine:
        return self.engine
    
    # @overload
    # def queryMany(self, sql, params:tuple):
    #     _logger.DEBUG(f"[SQL]:{sql}")
    #     _logger.DEBUG(f"[Parameter]:{params}")
    #     try:
    #         with self.engine.begin() as connection:
    #             results = connection.execute(text(sql), params).fetchall()  # 参数为元组
    #             return results                    
    #     except Exception as e:
    #         _logger.ERROR("[Exception]", e)
    #         raise e
    
    def queryMany(self, sql, params:dict=None):
        _logger.DEBUG(f"[SQL]:{sql}")
        _logger.DEBUG(f"[Parameter]:{params}")
        try:
            with self.engine.begin() as connection:
                results = connection.execute(text(sql), params).fetchall()   # 参数为Dict    
                rtn = []
                for one in results:
                    rtn.append(one._asdict())                
                return rtn
        except Exception as e:
            _logger.ERROR("[Exception]", e)
            raise e

    def queryOne(self, sql, params:dict=None)->dict:
        _logger.DEBUG(f"[SQL]:{sql}")
        _logger.DEBUG(f"[Parameter]:{params}")
        try:
            with self.engine.begin() as connection:
                results = connection.execute(text(sql), params).fetchone()   # 参数为Dict                    
                return results._asdict()
        except Exception as e:
            _logger.ERROR("[Exception]", e)
            raise e

    def queryCount(self, sql, params:dict=None)->int:
        result = self.queryOne(f'select count(1) cnt from ( {sql} ) a', params)  # 获取行
        return result['cnt']
            
    def queryPage(self, sql, params:dict=None, page=1, pagesize=10) -> PageResult:
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
                    params = {}                    
                
                params['_offset_'] = offset
                params['_pagesize_'] = pagesize
                sql_wrap = sql + ' LIMIT :_pagesize_  OFFSET :_offset_ '
                if self.engine.dialect.name == "postgresql": # ("postgresql", "mysql", "sqlite", "clickhouse", "openGauss", "dm", "kingbase")
                    sql_wrap = sql + ' LIMIT :_pagesize_  OFFSET :_offset_ '
                elif self.engine.dialect.name == "mysql":
                    # sql_wrap = sql + ' LIMIT :_pagesize_  OFFSET :_offset_ '
                    sql_wrap = sql + ' LIMIT :_offset_, :_pagesize_  '
                elif self.engine.dialect.name == "oracle":
                    sql_wrap = f'SELECT * FROM (SELECT t.*, ROWNUM rn FROM ({sql}) t) WHERE rn BETWEEN :_offset_ + 1 AND :_offset_ + :_pagesize_ '
                elif self.engine.dialect.name == "mssql":
                    sql_wrap = sql + ' OFFSET :_offset_ ROWS FETCH NEXT :_pagesize_ ROWS ONLY'
                elif self.engine.dialect.name == "hive":
                    sql_wrap = f'SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER (ORDER BY 1) AS rn FROM ({sql}) t) WHERE rn BETWEEN :_offset_ + 1 AND :_offset_ + :_pagesize_ '                                        
                    
                
                list = self.queryMany(sql_wrap, params)
                        
                return PageResult(total, pagesize, 1, (total + pagesize - 1)//pagesize, list)

    def update(self, sql, params=None, commit=True):
        _logger.DEBUG(f"[SQL]:{sql}")
        _logger.DEBUG(f"[Parameter]:{params}")
        with self.engine.begin() as connection:
            try:
                results = connection.execute(text(sql), params)
                if commit:
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
        
    def insertT(self, tablename:str, params=None, commit=True):
        pass

    def updateT(self, tablename:str, params=None, commit=True):
        pass
    
    
    def get_table_info(self, table_name: str, **kwargs) -> Dict[str, Any]:
        """获取表的字段信息"""
        db_type = self.engine.dialect.name
        cache_key = f"{db_type}.{table_name}"
        
        if cache_key in self._table_cache:
            return self.table_cache[cache_key]
        
        engine = None
        try:
            if db_type in self.engines:
                engine = self.engines[db_type]
            else:
                engine = self.connect(db_type, **kwargs)
            
            # 使用 SQLAlchemy 的 inspect 功能获取表结构
            inspector = inspect(engine)
            
            # 获取列信息
            columns_info = {}
            for column in inspector.get_columns(table_name):
                col_name = column['name']
                columns_info[col_name] = {
                    'type': str(column['type']),
                    'nullable': column['nullable'],
                    'default': column['default'],
                    'autoincrement': column.get('autoincrement', False),
                    'primary_key': False
                }
            
            # 获取主键信息
            primary_keys = inspector.get_pk_constraint(table_name)
            if primary_keys and 'constrained_columns' in primary_keys:
                for pk_col in primary_keys['constrained_columns']:
                    if pk_col in columns_info:
                        columns_info[pk_col]['primary_key'] = True
            
            table_info = {
                'columns': columns_info,
                'primary_keys': primary_keys.get('constrained_columns', []) if primary_keys else [],
                'auto_increment_column': self._find_auto_increment_column(columns_info)
            }
            
            # 缓存表信息
            self.table_cache[cache_key] = table_info
            return table_info
            
        except SQLAlchemyError as e:
            _logger.error(f"获取表结构失败: {e}")
            return {}
    
    def batch(self, sql, paramsList:list[dict|tuple]=None, batchsize:int=100, commit=True):
        _logger.DEBUG(f"[SQL]:{sql}")
        _logger.DEBUG(f"[Parameters]:{paramsList}")
        results = 0
        
        if paramsList is None or len(paramsList)==0:
            return 0
        
        with self.engine.begin() as connection:
            try:
                datas = []
                for params in paramsList:                                            
                    datas.append(params)
                    if len(datas) >= batchsize:
                        count = connection.connection.cursor().executemany(sql, datas)  # 参数为元组    
                        results += count
                        _logger.DEBUG(f'批处理执行{len(datas)}条记录，更新数据{count}')                            
                        if commit :
                            # connection.commit()
                            self.commit(connection)
                            
                        datas.clear()
                if len(datas) > 0:
                    count = connection.connection.cursor().executemany(sql, datas)  # 参数为元组    
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
        
# @event.listens_for(engine, "checkout")
# def on_checkout(dbapi_conn, conn_record, conn_proxy):
#     print("[池] 取出连接", conn_record)        

# 数据库	推荐驱动	URL 模板（把 u/p/host/db 换成自己的）	备注
# PostgreSQL	psycopg2	postgresql+psycopg2://u:p@host:5432/db?charset=utf8	官方最快
# PostgreSQL	asyncpg	postgresql+asyncpg://u:p@host:5432/db	异步专用
# MySQL	pymysql	mysql+pymysql://u:p@host:3306/db?charset=utf8mb4	纯 Python
# MySQL	mysqlclient	mysql+mysqldb://u:p@host:3306/db?charset=utf8mb4	C 扩展，更快
# Oracle	cx_Oracle	oracle+cx_oracle://u:p@host:1521/?service_name=XE	可换 sid=ORCL
# SQL Server	pyodbc	mssql+pyodbc://u:p@host:1433/db?driver=ODBC+Driver+17+for+SQL+Server	Windows/Linux 通用
# SQLite	内置	sqlite:///./app.db（相对）或 sqlite:////absolute/path.db	文件库
# ClickHouse	clickhouse-sqlalchemy	clickhouse+http://u:p@host:8123/db	默认 HTTP 协议
# 达梦 DM	dmPython	dm+dmPython://u:p@host:5236/db	国产库
# KingBase	ksycopg2	kingbase+ksycopg2://u:p@host:54321/db

if __name__ == "__main__":
    url = make_url('postgresql+psycopg2://root:12345@host:5432/db?charset=utf8')    
    print(url)
    url = url.set(username='liuyong')
    url = url.set(password='123456')
    print(url)
    
    url = 'mysql+pymysql://u:p@localhost:61306/stock_agent?charset=utf8mb4'
    p = PydbcTools(url=url, username='stock_agent', password='1qaz2wsx', test='select 1')
    print(p)
    # print(p.queryOne('select * from sa_security_realtime_daily limit 10'))
    # print(p.queryPage('select * from sa_security_realtime_daily order by tradedate desc', None, page=1, pagesize=10))        
    t = p.queryPage('select * from sa_security_realtime_daily where code=:code order by tradedate desc', {'code':'300492'}, page=1, pagesize=10)
    print(json_to_str(t))
    
    url = 'postgresql+psycopg2://u:p@pgvector.ginghan.com:29432/aiproxy'
    p = PydbcTools(url=url, username='postgres', password='aiproxy', test='select 1')
    print(p)
    # print(p.queryOne('select * from logs limit 10'))
    # print(p.queryPage('select * from logs order by request_at desc', None, page=1, pagesize=10))        
    # print(p.queryPage('select * from logs where endpoint=:code order by request_at desc', {'code':'/v1/chat/completions'}, page=1, pagesize=10))
    t = p.queryPage('select * from logs where endpoint=:code order by request_at desc', {'code':'/v1/chat/completions'}, page=1, pagesize=10)
    print(json_to_str(t))
    
    # url = 'oracle+cx_oracle://u:p@localhost:1521/?service_name=XE'
    url = 'oracle+oracledb://u:p@localhost:60521/?service_name=ORCL'
    p = PydbcTools(url=url, username='system', password='orcl', test='select 1 from dual')
    print(p)
    # print(p.queryOne('SELECT * FROM dba_registry'))
    # print(p.queryPage('SELECT * FROM dba_registry', None, page=1, pagesize=10))        
    # print(p.queryPage("SELECT * FROM dba_registry where version like '%'||:version||'%' order by comp_id desc", {'version':'19'}, page=1, pagesize=10))
    t = p.queryPage("SELECT * FROM dba_registry where version like '%'||:version||'%' order by comp_id desc", {'version':'19'}, page=1, pagesize=10)
    print(json_to_str(t))
    
    
    
    