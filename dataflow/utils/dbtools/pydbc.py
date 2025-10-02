from sqlalchemy import create_engine, Engine, text, event, make_url, inspect
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from dataflow.utils.log import Logger
from dataflow.utils.utils import PageResult
from dataflow.utils.utils import json_to_str, str_isEmpty, get_unique_seq
from typing import Any, Dict, Optional,Self
from cachetools import Cache

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

INNER_PLACEHOLDER = '_$inner$_'
INNER_UPDATE_PLACEHOLDER = '_update__'


class _NULLObj:
    pass

NULL = _NULLObj()    

def _is_null(obj):
    return NULL == obj


class PydbcTools:
    def __init__(self, **kwargs):
        self._table_cache = Cache(maxsize=10000000)
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
    
    def getDbType(self)->str:
        return self.engine.dialect.name.lower()
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
        _logger.INFO(f"[SQL]:{sql}")
        _logger.INFO(f"[Parameter]:{params}")
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
                if self.getDbType() == "postgresql": # ("postgresql", "mysql", "sqlite", "clickhouse", "openGauss", "dm", "kingbase")
                    sql_wrap = sql + ' LIMIT :_pagesize_  OFFSET :_offset_ '
                elif self.getDbType() == "mysql":
                    # sql_wrap = sql + ' LIMIT :_pagesize_  OFFSET :_offset_ '
                    sql_wrap = sql + ' LIMIT :_offset_, :_pagesize_  '
                elif self.getDbType() == "oracle":
                    sql_wrap = f'SELECT * FROM (SELECT t.*, ROWNUM rn FROM ({sql}) t) WHERE rn BETWEEN :_offset_ + 1 AND :_offset_ + :_pagesize_ '
                elif self.getDbType() == "mssql":
                    sql_wrap = sql + ' OFFSET :_offset_ ROWS FETCH NEXT :_pagesize_ ROWS ONLY'
                elif self.getDbType() == "hive":
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
                return results.rowcount
            except Exception as e:
                connection.rollback()
                _logger.ERROR("[Exception]", e)
                raise e
            
    def insert(self, sql, params=None, commit=True):
        return self.update(sql, params, commit)

    def delete(self, sql, params=None, commit=True):
        return self.update(sql, params, commit)
        
    def insertT(self, tablename:str, params:dict=None, commit=True)->int:
        if not params:
            _logger.WARN("插入对象不能为空")
            return 0
        
        # 获取表结构信息
        table_info = self.get_table_info(tablename)
        if not table_info:
            _logger.ERROR(f"无法获取表 {tablename} 的结构信息")
            return 0        
                
        columns_info = table_info['columns']
        auto_increment_column = table_info['auto_increment_column']
        
        valided_data = {}
        
        for field_name, field_value in params.items():
            if field_name in columns_info:
                # 跳过自增主键（通常由数据库自动生成）
                if field_name == auto_increment_column:
                    continue
                if _is_null(field_value):
                    valided_data[field_name] = None
                else:                
                    valided_data[field_name] = field_value            
        
        if not valided_data:
            _logger.ERROR("没有有效的字段可以插入")
            return 0
        
        # 构建列名和值
        quoted_columns = [self._quote_identifier(col) for col in valided_data.keys()]
        columns = ', '.join(quoted_columns)
        placeholders = ', '.join([f':{col}' for col in valided_data.keys()])
        
        # 构建 SQL 语句
        sql = f'INSERT INTO {tablename} ({columns}) VALUES ({placeholders})'
        _logger.DEBUG(f'SQL={sql}')
        _logger.DEBUG(f'Paramters={valided_data}')
        
        with self.engine.begin() as connection:
            try:
                results = connection.execute(text(sql), valided_data)                    
                # 获取自增长ID
                inserted_id = None
                if auto_increment_column:
                    inserted_id = self._get_last_insert_id(connection, tablename, auto_increment_column)
                    params[auto_increment_column] = inserted_id
                
                if commit:
                    connection.commit()
                return results.rowcount
            except Exception as e:
                connection.rollback()
                _logger.ERROR("[Exception]", e)
                raise e
        
    def updateT2(self, tablename:str, obj:dict=None, where:dict=None, condiftion:str=None, commit=True):
        if not obj:
            _logger.WARN("更新对象不能为空")
            return 0
        
        # 获取表结构信息
        table_info = self.get_table_info(tablename)
        if not table_info:
            _logger.ERROR(f"无法获取表 {tablename} 的结构信息")
            return 0        
                
        columns_info = table_info['columns']
        
        valided_data = {}
        
        for field_name, field_value in obj.items():
            if field_name in columns_info:                
                if _is_null(field_value):
                    valided_data[field_name] = None                    
                else:                
                    valided_data[field_name] = field_value            
        
        if not valided_data:
            _logger.ERROR("没有有效的字段可以更新")
            return 0
        
        # 构建列名和值
        quoted_columns_placeholders = [f'{self._quote_identifier(col)}=:{INNER_UPDATE_PLACEHOLDER}{col}' for col in valided_data.keys()]
        columns = ', '.join(quoted_columns_placeholders)         
        
        sql_params = {}
        for field_name, field_value in valided_data.items():
            sql_params[f'{INNER_UPDATE_PLACEHOLDER}{field_name}'] = field_value
        
        sql_params.update(where)
                    
        where_sql = ''
        
        where_data = {}
        for field_name, field_value in where.items():
            if field_name in columns_info:                
                if _is_null(field_value):
                    where_data[field_name] = None                    
                else:                
                    where_data[field_name] = field_value  
        
        if where_data:
            where_columns_placeholders = [f'{self._quote_identifier(col)}=:{col}' for col in where_data.keys()]
            where_sql = ' AND '.join(where_columns_placeholders)
                
        if not str_isEmpty(where_sql):
            where_sql = ' WHERE ' + where_sql
        
        # 构建 SQL 语句
        sql = f'UPDATE {tablename} SET {columns} {where_sql}'
        _logger.INFO(f'SQL={sql}')
        _logger.INFO(f'Paramters={sql_params}')
        
        with self.engine.begin() as connection:
            try:
                results = connection.execute(text(sql), sql_params)
                if commit:
                    connection.commit()
                return results.rowcount
            except Exception as e:
                connection.rollback()
                _logger.ERROR("[Exception]", e)
                raise e
    
    def updateT(self, tablename:str, obj:dict=None, condiftion:dict=None, commit=True):
        if not obj:
            _logger.WARN("更新对象不能为空")
            return 0
        
        # 获取表结构信息
        table_info = self.get_table_info(tablename)
        if not table_info:
            _logger.ERROR(f"无法获取表 {tablename} 的结构信息")
            return 0        
                
        columns_info = table_info['columns']
        
        valided_data = {}        
        
        for field_name, field_value in obj.items():
            if field_name in columns_info:                
                if _is_null(field_value):
                    valided_data[field_name] = None         
                    # null_data[field_name] = None          
                else:                
                    valided_data[field_name] = field_value            
        
        if not valided_data:
            _logger.ERROR("没有有效的字段可以更新")
            return 0
        
        # 构建列名和值
        quoted_columns_placeholders = [f'{self._quote_identifier(col)}=:{INNER_UPDATE_PLACEHOLDER}{col}' for col in valided_data.keys()]
        columns = ', '.join(quoted_columns_placeholders)         
        
        sql_params = {}
        for field_name, field_value in valided_data.items():
            sql_params[f'{INNER_UPDATE_PLACEHOLDER}{field_name}'] = field_value
        
        # sql_params.update(condiftion)
        
        condiftion_data = {}
        for field_name, field_value in condiftion.items():
            if field_name in columns_info:                
                if _is_null(field_value):
                    condiftion_data[field_name] = ('IS', 'NULL')
                    # null_data[field_name] = None          
                else:                
                    condiftion_data[field_name] =('=', f':{field_name}')
                    sql_params[field_name] = field_value
                    
        where_sql = ''
        
        if condiftion_data:
            where_columns_placeholders = [f'{self._quote_identifier(col)} {item[0]} {item[1]}' for col,item in condiftion_data.items()]
            where_sql = ' AND '.join(where_columns_placeholders)
        
        if not str_isEmpty(where_sql):
            where_sql = ' WHERE ' + where_sql
        
        # 构建 SQL 语句
        sql = f'UPDATE {tablename} SET {columns} {where_sql}'
        _logger.INFO(f'SQL={sql}')
        _logger.INFO(f'Paramters={sql_params}')
        
        with self.engine.begin() as connection:
            try:
                results = connection.execute(text(sql), sql_params)
                if commit:
                    connection.commit()
                return results.rowcount
            except Exception as e:
                connection.rollback()
                _logger.ERROR("[Exception]", e)
                raise e
    
    def deleteT(self, tablename:str, condiftion:dict=None, commit=True):
        
        # 获取表结构信息
        table_info = self.get_table_info(tablename)
        if not table_info:
            _logger.ERROR(f"无法获取表 {tablename} 的结构信息")
            return 0        
                
        columns_info = table_info['columns']
        sql_params = {}
        condiftion_data = {}
        for field_name, field_value in condiftion.items():
            if field_name in columns_info:                
                if _is_null(field_value):
                    condiftion_data[field_name] = ('IS', 'NULL')
                    # null_data[field_name] = None          
                else:                
                    condiftion_data[field_name] =('=', f':{field_name}')
                    sql_params[field_name] = field_value
                    
        where_sql = ''
        
        if condiftion_data:
            where_columns_placeholders = [f'{self._quote_identifier(col)} {item[0]} {item[1]}' for col,item in condiftion_data.items()]
            where_sql = ' AND '.join(where_columns_placeholders)
        
        if not str_isEmpty(where_sql):
            where_sql = ' WHERE ' + where_sql
        
        # 构建 SQL 语句
        sql = f'delete from {tablename} {where_sql}'
        _logger.DEBUG(f'SQL={sql}')
        _logger.DEBUG(f'Paramters={sql_params}')
        
        with self.engine.begin() as connection:
            try:
                results = connection.execute(text(sql), sql_params)
                if commit:
                    connection.commit()
                return results.rowcount
            except Exception as e:
                connection.rollback()
                _logger.ERROR("[Exception]", e)
                raise e
    
    def _get_last_insert_id(self, connection, table_name: str, 
                           auto_increment_column: str) -> Optional[Any]:
        """获取最后插入的自增长ID"""        
        db_type = self.getDbType()
        
        try:
            if db_type == "mysql":
                # MySQL 使用 LAST_INSERT_ID()
                result = connection.execute(text("SELECT LAST_INSERT_ID()"))
                return result.scalar()
            
            elif db_type == "postgresql":
                # PostgreSQL 使用 RETURNING 子句或 currval
                # 这里我们使用 currval，需要知道序列名
                # 注意：这需要序列名遵循命名约定
                sequence_name = f"{table_name}_{auto_increment_column}_seq"
                result = connection.execute(text(f"SELECT currval('{sequence_name}')"))
                return result.scalar()
            
            elif db_type == "sqlite":
                # SQLite 使用 last_insert_rowid()
                result = connection.execute(text("SELECT last_insert_rowid()"))
                return result.scalar()
            
            elif db_type == "mssql":
                # SQL Server 使用 SCOPE_IDENTITY()
                result = connection.execute(text("SELECT SCOPE_IDENTITY()"))
                return result.scalar()
            
            elif db_type == "oracle":
                # Oracle 使用 RETURNING 子句，但这里我们使用序列的 currval
                # 注意：这需要知道序列名
                sequence_name = f"SEQ_{table_name}"
                result = connection.execute(text(f"SELECT {sequence_name}.CURRVAL FROM DUAL"))
                return result.scalar()
            
            else:
                # 其他数据库的通用方法
                _logger.WARN(f"数据库{db_type}的自增长ID获取方法未实现")
                return None
                
        except Exception as e:
            _logger.WARN(f"获取自增长ID失败: {e}")
            return None
        
    def _quote_identifier(self, identifier: str) -> str:
        """根据数据库类型引用标识符"""
        # 大多数数据库使用双引号，但有些数据库使用其他符号
        db_type = self.getDbType()
        if db_type in ['mysql', 'sqlite']:
            return f"`{identifier}`"
        elif db_type in ['mssql']:
            return f"[{identifier}]"
        else:
            return f'"{identifier}"'
     
    def _find_auto_increment_column(self, columns_info: Dict[str, Any]) -> Optional[str]:
        """查找自增长字段"""
        for col_name, col_info in columns_info.items():
            if col_info.get('autoincrement', False) and col_info.get('primary_key', False):
                return col_name
        return None
    
    def get_table_info(self, table_name: str, **kwargs) -> Dict[str, Any]:
        """获取表的字段信息"""        
        cache_key = table_name
        
        if cache_key in self._table_cache:
            _logger.DEBUG(f'从Cache里找到{table_name}表信息')
            return self._table_cache[cache_key]
        
        engine = self.engine
        try:            
            # 使用 SQLAlchemy 的 inspect 功能获取表结构
            inspector = inspect(engine)
            
            arr = table_name.split('.')
            if len(arr) == 1:
                infos = inspector.get_columns(table_name)
            else:
                infos = inspector.get_columns(arr[1], arr[0])
            # 获取列信息
            columns_info = {}
            
            
            for column in infos:
                col_name = column['name']
                columns_info[col_name] = {
                    'type': str(column['type']),
                    'nullable': column['nullable'],
                    'default': column['default'],
                    'autoincrement': column.get('autoincrement', False),
                    'primary_key': False
                }
            
            # 获取主键信息
            
            if len(arr) == 1:
                primary_keys = inspector.get_pk_constraint(table_name)
            else:
                primary_keys = inspector.get_pk_constraint(arr[1], arr[0])
                
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
            self._table_cache[cache_key] = table_info
            _logger.DEBUG(f'缓存{table_name}表信息到Cache={table_info}')
            return table_info
            
        except SQLAlchemyError as e:
            _logger.ERROR(f"获取表结构失败: {e}")
            raise e
    
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


class SimpleExpression:
    class ExpressionException(Exception):
        pass
    
    def __init__(self):
        self.param_context = {}
        self.sql = ''
    def Sql(self)->str:
        return self.sql
    def Param(self)->dict:
        return self.param_context
    def _add(self, add:str, field:str, op:str, param:any)->Self:        
        if op.upper() not in ['IN', '>', '>=', '<', '<=', '<>', '=']:
            raise SimpleExpression.ExpressionException(f'不支持操作符{op}')
        s_k = f'p_{get_unique_seq()}'
        if self.sql :
            self.sql += f' {add} {field} {op} :{s_k}'
        else:
            self.sql += f'{field} {op} :{s_k}'            
        self.param_context[s_k] = param
        return self
    def AND(self, field:str, op:str, param:any)->Self:
        return self._add('AND', field, op, param)
    def OR(self, field:str, op:str, param:any)->Self:
        return self._add('OR', field, op, param)
    def AND_ISNULL(self, field:str, nullornot:bool)->Self:
        return self._addNULL('AND', field, nullornot)
    def OR_ISNULL(self, field:str, nullornot:bool)->Self:
        return self._addNULL('OR', field, nullornot)
    def AND_BETWEEN(self, field:str, value1:any, value2:any)->Self:
        return self._addBetween('AND', field, value1, value2)
    def OR_BETWEEN(self, field:str, value1:any, value2:any)->Self:
        return self._addBetween('OR', field, value1, value2)
    def AND_IN(self, field:str, values:list[any])->Self:
        return self._addIn('AND', field, values)
    def OR_IN(self, field:str, values:list[any])->Self:
        return self._addIn('OR', field, values)
    def AND_EXPRESSION(self, field:str, sql2:Self)->Self:
        return self._addExpression('AND', sql2)
    def OR_EXPRESSION(self, field:str, sql2:Self)->Self:
        return self._addExpression('OR', field, sql2)
    def AND_SQL(self, field:str, sql2:str, param2:dict)->Self:
        return self._addSQL('AND', sql2, param2)
    def OR_SQL(self, field:str, sql2:str, param2:dict)->Self:
        return self._addSQL('OR', sql2, param2)
    def _addExpression(self, add:str,sql2:Self)->Self:
        if sql2:
            if self.sql :
                self.sql += f' {add} {sql2.sql}'
            else:
                self.sql += f' {sql2.sql}'
            self.param_context.update(sql2.param_context)
        return self    
    def _addNULL(self, add:str, field:str, nullornot:bool)->Self:                
        sql = 'IS NULL' if nullornot else 'IS NOT NULL'
        if self.sql :
            self.sql += f' {add} {field} {sql}'
        else:
            self.sql += f' {field} {sql}'        
        return self
    def _addBetween(self, add:str, field:str, value1:any, value2:any)->Self:                
        s = get_unique_seq()
        s_k_1 = f'p_{s}_s'
        s_k_2 = f'p_{s}_e'
        if self.sql :
            self.sql += f' {add} {field} BETWEEN :{s_k_1} AND :{s_k_2}'
        else:
            self.sql += f' {field} BETWEEN :{s_k_1} AND :{s_k_2}'        
        self.param_context[s_k_1]=value1
        self.param_context[s_k_2]=value2
        return self    
    def _addIn(self, add:str, field:str, values:list[any])->Self:
        if values:
            s = get_unique_seq()
            cols = [f':p_{s}_{i}' for i, v in enumerate(values)]
            [self.param_context.update({f'p_{s}_{i}': v}) for i, v in enumerate(values)]
            col = ','.join(cols)
            if self.sql :
                self.sql += f' {add} {field} IN ({col})'
            else:
                self.sql += f' {field} IN ({col})' 
        return self  
    def _addSQL(self, add:str, sql2:str, param2:dict)->Self:        
        if self.sql :
            self.sql += f' {add} {sql2}'
        else:
            self.sql += f' {sql2}'
        self.param_context.update(param2)
        return self
        
        
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
    
    # print('123123'.index(INNER_PLACEHOLDER))
    
    url = 'mysql+pymysql://u:p@localhost:61306/dataflow_test?charset=utf8mb4'
    p = PydbcTools(url=url, username='stock_agent', password='1qaz2wsx', test='select 1')
    print(p)
    # print(p.queryOne('select * from sa_security_realtime_daily limit 10'))
    # print(p.queryPage('select * from sa_security_realtime_daily order by tradedate desc', None, page=1, pagesize=10))        
    # print(p.queryPage('select * from sa_security_realtime_daily where code=:code order by tradedate desc', {'code':'300492'}, page=1, pagesize=10))
    t = p.queryPage('select * from sa_security_realtime_daily where tradedate=:tradedate order by tradedate desc', {'tradedate':'2025-09-30'}, page=1, pagesize=10)
    print(json_to_str(t))
    
    print(p.get_table_info('dataflow_test.sa_security_realtime_daily'))
    
    
    sample = '''
    {"id":435177,"tradedate":"2025-09-30","code":"920819","name":"颖泰生物","price":"4.25","changepct":"-0.47","change":"-0.02","volume":"56537","turnover":"24137761.32","amp":"1.17","high":"4.3","low":"4.25","topen":"4.3","lclose":"4.27","qrr":"0.62","turnoverpct":"0.47","pe_fwd":"170.35","pb":"1.02","mc":"5209650000","fmc":"5131906875","roc":"-0.23","roc_5min":"-0.23","changepct_60day":"1.67","changepct_currentyear":"19.72","hot_rank_em":5116,"market":"SZ","createtime":"2025-09-30 09:32:17","updatetime":"2025-09-30 17:06:09","enable":1}
    '''
    from dataflow.utils.utils import str_to_json
    sample:dict = str_to_json(sample)    
    sample.pop('id',None)
    sample.pop('high',None)
    sample['low']=NULL    
    sample['tradedate']='2025-01-05'
    # rtn = p.insertT('dataflow_test.sa_security_realtime_daily', sample)
    # print(f'Result={rtn}')
    
    sample = '''
    {"price":"4.25","changepct":"-0.47","change":"-0.02","volume":"56537","turnover":"24137761.32","amp":"1.17"}
    '''
    sample:dict = str_to_json(sample)
    sample['topen']=NULL
    rtn = p.updateT2('dataflow_test.sa_security_realtime_daily', sample, {"code":"920819","tradedate":"2025-01-05"}, "code=:code and tradedate=:tradedate")
    print(f'Result={rtn}')
    
    sample1 = '''
    {"code":"920819","tradedate":"2025-01-05","price":"4.25","changepct":"-0.47","change":"-0.02","volume":"56537","turnover":"24137761.32","amp":"1.17"}
    '''
    sample1:dict = str_to_json(sample1)
    sample1['topen']=1.0
    rtn = p.updateT('dataflow_test.sa_security_realtime_daily', sample, sample1)
    print(f'Result={rtn}')
    
    sample1['topen']=NULL
    rtn = p.deleteT('dataflow_test.sa_security_realtime_daily', sample1)
    print(f'Result={rtn}')
    
    exp = SimpleExpression()
    exp = exp.AND('code','=','920819').AND('price','=',4.25).AND_ISNULL('volume',False)
    exp = exp.AND_IN('code',['920819','920813'])
    exp = exp.AND_BETWEEN('tradedate','2025-01-05','2025-01-06')
    
    rtn = p.queryMany('select * from dataflow_test.sa_security_realtime_daily where 1=1 AND ' + exp.Sql(), exp.Param())
    print(f'Result={rtn}')
    
    import sys
    sys.exit()
    
    
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
    
    
    
    
    