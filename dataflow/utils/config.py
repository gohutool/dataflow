"""Application configuration management.

This module handles environment-specific configuration loading, parsing, and management
for the application. It includes environment detection, .env file loading, and
configuration value parsing.
"""
import os
from enum import Enum
from dotenv import load_dotenv
from dataflow.utils.log import Logger
from typing import Optional,List,Dict
from dataflow.utils.utils import str2Num,str_isEmpty, str2Bool
from dataflow.utils.reflect import getAttrPlus
from omegaconf import OmegaConf
import threading
from typing import Self
import re
from string import Template
from pathlib import Path


_logger = Logger('dataflow.utils.config')


# Define environment types
class Environment(str, Enum):
    """Application environment types.

    Defines the possible environments the application can run in:
    development, staging, production, and test.
    """

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


# Determine environment
def get_environment() -> Environment:
    """Get the current environment.

    Returns:
        Environment: The current environment (development, staging, production, or test)
    """
    match os.getenv("APP_ENV", "development").lower():
        case "production" | "prod":
            return Environment.PRODUCTION
        case "staging" | "stage":
            return Environment.STAGING
        case "test":
            return Environment.TEST
        case _:
            return Environment.DEVELOPMENT


# Load appropriate .env file based on environment
def load_env_file():
    """Load environment-specific .env file."""
    env = get_environment()
    _logger.INFO(f"Loading environment: {env}")
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    # Define env files in priority order
    env_files = [
        os.path.join(base_dir, f".env.{env.value}.local"),
        os.path.join(base_dir, f".env.{env.value}"),
        os.path.join(base_dir, ".env.local"),
        os.path.join(base_dir, ".env"),
    ]

    # Load the first env file that exists
    for env_file in env_files:
        if os.path.isfile(env_file):
            load_dotenv(dotenv_path=env_file)
            _logger.INFO(f"Loaded environment from {env_file}")
            return env_file

    # Fall back to default if no env file found
    return None


ENV_FILE = load_env_file()


class Settings:
    """Application settings without using pydantic."""

    def __init__(self):
        """Initialize application settings from environment variables.

        Loads and sets all configuration values from environment variables,
        with appropriate defaults for each setting. Also applies
        environment-specific overrides based on the current environment.
        """
        # Set the environment
        self.ENVIRONMENT = get_environment()
        # Application Settings
        self.PROJECT_NAME = os.getenv("PROJECT_NAME", "DataFlow Project")
        self.VERSION = os.getenv("VERSION", "1.0.0")
        self.DESCRIPTION = os.getenv(
            "DESCRIPTION", "A production-ready FastAPI with DataFlow at AI Agent"
        )     
                     
    def getInt(self, env, dv:Optional[int]=0)->int:
        value = os.getenv(env)
        if value is None:
            value = dv
        else:
            value = str2Num(f'{value}', dv)
        return int(value)
    
    def getStr(self, env, dv:Optional[str]=0)->str:
        value = os.getenv(env)
        if value is None:
            value = dv
        else:
            value = f'{value}'
            if len(value.strip()) == 0:
                value = dv
        return value
    
    def getFloat(self, env, dv:Optional[int]=0)->float:
        value = os.getenv(env)
        if value is None:
            value = dv
        else:
            value = str2Num(f'{value}', dv)
        return value
    
    def getList(self, env, dv:Optional[List[str]]=None)->List[str]:
        """Parse a comma-separated list from an environment variable."""
        value = os.getenv(env)
        if not value:
            return dv

        # Remove quotes if they exist
        value = value.strip("\"'")
        # Handle single value case
        if "," not in value:
            return [value]
        # Split comma-separated values
        return [item.strip() for item in value.split(",") if item.strip()]
    
    def getDict(self, env_prefix, default_dict:Optional[Dict[str,str|List]]=None)->Dict[str,str|List]:
        """Parse dictionary of lists from environment variables with a common prefix."""
        result = None
        
        # Look for all env vars with the given prefix
        for key, value in os.environ.items():
            self._logger.DEBUG(f'{key} = {value}')
            if key.startswith(env_prefix):
                if result is None:
                    result = default_dict or {}
                    
                endpoint = key[len(env_prefix):].lower()  # Extract endpoint name
                # Parse the values for this endpoint
                if value:
                    value = value.strip("\"'")
                    if "," in value:
                        result[endpoint] = [item.strip() for item in value.split(",") if item.strip()]
                    else:
                        result[endpoint] = value

        return result


# Create settings instance
settings = Settings()

def ___resolve_custom_env_var(interpolation_str):
    """
    解析 'VAR_NAME:default_value' 格式的字符串。
    首先检查环境变量VAR_NAME，若存在则使用其值，否则使用default_value。
    """
    # 分割字符串，获取环境变量名和默认值    
    # print(interpolation_str)
    if ':' in interpolation_str:
        var_name, default_value = interpolation_str.split(':', 1)
    else:
        # 如果没有提供默认值，则设为空字符串
        var_name, default_value = interpolation_str, ''
    # 从环境变量获取值，如果不存在则使用默认值
    return os.environ.get(var_name, default_value)

# 在加载配置之前注册解析器
OmegaConf.register_new_resolver("env", ___resolve_custom_env_var)

def getValue_plus(_root_, key)->any:
    v = OmegaConf.select(_root_, key)
    if v is None:
        v = os.environ.get(key, None)
    return v

def colon_default_resolver(expression, _parent_,_node_,_root_):
    """
    使用冒号作为默认值分隔符的解析器
    语法: ${colon_default:key:default_value}
    """
    # 分割键和默认值
    parts = expression.split(':', 1)  # 只分割第一个冒号
    
    if len(parts) == 1:
        # 没有默认值，直接返回键
        key = parts[0]
        # return OmegaConf.select(_root_, key)
        v = getValue_plus(_root_, key)
        if v is None:
            raise ValueError(f'{expression}没有对应值，请确认配置，或者通过'+'${key:default}方式设置默认值')
        return v
    else:
        # 有默认值的情况
        key, default_value = parts
        # value = OmegaConf.select(_root_, key)
        value = getValue_plus(_root_, key)
        
        # 如果键不存在，返回默认值
        if value is None:
            return default_value
        return value

"""注册冒号默认值解析器"""
# doc="使用冒号作为默认值分隔符，语法: ${p:key:default_value}"
OmegaConf.register_new_resolver(
    "p",     
    colon_default_resolver,
    replace=True
)
    
# # 最后一个冒号当分隔符，优先级：ctx > env > default
# OmegaConf.register_new_resolver(
#     "cfg",                       # 名字随意，这里叫 cfg
#     lambda key, default=None: OmegaConf.select(
#         OmegaConf.get_config(),  # 当前全局配置
#         key,
#         default=default if default is not None else f"${{{key}}}"
#     ),
#     replace=True
# )

# OmegaConf.register_new_resolver(
#         "",  # 空名 = 匿名
#         lambda key, default=None: OmegaConf.select(
#             OmegaConf.get_config(), key, default=default
#         ),
#         replace=True,
#     )

_token_re = re.compile(r'\$\{([^:}]+)(?::([^}]*))?\}$')

def parse_placeholder(raw: str) -> tuple[str, Optional[str]]:
    """
    把 ${path.to.key:default} 拆成 ('path.to.key', 'default')
    没有冒号则 default=None
    """
    m = _token_re.match(raw.strip())
    if not m:
        raise ValueError(f'不是合法 OmegaConf 占位符: {raw}')
    key, default = m.groups()
    return key, default

class YamlConfigation:    
    _lock: any = threading.Lock()
    _MODEL_CACHE: dict[str, any] = {}
    
    @staticmethod
    def getConfiguration()->Self:
        return next(iter(YamlConfigation._MODEL_CACHE.values()))
    
    @staticmethod
    def loadConfiguration(yaml_path:str=None)->Self:        
        
        if yaml_path in YamlConfigation._MODEL_CACHE:               # 快速路径无锁
            _logger.WARN('Load Configuration from memory')
            return YamlConfigation._MODEL_CACHE[yaml_path]

        with YamlConfigation._lock:                            # 并发加载保护
            if yaml_path not in YamlConfigation._MODEL_CACHE:       # 二次检查
                YamlConfigation._MODEL_CACHE[yaml_path] = YamlConfigation(yaml_path)
                _logger.WARN('Load Configuration from local')
            return YamlConfigation._MODEL_CACHE[yaml_path]
        
    @staticmethod
    def _load_yamlfile_plus(yaml_path:str|Path):
        # 加载 YAML 配置（支持 ${} 占位符）    
        # c = OmegaConf.load(yaml_path)    
        
        # 改进
        # 1. 读取文件
        if isinstance(yaml_path, str):
            yaml_path = Path(yaml_path)
            
        yaml_text = yaml_path.read_text(encoding="utf-8")                
        # 2. 替换插值前缀
        converted_text = convert_yaml_config_txt(yaml_text)        
        # _logger.DEBUG(converted_text)
        
        c = OmegaConf.create(converted_text)
        return c
        
    def __init__(self, yaml_path, **kwargs):                    
        c = YamlConfigation._load_yamlfile_plus(yaml_path)
        self._c = c
        # OmegaConf.resolve(c)
        self._config = OmegaConf.to_container(c, resolve=True)
        self._config_temp = OmegaConf.create(self._config)
        
    def getConfig(self, prefix:str=None)->any:
        c = self._config
        if str_isEmpty(prefix):
            return c
        else:            
            return getAttrPlus(c,prefix)
        
    def getStr(self, key, dv:str=None)->str:
        c = self._config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return dv
        else:
            return str(obj)
        
    def getBool(self, key, dv:int=None)->bool:
        c = self._config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return dv
        else:
            return str2Bool(str(obj))
        
    def getInt(self, key, dv:int=None)->int:
        c = self._config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return dv
        else:
            return int(str2Num(str(obj)))
        
    def getFloat(self, key, dv:float=None)->float:
        c = self._config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return dv
        else:
            return str2Num(str(obj))
        
    def getList(self, key)->List:
        c = self._config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return []
        else:
            return list(obj).copy()
        
    def mergeDict(self, config:dict={}):        
        if config:
            dotlist = [f"{k}={v}" for k, v in config.items()]
            return self.mergeDotlist(dotlist)
            # update_config = OmegaConf.from_dotlist()
            # merged = OmegaConf.merge(self._c, update_config)
            # self._c = merged
            # self._config = OmegaConf.to_container(self._c, resolve=True)
            # self._config_temp = OmegaConf.create(self._config)
            # # self._config.update(merged)
            # return merged
        return {}
    
    def mergeDotlist(self, dotlist:list=[]):        
        if dotlist:
            
            converted_texts = []
            for v in dotlist:
                converted_texts.append(convert_yaml_config_txt(v))
            
            update_config = OmegaConf.from_dotlist(converted_texts)
            merged = OmegaConf.merge(self._c, update_config)
            self._c = merged
            self._config = OmegaConf.to_container(self._c, resolve=True)
            self._config_temp = OmegaConf.create(self._config)
            # self._config.update(merged)
            return merged
        return {}
    
    def mergeFile(self, filepath:str|Path):
        if filepath:
             
            update_config = YamlConfigation._load_yamlfile_plus(filepath)
            # update_config = OmegaConf.load(filepath)
            
            merged = OmegaConf.merge(self._c, update_config)
            self._c = merged
            self._config = OmegaConf.to_container(self._c, resolve=True)
            self._config_temp = OmegaConf.create(self._config)
            # self._config.update(merged)
            return merged
        return {}
    
    def value2(self, placeholder:str)->any:
        placeholder = convert_yaml_config_txt(placeholder)
        
        temp_config = OmegaConf.create({"___temp___": placeholder})
        merged = OmegaConf.merge(self._config_temp, temp_config)
        return merged['___temp___']
        
        # return OmegaConf.resolve(placeholder, self._c)
        # return self._c.resoleve
        #  return OmegaConf.resolve(placeholder, self._c)
        return Template(placeholder).substitute(self._config)
    
        
    def value(self, placeholder:str)->any:
        if not placeholder:
            return placeholder
        return self.value2(placeholder)
        # if '${env' in placeholder:
        #     temp_config = OmegaConf.create({"___temp___": placeholder})
        #     merged = OmegaConf.merge(self._config_temp, temp_config)
        #     return merged['___temp___']        
        # if '${' in placeholder:
        #     if ':' in placeholder:
        #         key, value = parse_placeholder(placeholder)
        #         placeholder = '${' + key + '}' 
        #         # print(f'placeholder = {placeholder} value={value}')
        #         temp_config = OmegaConf.create({"___temp___": placeholder})
        #         merged = OmegaConf.merge(self._config_temp, temp_config)
                                
        #         if '___temp___' in merged:
        #             try:
        #                 if merged['___temp___']:
        #                     return merged['___temp___']
        #                 else:
        #                     return value
        #             except Exception:
        #                 return value
        #         else:
        #             return value
        #     else:                
        #         temp_config = OmegaConf.create({"___temp___": placeholder})
        #         merged = OmegaConf.merge(self._config_temp, temp_config)
        #         return merged['___temp___']
 
def convert_yaml_config_txt(text: str, new_prefix: str = "p") -> str:
    result = convert_interpolation_pattern_enhanced(text, new_prefix)
    result = result.replace('${'+new_prefix+':env:', '${env:')
    return result
 
def convert_interpolation_pattern_enhanced(text: str, new_prefix: str = "p") -> str:
    """
    将文本中所有 ${...} 插值表达式的头部加上 new_prefix:
    例如：${application.app.test:default} -> ${p:application.app.test:default}
    支持任意层嵌套，如：${a:${b:${c}}} -> ${p:a:${p:b:${p:c}}}
    使用栈解析，确保从最内层开始替换
    """
    def replace_from_innermost(s: str) -> str:
        result = []
        i = 0
        n = len(s)
        while i < n:
            if s[i:i+2] == '${':
                # 找到匹配的 '}'
                depth = 1
                j = i + 2
                while j < n and depth > 0:
                    if s[j:j+2] == '${':
                        depth += 1
                        j += 1
                    elif s[j] == '}':
                        depth -= 1
                        if depth == 0:
                            break
                    j += 1
                if depth == 0:
                    # 提取内部内容
                    inner = s[i+2:j]
                    # 递归处理内部
                    inner = replace_from_innermost(inner)
                    # 加前缀
                    if not inner.startswith(f"{new_prefix}:"):
                        inner = f"{new_prefix}:{inner}"
                    result.append(f"${{{inner}}}")
                    i = j + 1
                else:
                    # 不匹配，原样保留
                    result.append(s[i])
                    i += 1
            else:
                result.append(s[i])
                i += 1
        return ''.join(result)

    return replace_from_innermost(text)
 

if __name__ == "__main__":
    yaml_path = 'conf/application.yaml'
    
    config:YamlConfigation = None
    config = YamlConfigation.loadConfiguration(yaml_path)
    config = YamlConfigation.loadConfiguration(yaml_path)
    
    print(config.getConfig())
    
    print(f'server={config.getConfig('application.server')} server.port={config.getConfig('application.server.port')}')
    s = '${env:LANGFUSE.secret_key:sk-lf-b60f4b33-ff5a-46ac-9086-e776373c86da}  ${env:DB_PASSWORD:password}'
    v = config.value(s)
    print(f'{s} = {v}')
    
    s = 'features.api_url'
    print(f'{s} = {config.getStr(s)}')
    
    s = '${features.api_url1:test}'
    print(f'{s} = {config.value(s)}')
    
    
    s = '${env:LANGFUSE.secret_key:1-sk-lf-b60f4b33-ff5a-46ac-9086-e776373c86da}'
    print(f'{s} = {config.value(s)}')
    
    dict = {'application.server.port': '${aaa.test:19999}', 'application.server.host':'192.168.1.1'}
    
    config.mergeFile('conf/application-dev.yaml')
    
    config.mergeDict(dict)
    
    print(config.getConfig('application.server'))
    print(config.getConfig('application.server.port'))
    print(config.getConfig('context.test'))
    
    dotlist = ['application.server.port=${aaa.test:9999}', 'application.server.host=192.168.0.2']
    config.mergeDotlist(dotlist)
    print(config.getConfig('application.server'))
    
    
    # print(config.getConfig())
    
    s = '${env:LANGFUSE.secret_key:1-sk-lf-b60f4b33-ff5a-46ac-9086-e776373c86da}'
    print(f'{s} = {config.value(s)}')
        
    s = 'features.api_url'
    print(f'{s} = {config.getStr(s)}')
    
    s = '${features.api_url:test}'
    print(f'{s} = {config.value(s)}')
    
    s = '${features.api_url1:test}'
    print(f'{s} = {config.value(s)}')
    
    
    s = '${features.api_url:test}'
    print(f'{s} = {config.value(s)}')
    
    
    s = 'http://${application.server.host:test}:${application.server.port:test}/${MILVUS.uri1:none}'
    print(f'{s} = {config.value2(s)}')
    
    simple_case = "连接地址: ${application.app.test:${env:DB_URL:${application.DB_URL:${application.DB_URL:localhost}}}}"
    result1 = convert_interpolation_pattern_enhanced(simple_case)
    print(result1)
    # 输出: 连接地址: ${p:application.app.test}

    # # 跳过 env 解析器
    # with_env = "环境变量: ${env:APPLICATION_NAME} 和配置: ${application.app.test}"
    # result2 = convert_interpolation_pattern_enhanced(with_env)
    # print(result2)
    # # 输出: 环境变量: ${env:APPLICATION_NAME} 和配置: ${p:application.app.test}

    # # 嵌套替换，跳过已有解析器
    # nested_case = """
    
    # 环境: ${env:APP_ENV:development}}
    # 配置: 
    # 环境: ${env:APP_ENV:development}
    # 数据库: ${application.db.url:${env:DB_URL:localhost}}
    # 端口: ${application.db.port:${env:DB_PORT:5432}}
    # """
    # result3 = convert_interpolation_pattern_enhanced(nested_case)
    # print(result3)
    # 输出:
    # 配置: 
    #   环境: ${env:APP_ENV:development}
    #   数据库: ${p:application.db.url:${env:DB_URL:localhost}}
    #   端口: ${p:application.db.port:${env:DB_PORT:5432}}

    # 复杂嵌套
    # complex_case = """
    # 应用配置:
    # 名称: ${env:APP_NAME:${application.name:默认应用}}
    # 日志级别: ${env:LOG_LEVEL:${application.log.level:info}}
    # 功能开关: 
    #     特性A: ${application.features.a:${env:FEATURE_A:false}}
    #     特性B: ${application.features.b:true}
    # """
    # result4 = convert_interpolation_pattern_enhanced(complex_case)
    # print(result4)
    # 输出:
    # 应用配置:
    #   名称: ${env:APP_NAME:${p:application.name:默认应用}}
    #   日志级别: ${env:LOG_LEVEL:${p:application.log.level:info}}
    #   功能开关: 
    #     特性A: ${p:application.features.a:${env:FEATURE_A:false}}
    #     特性B: ${p:application.features.b:true}
    