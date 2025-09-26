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

__logger = Logger('utils.config')


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
    __logger.INFO(f"Loading environment: {env}")
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
            __logger.INFO(f"Loaded environment from {env_file}")
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
        self.__logger = Logger('utils.config')
                     
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
            self.__logger.DEBUG(f'{key} = {value}')
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
    if ':' in interpolation_str:
        var_name, default_value = interpolation_str.split(':', 1)
    else:
        # 如果没有提供默认值，则设为空字符串
        var_name, default_value = interpolation_str, ''
    # 从环境变量获取值，如果不存在则使用默认值
    return os.environ.get(var_name, default_value)

# 在加载配置之前注册解析器
OmegaConf.register_new_resolver("env", ___resolve_custom_env_var)


class YamlConfigation:    
    _lock: any = threading.Lock()
    _MODEL_CACHE: dict[str, any] = {}
    
    @staticmethod
    def getConfiguration()->Self:
        return next(iter(YamlConfigation._MODEL_CACHE.values()))
    
    @staticmethod
    def loadConfiguration(yaml_path:str=None)->Self:
        __logger = Logger('utils.config')
        
        if yaml_path in YamlConfigation._MODEL_CACHE:               # 快速路径无锁
            __logger.WARN('Load Configuration from memory')
            return YamlConfigation._MODEL_CACHE[yaml_path]

        with YamlConfigation._lock:                            # 并发加载保护
            if yaml_path not in YamlConfigation._MODEL_CACHE:       # 二次检查
                YamlConfigation._MODEL_CACHE[yaml_path] = YamlConfigation(yaml_path)
                __logger.WARN('Load Configuration from local')
            return YamlConfigation._MODEL_CACHE[yaml_path]
        
    def __init__(self, yaml_path, **kwargs):            
        # 加载 YAML 配置（支持 ${} 占位符）    
        c = OmegaConf.load(yaml_path)    
        # OmegaConf.resolve(c)
        self.__config = OmegaConf.to_container(c, resolve=True)
        
    def getConfig(self, prefix:str=None)->dict:
        c = self.__config
        if str_isEmpty(prefix):
            return c
        else:            
            return getAttrPlus(c,prefix)
        
    def getStr(self, key, dv:str=None)->str:
        c = self.__config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return dv
        else:
            return str(obj)
        
    def getBool(self, key, dv:int=None)->bool:
        c = self.__config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return dv
        else:
            return str2Bool(str(obj))
        
    def getInt(self, key, dv:int=None)->int:
        c = self.__config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return dv
        else:
            return int(str2Num(str(obj)))
        
    def getFloat(self, key, dv:float=None)->float:
        c = self.__config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return dv
        else:
            return str2Num(str(obj))
        
    def getList(self, key)->List:
        c = self.__config
        obj = getAttrPlus(c, key, None)
        if str_isEmpty(obj):
            return []
        else:
            return list(obj).copy()
    

if __name__ == "__main__":
    yaml_path = 'conf/application.yaml'
    
    config = YamlConfigation.loadConfiguration(yaml_path)
    config = YamlConfigation.loadConfiguration(yaml_path)
    
    print(config.getConfig())
    
    print(f'server={config.getConfig('server')} server.port={config.getConfig('server.port')}')
    