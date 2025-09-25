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
from dataflow.utils.utils import str2Num


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
    
    def getStr(self, env, dv:Optional[int]=0)->str:
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
