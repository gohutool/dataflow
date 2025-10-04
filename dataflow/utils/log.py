import logging
import logging.config
import yaml
import traceback
import inspect
import os
from dataflow.utils.utils import date_datetime_cn, date2str_yyyymmddhhmmsss

_old_debug = logging.Logger.debug
_old_info = logging.Logger.info
_old_warn = logging.Logger.warn
_old_warning = logging.Logger.warning
_old_error = logging.Logger.error
_old_critical = logging.Logger.critical

def _get_time_cn():
    return date2str_yyyymmddhhmmsss(date_datetime_cn())
    
def _debug(self, msg, *args, **kwargs):    
    caller_frame = inspect.currentframe().f_back
    caller_filename = caller_frame.f_code.co_filename
    caller_lineno = caller_frame.f_lineno
    asctime_cn = _get_time_cn()
    if 'extra' not in kwargs or '_filename' not in kwargs['extra']:
        kwargs['extra'] = {'_filename': os.path.basename(caller_filename),
                                            '_lineno': caller_lineno, 
                                            '_full_filename':caller_filename,
                                            'asctime_cn':asctime_cn
                                            }
    _old_debug(self, msg, *args, **kwargs)
    
def _info(self, msg, *args, **kwargs):    
    caller_frame = inspect.currentframe().f_back
    caller_filename = caller_frame.f_code.co_filename
    caller_lineno = caller_frame.f_lineno
    asctime_cn = _get_time_cn()
    if 'extra' not in kwargs or '_filename' not in kwargs['extra']:
        kwargs['extra'] = {'_filename': os.path.basename(caller_filename),
                                            '_lineno': caller_lineno, 
                                            '_full_filename':caller_filename,
                                            'asctime_cn':asctime_cn
                                            }
    _old_info(self, msg, *args, **kwargs)
    
    
def _warn(self, msg, *args, **kwargs):    
    caller_frame = inspect.currentframe().f_back
    caller_filename = caller_frame.f_code.co_filename
    caller_lineno = caller_frame.f_lineno
    asctime_cn = _get_time_cn()
    if 'extra' not in kwargs or '_filename' not in kwargs['extra']:
        kwargs['extra'] = {'_filename': os.path.basename(caller_filename),
                                            '_lineno': caller_lineno, 
                                            '_full_filename':caller_filename,
                                            'asctime_cn':asctime_cn
                                            } 
    _old_warn(self, msg, *args, **kwargs)
    
    
def _warning(self, msg, *args, **kwargs):    
    caller_frame = inspect.currentframe().f_back
    caller_filename = caller_frame.f_code.co_filename
    caller_lineno = caller_frame.f_lineno
    asctime_cn = _get_time_cn()
    if 'extra' not in kwargs or '_filename' not in kwargs['extra']:
        kwargs['extra'] = {'_filename': os.path.basename(caller_filename),
                                            '_lineno': caller_lineno, 
                                            '_full_filename':caller_filename,
                                            'asctime_cn':asctime_cn
                                            }
    _old_warning(self, msg, *args, **kwargs)
    
def _err(self, msg, *args, **kwargs):    
    caller_frame = inspect.currentframe().f_back
    caller_filename = caller_frame.f_code.co_filename
    caller_lineno = caller_frame.f_lineno
    asctime_cn = _get_time_cn()
    if 'extra' not in kwargs or '_filename' not in kwargs['extra']:
        kwargs['extra'] = {'_filename': os.path.basename(caller_filename),
                                            '_lineno': caller_lineno, 
                                            '_full_filename':caller_filename,
                                            'asctime_cn':asctime_cn
                                            }
    _old_error(self, msg, *args, **kwargs)
    
def _critical(self, msg, *args, **kwargs):    
    caller_frame = inspect.currentframe().f_back
    caller_filename = caller_frame.f_code.co_filename
    caller_lineno = caller_frame.f_lineno
    asctime_cn = _get_time_cn()
    if 'extra' not in kwargs or '_filename' not in kwargs['extra']:
        kwargs['extra'] = {'_filename': os.path.basename(caller_filename),
                                            '_lineno': caller_lineno, 
                                            '_full_filename':caller_filename,
                                            'asctime_cn':asctime_cn
                                            }
    _old_critical(self, msg, *args, **kwargs)
    
logging.Logger.debug = _debug
logging.Logger.info = _info
logging.Logger.warning = _warning
logging.Logger.warn = _warn
logging.Logger.critical = _critical
logging.Logger.error = _err

__defaul_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'simple': {'format': '%(levelname)s:[%(_filename)s:%(_lineno)d][%(name)s] - %(asctime)s - %(message)s'}
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
            'level': 'DEBUG',
        }
    },
    'loggers':{        
        'utils': {          # 全局 logger
            'handlers': ['console'],
            'level': 'DEBUG',
        },
    },
    # 'root': {          # 全局 logger
    #     'handlers': ['console'],
    #     'level': 'INFO',
    # }
}

logging.config.dictConfig(__defaul_config)


class CustomLogRecord(logging.LogRecord):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 获取调用者的行号和文件名
        caller_frame = inspect.currentframe().f_back
        self.filename = caller_frame.f_code.co_filename
        self.lineno = caller_frame.f_lineno
        
def initLogWithYaml(config_file='logback.yaml'):
    with open(config_file, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)   
    # 应用日志配置
    logging.config.dictConfig(config)    
    # 替换默认的 LogRecord 工厂
    # logging.setLogRecordFactory(CustomLogRecord)
        
class Logger:
    def __init__(self, logger_name=None):
        self.__logger__ = logging.getLogger(logger_name)
    
    
    def CRITICAL(self, txt):
        # self.__logger__.critical(txt)
        
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = _get_time_cn()
        
        self.__logger__.critical(txt, extra={'_filename': os.path.basename(caller_filename), 
                                         '_lineno': caller_lineno, 
                                         '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })
        
    def FATAL(self, txt):
        # self.__logger__.fatal(txt)
        
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = _get_time_cn()
        
        self.__logger__.fatal(txt, extra={'_filename': os.path.basename(caller_filename), 
                                         '_lineno': caller_lineno, 
                                         '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })
        
    def LOG(self, txt):        
        # self.__logger__.info(txt)        
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = _get_time_cn()
        
        self.__logger__.info(txt, extra={'_filename': os.path.basename(caller_filename),
                                         '_lineno': caller_lineno, 
                                         '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })

    def DEBUG(self, txt):
        # self.__logger__.debug(txt)
        
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = _get_time_cn()
        
        self.__logger__.debug(txt, extra={'_filename': os.path.basename(caller_filename), 
                                          '_lineno': caller_lineno, 
                                          '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })
        
    def WARN(self, txt):
        # self.__logger__.warning(txt)
        
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = _get_time_cn()
        
        
        self.__logger__.warning(txt, extra={'_filename': os.path.basename(caller_filename), 
                                            '_lineno': caller_lineno, 
                                            '_full_filename':caller_filename,
                                            'asctime_cn':asctime_cn
                                            })
        
    def INFO(self, txt):
        # self.__logger__.info(txt)
        
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = _get_time_cn()
        
        self.__logger__.info(txt, extra={'_filename': os.path.basename(caller_filename), 
                                         '_lineno': caller_lineno, 
                                         '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })
        
    def ERROR(self, msg='', e=None):
        # tb = traceback.format_exc()
        # if msg is not None or msg != '':
        #     self.__logger__.error(f"{msg} 发生异常: {e}\n{tb}")
        # else:
        #     self.__logger__.error(f"发生异常: {e}\n{tb}")
        
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        
        asctime_cn = _get_time_cn()
        
        tb = traceback.format_exc()
        if msg is not None or msg != '':
            self.__logger__.error(f"{msg} 发生异常: {e}\n{tb}", 
                                  extra={'_filename': os.path.basename(caller_filename), 
                                         '_lineno': caller_lineno, 
                                         '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })
        else:
            self.__logger__.error(f"发生异常: {e}\n{tb}", 
                                  extra={'_filename': os.path.basename(caller_filename), 
                                         '_lineno': caller_lineno, 
                                         '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })
            
            
# ------------------- demo -------------------
if __name__ == '__main__':
    _logger = Logger('utils.log')    
    _logger.DEBUG('====')            