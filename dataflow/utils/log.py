import logging
import logging.config
import yaml
import traceback
import inspect
import os
from dataflow.utils.utils import date_datetime_cn, date2str_yyyymmddhhmmsss

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
        
    def ___get_time_cn(self):
        return date2str_yyyymmddhhmmsss(date_datetime_cn())

    def LOG(self, txt):        
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = self.___get_time_cn()
        
        self.__logger__.info(txt, extra={'_filename': os.path.basename(caller_filename),
                                         '_lineno': caller_lineno, 
                                         '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })

    def DEBUG(self, txt):
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = self.___get_time_cn()
        
        self.__logger__.debug(txt, extra={'_filename': os.path.basename(caller_filename), 
                                          '_lineno': caller_lineno, 
                                          '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })
        
    def WARN(self, txt):
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = self.___get_time_cn()
        
        
        self.__logger__.warning(txt, extra={'_filename': os.path.basename(caller_filename), 
                                            '_lineno': caller_lineno, 
                                            '_full_filename':caller_filename,
                                            'asctime_cn':asctime_cn
                                            })
        
    def INFO(self, txt):
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        asctime_cn = self.___get_time_cn()
        
        self.__logger__.info(txt, extra={'_filename': os.path.basename(caller_filename), 
                                         '_lineno': caller_lineno, 
                                         '_full_filename':caller_filename,
                                         'asctime_cn':asctime_cn
                                         })
        
    def ERROR(self, msg='', e=None):
        caller_frame = inspect.currentframe().f_back
        caller_filename = caller_frame.f_code.co_filename
        caller_lineno = caller_frame.f_lineno
        
        asctime_cn = self.___get_time_cn()
        
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