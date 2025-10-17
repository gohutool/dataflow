import uvicorn
from dataflow.utils.log import Logger, initLogWithYaml
from dataflow.utils.config import YamlConfigation  # noqa: F401
import logging

_logger = Logger()


# 设置时区（必须在导入其他时间相关模块前设置）
# os.environ["TZ"] = "Asia/Shanghai"
# if hasattr(time, 'tzset'):          # Unix / macOS / WSL
#     time.tzset()
    
    
# if os.name == 'posix':    
#     time.tzset()  # 使时区生效（仅 Unix 系统有效）

# port=45080

### USE python3.6.8
# async def run_server():
#     config = Config("main:app", host="0.0.0.0", port=port)
#     server = Server(config)
#     await server.serve()

# print(f'Start http server on {port}')
# # 在 Python 3.6.8 中运行
# try:
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(run_server())
# except KeyboardInterrupt:
#     print('CTRL+C to quit')
# except Exception as e:
#     print('Exit 1 with error {e}', e)

### USE python3.12.10

# initLogWithYaml('conf/logback.yaml')



class ApplicationBoot:
    scan:str
    application_yaml:str
    @staticmethod
    def Start(application_yaml:str='conf/application.yaml', scan:str|list[str]='application.**', cmd_args:dict={}):
        ApplicationBoot.application_yaml = application_yaml
        ApplicationBoot.scan = scan
        
        _c:YamlConfigation = YamlConfigation.loadConfiguration(application_yaml)
        _c.merge(cmd_args)
        
        host = _c.getStr('application.server.host', 'localhost')
        workers = _c.getInt('application.server.workers', 1)
        
        port = _c.getInt('application.server.port', 9000)

        log_config = _c.getStr('logging.config', None)
        if  log_config is not None and  log_config.strip()!='':
            initLogWithYaml(log_config)
            print(f'LOG Config : {log_config}')

        log_level = _c.getStr('logging.level', None)
        if  log_level is not None and  log_level.strip()!='':
            logging.basicConfig(level=log_level)
            print(f'LOG Level : {log_level}')

        _logger = Logger()
                
        _logger.INFO(f"{_c.getStr('application.name', 'DataFlow Application')} {_c.getStr('application.version', '1.0.0')} Start server on {host}:{port}")
        uvicorn.run("dataflow.router.endpoint:app", host=host, port=port, reload=False, workers=workers, headers=[("Server", "my-server/1.0")])
        _logger.INFO(f"{_c.getStr('application.name', 'DataFlow Application')} {_c.getStr('application.version', '1.0.0')} End server on {host}:{port}")        