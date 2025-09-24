import uvicorn
from dataflow.utils.logtools import Logger, initLogWithYaml
import os
import time

# 设置时区（必须在导入其他时间相关模块前设置）
os.environ["TZ"] = "Asia/Shanghai"
if hasattr(time, 'tzset'):          # Unix / macOS / WSL
    time.tzset()
    
# if os.name == 'posix':    
#     time.tzset()  # 使时区生效（仅 Unix 系统有效）

port=45080

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

initLogWithYaml('conf/logback.yaml')

_logger = Logger()

if __name__ == "__main__":
    _logger.INFO(f"Start server on {port}")
    uvicorn.run("dataflow.endpoint:app", host="0.0.0.0", port=port, reload=False, workers=1)
    _logger.INFO(f"End server on {port}")
    # try:
    #     uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
    # except KeyboardInterrupt:
    #     print('CTRL+C to quit')
    # except Exception as e:
    #     print('Exit 1 with error {e}', e)

