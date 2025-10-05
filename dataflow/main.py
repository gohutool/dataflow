import os
import time
from dataflow.boot import ApplicationBoot

# 设置时区（必须在导入其他时间相关模块前设置）
os.environ["TZ"] = "Asia/Shanghai"
if hasattr(time, 'tzset'):          # Unix / macOS / WSL
    time.tzset()
    
    
if __name__ == "__main__":
    ApplicationBoot.Start()
