import os
import time
from dataflow.boot import ApplicationBoot
import sys

# 设置时区（必须在导入其他时间相关模块前设置）
os.environ["TZ"] = "Asia/Shanghai"
if hasattr(time, 'tzset'):          # Unix / macOS / WSL
    time.tzset()
    

def parse_long_args(argv=sys.argv[1:]) -> dict[str, any]:
    """
    把 --a.b.c=value 变成 {'a': {'b': {'c': value}}}
    支持自动类型推断 int/float/bool
    """
    def cast(v: str):
        if v.isdigit(): return int(v)
        try: return float(v)
        except ValueError: pass
        if v.lower() in ('true', 'false'): return v.lower() == 'true'
        return v

    cfg: dict = {}
    for token in argv:
        if not token.startswith('--') or '=' not in token:
            continue
        k, v = token[2:].split('=', 1)
        keys = k.split('.')
        d = cfg
        for kk in keys[:-1]:
            d = d.setdefault(kk, {})
        d[keys[-1]] = cast(v)
    return cfg    
    
if __name__ == "__main__":
    ApplicationBoot.Start(cmd_args=parse_long_args)
