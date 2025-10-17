from pathlib import Path
from dataflow.utils.log import Logger

_logger = Logger('dataflow.utils.file')

def get_file_with_profile(path: str | Path, profile: str = "dev") -> Path:
    """
    在文件名与扩展名之间插入 `-profile`；
    若无扩展名，则直接拼接 `-profile`。
    """
    p = Path(path)
    
    if profile is None or profile.strip() == '':
        return p
    
    suffix = p.suffix          # 含点号，如 '.yaml'
    name = p.stem              # 纯文件名，不含后缀
    new_name = f"{name}-{profile}{suffix}" if suffix else f"{name}-{profile}"
    return p.with_name(new_name)



if __name__ == "__main__":
    print(get_file_with_profile('conf/application.yml'))