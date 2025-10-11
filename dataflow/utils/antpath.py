from antpathmatcher import AntPathMatcher
from dataflow.utils.utils import str_isEmpty
import os 
from pathlib import Path
import re

_matcher = AntPathMatcher()


def match(pattern, txt):
    return _matcher.match(pattern, txt)

def find(root:str, pattern:str=None)->list[tuple[str, Path]]:
    root_p = str(Path(root).absolute()).replace('\\', '/')
    
    # print(f'============== {root_p}')
    
    if not str_isEmpty(pattern) and not pattern.startswith('/'):
        pattern = '/' + pattern
    # print(f'========= {root_p}')
    
    rtn = []
    needP = False if str_isEmpty(pattern) else True
    for dirpath, dirnames, filenames in os.walk(root):
        p = Path(dirpath)
        p_s_o = str(p.absolute()).replace('\\', '/')
        p_s = re.sub(r'^'+root_p+'/', '/', p_s_o)        
        # print(f'dir={p_s_o} {p_s}, {root_p}')
        if not needP or match(pattern, p_s):
            if p_s:
                rtn.append((p_s, p))
        
        for f in filenames:
            p1 = Path(dirpath+ '/' + f)
            p1_s_o = str(p1.absolute()).replace('\\', '/')
            p1_s = re.sub(r'^'+root_p+'/', '/', p1_s_o)
            # print(f'file={p1_s_o} {p1_s}, {root_p}')
            if not needP or match(pattern, p1_s):
                rtn.append((p1_s, p1))
                
    return rtn

        
if __name__ == "__main__":    
    matcher = AntPathMatcher()
    def test_match(str1,str2):
        print(f'matcher.match("{str1}", "{str2}") = {matcher.match(str1, str2)}')       # 输出: True
        
    test_match("/api/?", "/api/d")       # 输出: True
    test_match("/api/?", "/api/dd")      # 输出: False
    test_match("/api/*", "/api/data")    # 输出: True
    test_match("/api/*", "/api/data-test.jsp")    # 输出: True
    test_match("/api/**", "/api/data/info") # 输出: True    
    test_match("/api/**", "/api/data/test.jsp")    # 输出: True
    test_match("/api/**", "/api/") # 输出: True    
    test_match("/api/**", "/api") # 输出: True    
    test_match("*/api/**", "/aaa/api/") # 输出: True    
    test_match("*/api/**", "aaa/api/") # 输出: True    
    test_match("**/api/**", "/test/aaa/api/") # 输出: True    
    
    # for dirpath, dirnames, filenames in os.walk('./dataflow/utils'):
    #     p = Path(dirpath)
    #     print(f'{p}')
        
    #     for f in filenames:
    #         p1 = Path(dirpath+ '/' + f)
    #         print(f'{p1} {str(p1).replace('\\', '/')}')
            
    rtn = find('./conf', '**/user*.xml')
    for o in rtn:
        print(o)