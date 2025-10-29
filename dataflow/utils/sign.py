from dataflow.utils.utils import r_bytes, str_isEmpty
import base64 
import urllib
import hashlib
import bcrypt
from Crypto.Cipher import AES
from Crypto.Cipher import DES3
from Crypto.Util.Padding import pad, unpad

def b64_encode(s: str|bytes) -> str:
    if isinstance(s, str):
        return base64.b64encode(s.encode()).decode()
    else:
        return base64.b64encode(s).decode()
    
def b64_decode(s: str) -> str:
    return base64.b64decode(s).decode()

def url_encode(s: str) -> str:
    return urllib.parse.quote(s, safe='')

def url_decode(s: str) -> str:
    return urllib.parse.unquote(s)

def b64url_encode(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip('=')

def b64url_decode(s: str) -> str:
    s += '=' * (4 - len(s) % 4)          # 补 padding
    return base64.urlsafe_b64decode(s).decode()

def md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()

def md5_salt(text: str, salt:str='dataflow') -> str:
    return hashlib.md5((text + salt).encode()).hexdigest()


def encode_password(plain_pwd: str) -> str:
    # 生成盐并加密， rounds=10 与 Spring Security 默认一致
    salt = bcrypt.gensalt(rounds=10)
    hashed = bcrypt.hashpw(plain_pwd.encode('utf-8'), salt)
    return hashed.decode('utf-8')   # 存库字段 varchar(60) 即可

def bcrypt_encode(plain_pwd: str) -> str:
    # 生成盐并加密， rounds=10 与 Spring Security 默认一致
    salt = bcrypt.gensalt(rounds=10)
    hashed = bcrypt.hashpw(plain_pwd.encode('utf-8'), salt)
    return hashed.decode('utf-8')   # 存库字段 varchar(60) 即可

def matches(plain_pwd: str, encoded_pwd: str) -> bool:
    return bcrypt.checkpw(plain_pwd.encode('utf-8'),
                          encoded_pwd.encode('utf-8'))

_iv = r_bytes(b'iv16 of dataflow.io', 16, b'\0', True)    # AES-CBC模式需要16字节的IV
_iv8 = r_bytes(b'dataflow.io', 8, b'\0', True)   # AES-CBC模式需要16字节的IV

def aes_cbc_encrypt(plain: str, key: str, iv:str=None) -> str:
    key32 = r_bytes(key.encode(), 32, b'\0', True)
    if str_isEmpty(iv):
        iv16  = _iv
    else:
        iv16 = r_bytes(iv.encode(), 16, b'\0', True)
    cipher = AES.new(key32, AES.MODE_CBC, iv16)
    return base64.b64encode(cipher.encrypt(pad(plain.encode(), AES.block_size))).decode()


def aes_cbc_decrypt(ct: str, key: str, iv:str=None) -> str:    
    key32 = r_bytes(key.encode(), 32, b'\0', True)
    if str_isEmpty(iv):
        iv16  = _iv
    else:
        iv16 = r_bytes(iv.encode(), 16, b'\0', True)
        
    cipher = AES.new(key32, AES.MODE_CBC, iv16)
    return unpad(cipher.decrypt(base64.b64decode(ct)), AES.block_size).decode()

# ECB 模式（无 iv）
def aes_ecb_encrypt(plain: str, key: str) -> str:
    key32 = r_bytes(key.encode(), 32, b'\0', True)
    cipher = AES.new(key32, AES.MODE_ECB)
    return base64.b64encode(cipher.encrypt(pad(plain.encode(), AES.block_size))).decode()

def aes_ecb_decrypt(ct: str, key: str) -> str:
    key32 = r_bytes(key.encode(), 32, b'\0', True)
    cipher = AES.new(key32, AES.MODE_ECB)
    return unpad(cipher.decrypt(base64.b64decode(ct)), AES.block_size).decode()


def des_encrypt(plain: str, key: str) -> str:
    key24 = r_bytes(key.encode(), 24, b'\0', True)    
    cipher = DES3.new(key24, DES3.MODE_CBC, _iv8)
    return base64.b64encode(cipher.encrypt(pad(plain.encode(), DES3.block_size))).decode()

def des_decrypt(ct: str, key: str) -> str:
    key24 = r_bytes(key.encode(), 24, b'\0', True)    
    cipher = DES3.new(key24, DES3.MODE_CBC, _iv8)
    return unpad(cipher.decrypt(base64.b64decode(ct)), DES3.block_size).decode()

if __name__ == '__main__':
    s = 'hello world 2025 中国无敌'
    k = 'dataflow.io'
    print('MD5      :', md5(s))
    print('MD5+salt :', md5_salt(s))
    print('Base64   :', b64_encode(s), '->', b64_decode(b64_encode(s)))
    print('URL      :', url_encode(s), '->', url_decode(url_encode(s)))
    print('AES-CBC  :', aes_cbc_encrypt(s, k), '->', aes_cbc_decrypt(aes_cbc_encrypt(s, k), k))
    print('AES-ECB  :', aes_ecb_encrypt(s, k), '->', aes_ecb_decrypt(aes_ecb_encrypt(s, k), k))
    print('DES-CBC  :', des_encrypt(s, k), '->', des_decrypt(des_encrypt(s, k), k))
    
    
    print('MD5+salt :', md5_salt('admin123'))
    # 示例
    encoded = encode_password("123456")
    print(f'encode_password={encoded}')
    print(matches("123456", encoded))  # True
    print(matches("wrong", encoded))    # False



