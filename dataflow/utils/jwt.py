from dataflow.utils.utils import date_datetime_cn
import datetime 
import jwt

SECRET = 'replace-with-256-bit-secret'      # 32 字节以上

def create_token(user_key: str, user_name:str, ttl_minutes: float = 24*60, secret:str=SECRET) -> str:
    
    payload = {
        'uid': user_key,
        'username':user_name,
        'exp': date_datetime_cn() + datetime.timedelta(minutes=ttl_minutes),
        'iat': date_datetime_cn(),
        'scope': 'read write'
    }
    return jwt.encode(payload, secret, algorithm='HS256')

def verify_token(token: str, secret:str=SECRET) -> dict[str, any]:
    try:
        return jwt.decode(token, secret, algorithms=['HS256'])
    except jwt.ExpiredSignatureError:
        raise RuntimeError('token 已过期')
    except jwt.InvalidTokenError:
        raise RuntimeError('token 无效')
    

if __name__ == "__main__":    
    token = create_token(1,'dataflow')
    
    data = verify_token(token)
    print(f'{token}={data}')