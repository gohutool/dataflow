from minio import Minio
from minio.error import S3Error
import uuid
from typing import List
from dataflow.utils.reflect import to_dict
# from minio.helpers import ObjectWriteResult
# import urllib3

client = Minio(
    # "minio.ginghan.com:29900",                # ① 地址
    "minio-api.ginghan.com",                # ① 地址
    # "minio-api.ginghan.com:443",                # ① 地址
    # access_key="5wqA5smHTMtijlgONWi2",
    access_key="f8o8So4ZLflCXXDZhDmC",         # ② 账号
    secret_key="tWgKceUOHMA1wSzUuM8xc5lF4M5H3Sl3IqSS9PDe",         # ③ 密码
    # access_key="liuyong",         # ② 账号
    # secret_key="11111111",         # ③ 密码
    # access_key="minioadmin",         # ② 账号
    # secret_key="minioadmin",         # ③ 密码
    secure=True,                     # ④ 本地 http 关掉 TLS
    # http_client=urllib3.PoolManager(cert_reqs='CERT_NONE')
)

class MiniTools:
    def __init__(self,
                endpoint: str,
                access_key: str | None = None,
                secret_key: str | None = None, **kwargs):
        self.config = kwargs.copy()
        self.config['access_key'] = access_key
        self.config['secret_key'] = secret_key
        
        self.__client =  Minio(                
                                endpoint,                # ① 地址                
                                access_key,         # ② 账号
                                secret_key,         # ③ 密码
                                **kwargs 
                            )

    def list_objects(self, bucket_name:str, prefix:str=None, recursive:bool=True, **kwargs )->List[any]:
        rtn = []
        for obj in self.__client.list_objects(bucket_name, prefix=prefix, recursive=recursive, **kwargs):
            rtn.append(to_dict(obj))
        return rtn

    def upload_object(self, bucket_name:str, file_id:str, file_path:str):
        result = self.__client.fput_object(bucket_name, file_id, file_path)
        return to_dict(result)

    def download_object(self, bucket_name:str, file_id:str, file_path:str):
        result = self.__client.fget_object(bucket_name, file_id, file_path)
        return to_dict(result)

    def create_bucket(self, bucket_name:str)->bool:
        try:
            self.__client.make_bucket(bucket_name)
            return True
        except S3Error as e:
            if e.code == "BucketAlreadyOwnedByYou":
                return False
            else:
                raise


if __name__ == "__main__":
    bucket = "dataflow"
    
    minio = MiniTools(        
        "minio-api.ginghan.com",            
        access_key="f8o8So4ZLflCXXDZhDmC",         # ② 账号
        secret_key="tWgKceUOHMA1wSzUuM8xc5lF4M5H3Sl3IqSS9PDe",         # ③ 密码
        secure=True
    )

    # 1) 建桶（已存在会抛异常，忽略即可）
    # try:
    #     minio.create_bucket(bucket)
    #     print("✅ 桶创建成功 /"+bucket)
    # except S3Error as e:
    #     if e.code == "BucketAlreadyOwnedByYou":
    #         print("桶已存在，复用")
    #     else:
    #         raise
    if minio.create_bucket(bucket):
        print("✅ 桶创建成功 /"+bucket)
    else:
        print("桶已存在，复用")

    # 2) 上传
    # uid = uuid.uuid4()
    # client.fput_object(bucket, f"hello-{uid}", "./.env.local")
    # print("✅ 上传完成")
    uid = uuid.uuid4()
    rtn = minio.upload_object(bucket, f"hello-{uid}", "./.env.local")
    print(f"✅ 上传完成 {rtn}")

    # 3) 下载
    # client.fget_object(bucket, f"hello-{uid}", "./down.txt")
    # print("✅ 下载完成，内容：", open("./down.txt").read())
    rtn = minio.download_object(bucket, f"hello-{uid}", "./down.txt")
    print(f"✅ 下载完成{rtn} \n类型：{rtn['content_type']}\n{open("./down.txt").read()}")

    # 4) 列举
    # for obj in client.list_objects(bucket, recursive=True):
    #     print("对象:", obj.object_name, "大小:", obj.size)
    for obj in minio.list_objects(bucket, recursive=True):
        print(f'对象:{obj['object_name']}, 大小:{obj['size']}, {obj['bucket_name']}, {obj['last_modified']}')