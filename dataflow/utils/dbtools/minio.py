from minio import Minio
from minio.error import S3Error
import uuid
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


if __name__ == "__main__":
    bucket = "dataflow"

    # 1) 建桶（已存在会抛异常，忽略即可）
    try:
        client.make_bucket(bucket)
        print("✅ 桶创建成功 /"+bucket)
    except S3Error as e:
        if e.code == "BucketAlreadyOwnedByYou":
            print("桶已存在，复用")
        else:
            raise

    # 2) 上传
    uid = uuid.uuid4()
    client.fput_object(bucket, f"hello-{uid}", "./.env.local")
    print("✅ 上传完成")

    # 3) 下载
    client.fget_object(bucket, f"hello-{uid}", "./down.txt")
    print("✅ 下载完成，内容：", open("./down.txt").read())

    # 4) 列举
    for obj in client.list_objects(bucket, recursive=True):
        print("对象:", obj.object_name, "大小:", obj.size)