
class AppReponseVO:
    # status: bool = Field(True, description="响应状态")
    # msg: str = Field('成功', description="返回消息")
    # data: Any = Field(None, description="返回数据")
    def __init__(self, status:bool=True, msg:str='成功', code:int=200, data:dict=None): 
        self.status = status
        self.msg = msg
        self.data = data
        self.code = code
        
    # ① 供 Pydantic 序列化
    def dict(self) -> dict:
        rtn =  {
            "status": self.status,
            "msg": self.msg,            
            "code": self.code,
        }
        if self.data:
            rtn.update(self.data)
        return rtn
        
    def __repr__(self):
        """
        定义对象的字符串表示。
        """
        return (f"AppReponseVO(status={self.status}, code={self.code}, msg={self.msg}, data={self.data}")


