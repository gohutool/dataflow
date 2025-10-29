from dataflow.module.context.pybatisplus import Mapper,Selete,Update  # noqa: F401
from dataflow.module import Context
from dataflow.utils.sign import matches,encode_password

# @Mapper(table='t_user', id_col='username')
class UserMapper:
    @Selete(datasource='ds04', sql='select * from t_user where username=:username')
    def selectUserByUserName(self, username:str)->dict:
        pass
    
    @Update(datasource='ds04', sql='update t_user set password=:password where username=:username')
    def modifyPwd(self, username:str, password:str)->int:
        pass
    

@Context.Service()
class UserService:
    userMapper:UserMapper=UserMapper()
    
    def login(self, username:str, password:str)->bool:
        one = self.userMapper.selectUserByUserName(username)
        if not one:
            raise Context.ContextExceptoin(f'没有{username}用户')
        
        if not matches(password, one['password']):
            raise Context.ContextExceptoin(f'没有{username}用户密码不匹配')
        
        return True
    
    def modifypwd(self, username:str, password:str, newpassword:str)->int:
        one = self.userMapper.selectUserByUserName(username)
        if not one:
            raise Context.ContextExceptoin(f'没有{username}用户')
        
        if not matches(password, one['password']):
            raise Context.ContextExceptoin(f'没有{username}用户密码不匹配')
        
        newpassword = encode_password(newpassword)
        
        rtn = self.userMapper.modifyPwd(username, newpassword)
        
        return rtn
