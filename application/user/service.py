from application.user.dao import UserMapper
from dataflow.module import Context

@Context.Service()
class UserService:
    userMapper:UserMapper = Context.Autowired()
            
    def loadUserByUsername(self, username:str)->str:        
        user:dict = self.userMapper.selectUserByUserName(username)
        if not user:
            raise Context.ContextException(f'登录用户：{username} 不存在.')
        if str(user['del_flag']) == '1':
            raise Context.ContextException(f'登录用户：{username} 已被删除.')
        if str(user['status']) == '1':
            raise Context.ContextException(f'登录用户：{username} 已被停用.')
        
        return user