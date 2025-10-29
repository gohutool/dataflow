from dataflow.module.context.pybatisplus import Mapper,Selete,Update  # noqa: F401
from dataflow.module import Context
from dataflow.utils.sign import matches,encode_password
from dataflow.utils.utils import get_str_from_dict, date2str_yyyymmddddmmss, date_datetime_cn # noqa: F401

# @Mapper(table='t_user', id_col='username')
class EtcdV3Mapper:
    @Selete(datasource='ds04', sql='select * from t_user where username=:username')
    def selectUserByUserName(self, username:str)->dict:
        pass
    
    @Update(datasource='ds04', sql='update t_user set password=:password where username=:username')
    def modifyPwd(self, username:str, password:str)->int:
        pass
    
    @Selete(datasource='ds04', sql='select * from t_config order by node_name asc')
    def getallconfig(self)->list:
        pass

@Context.Service()
class EtcdV3Service:
    userMapper:EtcdV3Mapper=EtcdV3Mapper()
    
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
    
    def getallconfig(self)->dict:
        rtn = {}
        datas = self.userMapper.getallconfig()
        
        rtn['nodes'] = datas
        # for one in list:
        #     one:dict = one
        #     rtn[get_str_from_dict(one, 'id')] = one
        rtn['updatetime'] = date2str_yyyymmddddmmss(date_datetime_cn())
        
        return rtn
