from dataflow.module.context.pybatisplus import Mapper,Selete,Update  # noqa: F401
from dataflow.module import Context
from dataflow.utils.sign import matches,encode_password
from dataflow.utils.utils import get_str_from_dict, date2str_yyyymmddddmmss, date_datetime_cn,str_to_json,json_to_str # noqa: F401


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
    
    @Update(datasource='ds04', sql='insert into t_config(id,node_name,node_host,node_port,node_demo,createtime) values(:id,:node_name,:node_host,:node_port,:node_demo,:createtime)')
    def addconfig(self, id, node_name, node_host, node_port, node_demo, createtime)->int:
        pass
    
    @Update(datasource='ds04', sql='''
            update t_config set node_name=:node_name,node_host=:node_host,node_port=:node_port,
            node_demo=:node_demo,updatetime=:updatetime where id=:id
            ''')
    def updateconfig(self, id, node_name, node_host, node_port, node_demo, updatetime)->int:
        pass
    
    @Selete(datasource='ds04', sql='select * from t_config where id=:id')
    def getconfig(self, id)->dict:
        pass
    
    @Update(datasource='ds04', sql='delete from t_config where id=:id')
    def deleteconfig(self, id)->int:
        pass
    
    @Selete(datasource='ds04', sql='select * from t_search where config_id=:id order by search_name asc')
    def getsearchs(self, id)->list:
        pass
    
    @Selete(datasource='ds04', sql='select * from t_search where config_id=:id and search_id=:search_id')
    def getsearch(self, id, search_id:str)->dict:
        pass
        
    @Update(datasource='ds04', sql='delete from t_search where config_id=:id and search_id=:search_id')
    def deletesearch(self, id:str, search_id:str)->int:
        pass
    
    @Update(datasource='ds04', sql='delete from t_search where config_id=:id')
    def deletesearchByConfig(self, id:str)->int:
        pass
    
    @Update(datasource='ds04', sql='''insert into t_search(config_id,search_id,search_name,search_content,createtime) 
            values(:config_id,:search_id,:search_name,:search_content,:createtime)
            ''')
    def addsearch(self, config_id, search_id, search_name, search_content, createtime)->int:
        pass
     
    @Update(datasource='ds04', sql='''
            update t_search set search_name=:search_name,search_content=:search_content,
            updatetime=:updatetime where config_id=:config_id and search_id=:search_id
            ''')
    def updatesearch(self, config_id, search_id, search_name, search_content, updatetime)->int:
        pass
    
    
    @Update(datasource='ds04', sql='''insert into t_group(config_id,group_content,createtime) 
            values(:config_id,:group_content,:createtime)
            ''')
    def addgroup(self, config_id, group_content, createtime)->int:
        pass
    
    @Update(datasource='ds04', sql='''update t_group set group_content=:group_content,
            updatetime=:updatetime where config_id=:config_id
            ''')
    def updategroup(self, config_id, group_content, updatetime)->int:
        pass
    
    @Update(datasource='ds04', sql='''delete from t_group where config_id=:config_id
            ''')
    def deletegroup(self, config_id)->int:
        pass
    
    @Selete(datasource='ds04', sql='select * from t_group where config_id=:id')
    def getgroup(self, id)->dict:
        pass

@Context.Service()
class EtcdV3Service:
    userMapper:EtcdV3Mapper=EtcdV3Mapper()
    
    def login(self, username:str, password:str)->bool:
        one = self.userMapper.selectUserByUserName(username)
        if not one:
            raise Context.ContextException(f'没有{username}用户')
        
        if not matches(password, one['password']):
            raise Context.ContextException(f'没有{username}用户密码不匹配')
        
        return True
    
    def modifypwd(self, username:str, password:str, newpassword:str)->int:
        one = self.userMapper.selectUserByUserName(username)
        if not one:
            raise Context.ContextException(f'没有{username}用户')
        
        if not matches(password, one['password']):
            raise Context.ContextException(f'没有{username}用户密码不匹配')
        
        newpassword = encode_password(newpassword)
        
        rtn = self.userMapper.modifyPwd(username, newpassword)
        
        return rtn
    
    def getallconfig(self)->dict:
        rtn = {}
        datas = self.userMapper.getallconfig()
        
        for data in datas:
            s = self.userMapper.getsearchs(data['id'])
            searchs = []
            for o in s:
                one = str_to_json(o['search_content'])
                searchs.append(one)
            data['search'] = searchs
            g = self.userMapper.getgroup(data['id'])
            if g:
                data['group'] = str_to_json(g['group_content'])
            else:
                data['group'] = []
        
        rtn['nodes'] = datas
        # for one in list:
        #     one:dict = one
        #     rtn[get_str_from_dict(one, 'id')] = one
        rtn['updatetime'] = date2str_yyyymmddddmmss(date_datetime_cn())
        
        return rtn
    
    def removeoneconfig(self, id:str)->int:
        self.userMapper.deletesearchByConfig(id)
        self.userMapper.deletegroup(id)
        return self.userMapper.deleteconfig(id)
    
    def saveoneconfig(self, config:dict)->int:
        id = get_str_from_dict(config, 'id')
        node_name = get_str_from_dict(config, 'node_name')
        node_host = get_str_from_dict(config, 'node_host')
        node_port = get_str_from_dict(config, 'node_port')
        node_demo = get_str_from_dict(config, 'node_demo')
        
        ctime = date2str_yyyymmddddmmss(date_datetime_cn())        
        old = self.userMapper.getconfig(id)
        
        cnt = 0
        if old:
            cnt = self.userMapper.updateconfig(id, node_name, node_host, node_port, node_demo, ctime)
        else:
            cnt = self.userMapper.addconfig(id, node_name, node_host, node_port, node_demo, ctime)
        
        return cnt
    
    def saveonesearch(self, id:str, search:dict)->int:
        
        search_id = get_str_from_dict(search, 'id')
        search_name = get_str_from_dict(search, 'search_name')
        
        ctime = date2str_yyyymmddddmmss(date_datetime_cn())        
        old = self.userMapper.getsearch(id, search_id)
        cnt = 0
        if old:
            cnt = self.userMapper.updatesearch(id, search_id, search_name, json_to_str(search), ctime)
        else:
            cnt = self.userMapper.addsearch(id, search_id, search_name, json_to_str(search), ctime)
            
        return cnt
    
    def removeonesearch(self, id:str,search_id:str)->int:
        return self.userMapper.deletesearch(id, search_id)
    
    def savegroup(self, id:str, group:list)->int:
        ctime = date2str_yyyymmddddmmss(date_datetime_cn())
        old = self.userMapper.getgroup(id)
        cnt = 0
        if old:
            cnt = self.userMapper.updategroup(id, json_to_str(group), ctime)
        else:
            cnt = self.userMapper.addgroup(id, json_to_str(group), ctime)            
        return cnt
        




