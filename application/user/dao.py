from dataflow.module.context.pybatisplus import Mapper

@Mapper(table='sys_user', id_col='user_id')
class UserMapper:
    def selectUserByUserName(self, userName:str)->dict:
        pass