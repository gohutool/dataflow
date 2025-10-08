from dataflow.module.context.pybatisplus import Mapper



@Mapper(table='sys_user',id_col='user_id')
class TestMapper:
    pass

# 

if __name__ == "__main__":
    print('== start')
    t = TestMapper()
    print(dir(t))
    
    print(t.select_by_id('2'))
    t.say('Liuyong')