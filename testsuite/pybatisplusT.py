# from dataflow.module.context.pybatisplus import Mapper



# @Mapper(table='sys_user',id_col='user_id')
# class TestMapper:    
#     def __init__(self, name:str='LiuYong'):
#         self.name = name
        
#     def say(self, word)->str:
#         return f'{self.name} Say: {word}'
    
#     def run(self, road)->str:
#         return f'{self.name} run: {road}'

# if __name__ == "__main__":
#     print('== start')
#     t = TestMapper('LiuYong')
#     print(dir(t))
    
#     print(t.select_by_id('2'))
#     print(f'Say={t.say('Liuyong')}')
#     print(f'Run={t.run('Shenzhen')}')
    
#     t = TestMapper('Dataflow')
#     print(t.select_by_id('2'))
#     print(f'Say={t.say('Liuyong')}')
#     print(f'Run={t.run('Shenzhen')}')
    