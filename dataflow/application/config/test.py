from dataflow.module.context import Context


@Context.Configurationable(prefix='context.test')
def config_all(config):
    print(f'========={config}')
    pass

config_all()