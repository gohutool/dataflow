import importlib.util
from dataflow.utils.log import Logger

_logger = Logger('dataflow.module.context')

__ignore__ = []


if not importlib.util.find_spec("pymilvus"):
    _logger.WARN('没有pymilvus环境，不加载milvus模块，或者pip install pymilvus')
    __ignore__.append('milvus')
    
if not importlib.util.find_spec("confluent_kafka"):
    _logger.WARN('没有confluent_kafka环境，不加载kafka模块，或者pip install confluent_kafka')
    __ignore__.append('kafka')
    
if not importlib.util.find_spec("langfuse"):
    _logger.WARN('没有langfuse环境，不加载langfuse模块，或者pip install langfuse')
    __ignore__.append('langfuse')
    
if (not importlib.util.find_spec("prometheus_client")) or (not importlib.util.find_spec("prometheus_client")):
    _logger.WARN('没有prometheus_client环境或者，不加载metrics模块，或者pip install prometheus_client')
    __ignore__.append('metrics')
    
if not importlib.util.find_spec("redis"):
    _logger.WARN('没有redis环境，不加载redis模块，或者pip install redis')
    __ignore__.append('redis')
    
if not importlib.util.find_spec("etcd3"):
    _logger.WARN('没有etcd3环境，不加载etcd模块，或者pip install etcd3')
    __ignore__.append('milvus')    
