import time
import threading
from dataflow.utils.log import Logger
from dataflow.utils.utils import str_isEmpty
from dataflow.utils.reflect import get_methodname
from typing import Optional,Callable,Any, Dict
import functools
import queue
import uuid
from dataclasses import dataclass
from enum import Enum
import atexit


_logger = Logger('dataflow.utils.thread')

def Sleep(second:float):
    time.sleep(second)
    
def getCurrentThread()->threading.Thread:
    me = threading.current_thread()
    return me

def getCurrentThreadName()->str:
    return getCurrentThread().name

def getCurrentThreadId()->int:
    return getCurrentThread().ident

def newThread(func:callable, name:str=None, daemon:bool=None, *args, **kwargs)->threading.Thread:
    t = threading.Thread(target=func, name=name, *args, **kwargs, daemon=daemon)
    return t
    
class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class TaskResult:
    """任务结果"""
    task_id: str
    status: TaskStatus
    result: Any = None
    error: Exception = None
    execution_time: float = 0.0

class ThreadPool:
    """高级线程池，支持任务状态跟踪和结果返回"""
    
    def __init__(self, num_workers: int, max_queue_size: int = 0, name: str = "AdvancedPool"):
        """
        初始化高级线程池
        
        Args:
            num_workers: 工作线程数量
            max_queue_size: 最大队列大小，0表示无限制
            name: 线程池名称
        """
        self.num_workers = num_workers
        self.name = name
        self.task_queue = queue.Queue(maxsize=max_queue_size)
        self.workers = []
        self.shutdown_flag = False
        self.results: Dict[str, TaskResult] = {}
        self.lock = threading.Lock()
        
        self._start_workers()
    
    def _start_workers(self):
        """启动工作线程"""
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"{self.name}-Worker-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
        
        _logger.DEBUG(f"AdvancedThreadPool {self.name} started with {self.num_workers} workers")
    
    def _worker_loop(self):
        """工作线程主循环"""
        thread_name = threading.current_thread().name
        
        while not self.shutdown_flag:
            try:
                # 获取任务
                task_id, func, args, kwargs = self.task_queue.get(timeout=1)
                
                # 更新任务状态
                with self.lock:
                    self.results[task_id].status = TaskStatus.RUNNING
                
                # 执行任务
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    execution_time = time.time() - start_time
                    
                    with self.lock:
                        self.results[task_id].status = TaskStatus.COMPLETED
                        self.results[task_id].result = result
                        self.results[task_id].execution_time = execution_time
                        
                    _logger.DEBUG(f"Worker {thread_name} completed task {task_id} in {execution_time:.2f}s")
                    
                except Exception as e:
                    execution_time = time.time() - start_time
                    
                    with self.lock:
                        self.results[task_id].status = TaskStatus.FAILED
                        self.results[task_id].error = e
                        self.results[task_id].execution_time = execution_time
                    
                    _logger.ERROR(f"Worker {thread_name} failed task {task_id}: {e}")
                    
            except queue.Empty:
                continue
            finally:
                if not self.shutdown_flag:
                    self.task_queue.task_done()
    
    def submit(self, func: Callable, *args, **kwargs) -> str:
        """
        提交任务到线程池
        
        Returns:
            task_id: 任务ID，用于查询结果
        """
        if self.shutdown_flag:
            raise RuntimeError("Cannot submit tasks to stopped ThreadPool")
        
        task_id = str(uuid.uuid4())
        
        # 初始化任务结果
        with self.lock:
            self.results[task_id] = TaskResult(task_id=task_id, status=TaskStatus.PENDING)
        
        # 提交任务
        task = (task_id, func, args, kwargs)
        self.task_queue.put(task)
        
        _logger.DEBUG(f"Task {task_id} submitted to {self.name}")
        return task_id
    
    def get_result(self, task_id: str, timeout: Optional[float] = None) -> Any:
        """
        获取任务结果
        
        Args:
            task_id: 任务ID
            timeout: 超时时间（秒）
            
        Returns:
            任务执行结果
            
        Raises:
            TimeoutError: 超时
            Exception: 任务执行时的异常
        """
        start_time = time.time()
        
        while True:
            with self.lock:
                result = self.results.get(task_id)
                
            if not result:
                raise KeyError(f"Task {task_id} not found")
                
            if result.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                if result.status == TaskStatus.FAILED:
                    raise result.error
                return result.result
            
            # 检查超时
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(f"Timeout waiting for task {task_id}")
            
            time.sleep(0.1)  # 避免忙等待
    
    def wait_all(self, timeout: Optional[float] = None):
        """等待所有任务完成"""
        self.task_queue.join()
    
    def shutdown(self, wait: bool = True):
        """关闭线程池"""
        _logger.DEBUG(f"Shutting down {self.name}")
        self.shutdown_flag = True
        
        if wait:
            self.wait_all()
            for worker in self.workers:
                worker.join()
        
        _logger.DEBUG(f"{self.name} shutdown completed")
    
    def get_pool_status(self) -> dict:
        """获取线程池状态"""
        with self.lock:
            status_count = {
                status: 0 for status in TaskStatus
            }
            for result in self.results.values():
                status_count[result.status] += 1
            
            return {
                'workers': self.num_workers,
                'queue_size': self.task_queue.qsize(),
                'tasks': status_count,
                'shutdown': self.shutdown_flag
            }

# class ThreadInteruptedException(Exception):
#     pass

def LoopDaemonThread(func:callable, name:str=None, sleep:float=1, *args, **kwargs)->threading.Thread:
    obj = {
        'is_running':True
    }    
    @functools.wraps(func)
    def wrap():
        while obj['is_running']:
            try:
                func(*args, **kwargs)
            except Exception as e:                
                _logger.ERROR('Exception Now to exit', e)
                break
            Sleep(sleep)
        _logger.DEBUG('Exit LoopThread')
    
    t = newThread(wrap, name=name, daemon=True)    
    t.start()
    
    def on_exit():
        obj['is_running'] = False
        
    atexit.register(on_exit)
    
    return t
    
def loopThread(name:str=None, sleep:int=1):    
    def decorator(func:Callable):
        _name = name
        if str_isEmpty(_name):
            _name = get_methodname(func)
        LoopDaemonThread(func, name=_name, sleep=sleep)
        @functools.wraps(func)
        def wrap(*args, **kwargs):
            return func(*args, **kwargs)
        return wrap
    return decorator

# # 使用示例
# def demo_advanced_thread_pool():
#     """演示高级线程池的使用"""
    
#     def compute_square(number: int) -> int:
#         """计算平方"""
#         time.sleep(0.5)  # 模拟计算时间
#         if number == 3:  # 模拟错误
#             raise ValueError("I don't like number 3!")
#         return number * number
    
#     # 创建线程池
#     pool = ThreadPool(num_workers=2, name="ComputePool")
    
#     try:
#         # 提交任务
#         task_ids = []
#         for i in range(5):
#             task_id = pool.submit(compute_square, i)
#             task_ids.append(task_id)
#             print(f"Submitted task {task_id} for number {i}")
        
#         # 获取结果
#         for task_id in task_ids:
#             try:
#                 result = pool.get_result(task_id, timeout=10)
#                 print(f"Task {task_id} result: {result}")
#             except Exception as e:
#                 print(f"Task {task_id} failed: {e}")
        
#         # 查看线程池状态
#         status = pool.get_pool_status()
#         print("Pool status:", status)
        
#     finally:
#         pool.shutdown()
            
# def threadpool(func:Callable):
#     @functools.wraps(func)
#     def wrap(*args, **kwargs):
#         pass        
#     return wrap


if __name__ == "__main__":
    # demo_advanced_thread_pool()
    pool = ThreadPool(num_workers=2, name="ComputePool")
    pass