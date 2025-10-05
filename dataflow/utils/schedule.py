from dataflow.utils.log import Logger
from apscheduler.schedulers.background import BackgroundScheduler
import apscheduler.triggers.cron as s_cron
import apscheduler.triggers.base as s_base
import apscheduler.triggers.interval as s_interval
import apscheduler.triggers.combining as s_combining
import apscheduler.triggers.date as s_date
import apscheduler.events as s_events
from apscheduler.job import Job as s_Job
import atexit
from typing import Callable
import functools
from dataflow.utils.utils import date_datetime_cn,date2str_yyyymmddhhmmsss
import time

_logger = Logger('dataflow.utils.schedule')


class ScheduleContext:    
    class Job(s_Job):
        pass
    
    class Event:        
        class CronTrigger(s_cron.CronTrigger):
            pass
        class BaseTrigger(s_base.BaseTrigger):
            pass
        class IntervalTrigger(s_interval.IntervalTrigger):
            pass
        class AndTrigger(s_combining.AndTrigger):
            pass
        class OrTrigger(s_combining.OrTrigger):
            pass
        class BaseCombiningTrigger(s_combining.BaseCombiningTrigger) :
            pass
        class DateTrigger(s_date.DateTrigger):
            pass
        
        EVENT_SCHEDULER_STARTED = s_events.EVENT_SCHEDULER_STARTED
        EVENT_SCHEDULER_SHUTDOWN = s_events.EVENT_SCHEDULER_SHUTDOWN
        EVENT_SCHEDULER_PAUSED = s_events.EVENT_SCHEDULER_PAUSED
        EVENT_SCHEDULER_RESUMED = s_events.EVENT_SCHEDULER_RESUMED
        EVENT_EXECUTOR_ADDED = s_events.EVENT_EXECUTOR_ADDED
        EVENT_EXECUTOR_REMOVED = s_events.EVENT_EXECUTOR_REMOVED
        EVENT_JOBSTORE_ADDED = s_events.EVENT_JOBSTORE_ADDED
        EVENT_JOBSTORE_REMOVED = s_events.EVENT_JOBSTORE_REMOVED
        EVENT_ALL_JOBS_REMOVED = s_events.EVENT_ALL_JOBS_REMOVED
        EVENT_JOB_ADDED = s_events.EVENT_JOB_ADDED
        EVENT_JOB_REMOVED = s_events.EVENT_JOB_REMOVED
        EVENT_JOB_MODIFIED = s_events.EVENT_JOB_MODIFIED
        EVENT_JOB_EXECUTED = s_events.EVENT_JOB_EXECUTED
        EVENT_JOB_ERROR = s_events.EVENT_JOB_ERROR
        EVENT_JOB_MISSED = s_events.EVENT_JOB_MISSED
        EVENT_JOB_SUBMITTED = s_events.EVENT_JOB_SUBMITTED
        EVENT_JOB_MAX_INSTANCES = s_events.EVENT_JOB_MAX_INSTANCES
        EVENT_ALL = s_events.EVENT_ALL            
        @staticmethod
        def add_listener(callback, mask=EVENT_ALL):
            ScheduleContext.getSchduler().add_listener(callback, mask)
        @staticmethod    
        def remove_listener(callback):
            ScheduleContext.getSchduler().remove_listener(callback)
        @staticmethod
        def on_Listener(event:int=EVENT_ALL):
            def _on_listener(func:Callable):
                @functools.wraps(func)             
                def wrapper(*args, **kwargs):
                    result = func(*args, **kwargs)                
                    return result
                
                ScheduleContext.Event.add_listener(func, event)
                _logger.DEBUG(f'ScheduleContext添加事件监听器[{event}]={func}')
                return wrapper            
            return _on_listener
        class JobEvent(s_events.JobEvent):
            pass
        
    _scheduler : BackgroundScheduler = BackgroundScheduler()    
    _started: bool = False
    @staticmethod
    def getSchduler()->BackgroundScheduler:
        return ScheduleContext._scheduler
    @staticmethod
    def startContext():   
        if not ScheduleContext._started:
            ScheduleContext.getSchduler().start()
            def on_exit():        
                _logger.INFO('Shutdown scheduler')
                ScheduleContext.getSchduler().shutdown()
                
            atexit.register(on_exit)  
            _logger.INFO('ScheduleContext启动结束')            
    @staticmethod
    def add_job(func, trigger=None, args=None, kwargs=None, id=None, name=None,
                jobstore='default', executor='default',
                replace_existing=False, **trigger_args)->any:        
        _logger.DEBUG(f'ScheduleContext添加任务{trigger}={func}')
        return ScheduleContext.getSchduler().add_job(func, trigger, args, kwargs, id, name,
                                              jobstore=jobstore, executor=executor, replace_existing=replace_existing, **trigger_args)
    @staticmethod
    def pause():
        ScheduleContext.getSchduler().pause()
    @staticmethod
    def resume():    
        ScheduleContext.getSchduler().resume()
    @staticmethod
    def remove_job(job_id, jobstore=None):        
        ScheduleContext.getSchduler().remove_job(job_id, jobstore)
    @staticmethod
    def remove_all_jobs(jobstore=None):            
        ScheduleContext.getSchduler().remove_all_jobs(jobstore)
    @staticmethod    
    def on_Trigger(trigger,args=None, kwargs=None, id=None, name=None,
                jobstore='default', executor='default',
                replace_existing=False, **trigger_args):
        if trigger is None:
            raise Exception('必须指定一个触发器')
        
        def _on_trigger(func:Callable):
            @functools.wraps(func)             
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)                
                return result
            
            job:ScheduleContext.Job = ScheduleContext.add_job(func, trigger, args, kwargs, id, name,
                                              jobstore, executor, replace_existing, **trigger_args)
            _logger.DEBUG(f'ScheduleContext添加任务事件{job}[{func}]={trigger}[{args},{kwargs}]结束')
            
            return wrapper            
        return _on_trigger
    @staticmethod
    def get_job(job_id, jobstore=None):
        return ScheduleContext.getSchduler().get_job(job_id, jobstore)        
    @staticmethod
    def pause_job(job_id, jobstore=None):        
        ScheduleContext.getSchduler().pause_job(job_id, jobstore)        
    @staticmethod
    def resume_job(job_id, jobstore=None):        
        ScheduleContext.getSchduler().resume_job(job_id, jobstore)   
    @staticmethod
    def reschedule_job(job_id, jobstore=None, trigger=None, **trigger_args):        
        ScheduleContext.getSchduler().reschedule_job(job_id, jobstore, trigger, **trigger_args)
        
    
ScheduleContext.startContext()    
    
    
if __name__ == "__main__":
    def _print_date_info():
        _logger.DEBUG(f'当前时间==={date2str_yyyymmddhhmmsss(date_datetime_cn())}')
        
    
    ScheduleContext.add_job(_print_date_info, ScheduleContext.Event.CronTrigger(second='*/20'))
    
    while True:
        time.sleep(1)
        pass