"""Prometheus metrics configuration for the application.

This module sets up and configures Prometheus metrics for monitoring the application.
"""

from prometheus_client import Counter, Histogram, Gauge  # noqa: F401
from starlette_prometheus import metrics, PrometheusMiddleware

import time
from typing import Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from dataflow.utils.log import Logger
from dataflow.utils.utils import get_bool_from_dict
from dataflow.utils.reflect import get_fullname
from dataflow.module import WebContext, Context

_logger = Logger('dataflow.module.context.metrics')

class MetricsContext:
    def __init__(self):        
        # Request metrics
        self.__llm_process_duration_seconds = Histogram(
            "llm_process_duration_seconds",
            "Time spent processing LLM",
            ["model", "label"],
            buckets=[0.1, 0.3, 0.5, 1.0, 2.0, 5.0, 60]    
        )
        self.__llm_inference_duration_seconds = Histogram(
            "llm_inference_duration_seconds",
            "Time spent processing LLM inference",
            ["model"],
            buckets=[0.1, 0.3, 0.5, 1.0, 2.0, 5.0, 60]    
        )
        self.__llm_stream_duration_seconds = Histogram(
            "llm_stream_duration_seconds",
            "Time spent processing LLM stream inference",
            ["model"],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 60]
        )        
        # # Database metrics
        # __db_connections = Gauge("db_connections", "Number of active database connections")
        # # Custom business metrics
        # __orders_processed = Counter("orders_processed_total", "Total number of orders processed")
    
    
    def llm_duration_stream_metrics(self, model:str, duration:float)->None:
        self.__llm_stream_duration_seconds.labels(model=model).observe(duration)

    def llm_duration_inference_metrics(self, model:str, duration:float)->None:
        self.__llm_inference_duration_seconds.labels(model=model).observe(duration)

    def llm_duration_metrics(self, model:str, label:str, duration:float)->None:
        self.__llm_process_duration_seconds.labels(model=model, label=label).observe(duration)


def setup_metrics(app):
    """Set up Prometheus metrics middleware and endpoints.

    Args:
        app: FastAPI application instance
    """
    # Add Prometheus middleware
    app.add_middleware(PrometheusMiddleware)
    _logger.INFO('添加过滤器PrometheusMiddleware')
    app.add_middleware(MetricsMiddleware)
    _logger.INFO('添加过滤器MetricsMiddleware')

    # Add metrics endpoint
    app.add_route("/prometheus/metrics", metrics)
    _logger.INFO('Metrics组件加载成功')
    
    metricsContext = MetricsContext()
    Context.getContext().registerBean(get_fullname(metricsContext), metricsContext)
    
    
class MetricsMiddleware(BaseHTTPMiddleware):
    """Middleware for tracking HTTP request metrics."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)                   
        self.http_requests_total = Counter("http_requests_total", "Total number of HTTP requests", ["method", "endpoint", "status"])
        self.http_request_duration_seconds = Histogram(
            "http_request_duration_seconds", "HTTP request duration in seconds", ["method", "endpoint"]
        )                             

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Track metrics for each request.

        Args:
            request: The incoming request
            call_next: The next middleware or route handler

        Returns:
            Response: The response from the application
        """
        start_time = time.time()

        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            status_code = 500
            raise e
        finally:
            duration = time.time() - start_time
            # Record metrics
            self.http_requests_total.labels(method=request.method, endpoint=request.url.path, status=status_code).inc()
            self.http_request_duration_seconds.labels(method=request.method, endpoint=request.url.path).observe(duration)
            _logger.DEBUG(f'http_requests_total={self.http_requests_total.labels}')
        return response

prefix = 'context.metrics.prometheus'

@Context.Configurationable(prefix=prefix)
def _init_metrics_context(config:dict):
    if config and get_bool_from_dict(config, 'enabled'):
        setup_metrics(WebContext.getRoot())
        _logger.DEBUG(f'初始化Metreics上下文={config}')
    else:
        _logger.DEBUG(f'没有启动Metreics上下文，设置{prefix}.enabled: True启动')

