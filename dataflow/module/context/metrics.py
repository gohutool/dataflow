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
from dataflow.module import WebContext

_logger = Logger('module.context.metrics')

# Request metrics
http_requests_total = Counter("http_requests_total", "Total number of HTTP requests", ["method", "endpoint", "status"])

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds", "HTTP request duration in seconds", ["method", "endpoint"]
)

# # Database metrics
# __db_connections = Gauge("db_connections", "Number of active database connections")

# # Custom business metrics
# __orders_processed = Counter("orders_processed_total", "Total number of orders processed")

__llm_process_duration_seconds = Histogram(
    "llm_process_duration_seconds",
    "Time spent processing LLM",
    ["model", "label"],
    buckets=[0.1, 0.3, 0.5, 1.0, 2.0, 5.0, 60]    
)

__llm_inference_duration_seconds = Histogram(
    "llm_inference_duration_seconds",
    "Time spent processing LLM inference",
    ["model"],
    buckets=[0.1, 0.3, 0.5, 1.0, 2.0, 5.0, 60]    
)

__llm_stream_duration_seconds = Histogram(
    "llm_stream_duration_seconds",
    "Time spent processing LLM stream inference",
    ["model"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 60]
)

def llm_duration_stream_metrics(model:str, duration:float)->None:
    __llm_stream_duration_seconds.labels(model=model).observe(duration)

def llm_duration_inference_metrics(model:str, duration:float)->None:
    __llm_inference_duration_seconds.labels(model=model).observe(duration)

def llm_duration_metrics(model:str, label:str, duration:float)->None:
    __llm_process_duration_seconds.labels(model=model, label=label).observe(duration)
    
    
def setup_metrics(app):
    """Set up Prometheus metrics middleware and endpoints.

    Args:
        app: FastAPI application instance
    """
    # Add Prometheus middleware
    app.add_middleware(PrometheusMiddleware)
    app.add_middleware(MetricsMiddleware)

    # Add metrics endpoint
    app.add_route("/prometheus/metrics", metrics)
    _logger.INFO('Metrics组件加载成功')
    


class MetricsMiddleware(BaseHTTPMiddleware):
    """Middleware for tracking HTTP request metrics."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)                
        _logger.INFO('初始化MetricsMiddleware过滤器')

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
        except Exception:
            status_code = 500
            raise
        finally:
            duration = time.time() - start_time
            # Record metrics
            http_requests_total.labels(method=request.method, endpoint=request.url.path, status=status_code).inc()
            http_request_duration_seconds.labels(method=request.method, endpoint=request.url.path).observe(duration)
            _logger.DEBUG(f'http_requests_total={http_requests_total.labels}')

        return response


setup_metrics(WebContext.getRoot())

