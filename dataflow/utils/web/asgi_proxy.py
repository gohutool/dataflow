from fastapi import Request, HTTPException
from fastapi.responses import Response, JSONResponse,StreamingResponse
import httpx
import time
from typing import Dict, Optional, Callable
from dataclasses import dataclass
from contextlib import asynccontextmanager
from dataflow.utils.log import Logger
import asyncio
import requests
import json

_logger = Logger('dataflow.utils.web.asgi_proxy')

@dataclass
class ProxyConfig:
    """代理配置"""
    timeout: float = 30.0
    max_connections: int = 100
    enable_caching: bool = False
    cache_ttl: int = 300
    rate_limit: Optional[int] = None
    blocked_user_agents: list[str] = None

class AdvancedProxyService:
    """高级代理服务"""
    
    def __init__(self, config: ProxyConfig = None):
        self.config = config or ProxyConfig()
        self.client = None
        self.request_log = []
        self.rate_limits = {}
        self.cache = {}
        self.request_filters = []
        self.response_filters = []
        
        if self.config.blocked_user_agents is None:
            self.config.blocked_user_agents = [
                "malicious-bot",
                "scanner"
            ]
    
    @asynccontextmanager
    async def get_client(self):
        """获取HTTP客户端"""
        if self.client is None:
            limits = httpx.Limits(
                max_connections=self.config.max_connections,
                max_keepalive_connections=20
            )
            self.client = httpx.AsyncClient(
                timeout=self.config.timeout,
                limits=limits,
                follow_redirects=True
            )
        
        try:
            yield self.client
        except Exception:
            await self.client.aclose()
            self.client = None
            raise
    
    async def proxy_request(
        self,
        target_url: str,
        request: Request,
        method: str = None,
        header_callback:Callable = None
    ) -> Response:
        """代理请求的核心方法"""
        
        # 记录请求
        request_id = self._generate_request_id()
        start_time = time.time()
        
        # 应用请求过滤器
        filter_result = await self.apply_request_filters(target_url, request)
        if filter_result:
            return filter_result
        
        # 检查速率限制
        if await self.check_rate_limit(request):
            return JSONResponse(
                status_code=429,
                content={"error": "Rate limit exceeded"}
            )
        
        method = method or request.method
        body = await request.body()
        
        async with self.get_client() as client:
            try:
                # 准备请求
                headers = self.prepare_headers(dict(request.headers))
                if callable(header_callback):
                    _tmp = header_callback(headers)
                    if _tmp is not None:
                        headers = _tmp
                
                params = dict(request.query_params)
                
                # 发送请求
                response = await client.request(
                    method=method,
                    url=target_url,
                    headers=headers,
                    content=body if body else None,
                    params=params
                )
                
                # 应用响应过滤器
                filtered_response = await self.apply_response_filters(response)
                
                # 记录成功请求
                self.log_request(
                    request_id=request_id,
                    method=method,
                    url=target_url,
                    status_code=response.status_code,
                    duration=time.time() - start_time,
                    success=True
                )
                
                return filtered_response
                
            except httpx.TimeoutException:
                self.log_request(
                    request_id=request_id,
                    method=method,
                    url=target_url,
                    status_code=504,
                    duration=time.time() - start_time,
                    success=False
                )
                raise HTTPException(status_code=504, detail="Gateway Timeout")
                
            except httpx.ConnectError:
                self.log_request(
                    request_id=request_id,
                    method=method,
                    url=target_url,
                    status_code=502,
                    duration=time.time() - start_time,
                    success=False
                )
                raise HTTPException(status_code=502, detail="Bad Gateway")
                
            except Exception as e:
                self.log_request(
                    request_id=request_id,
                    method=method,
                    url=target_url,
                    status_code=500,
                    duration=time.time() - start_time,
                    success=False
                )
                raise HTTPException(status_code=500, detail=f"Proxy error: {str(e)}")
    
    def prepare_headers(self, headers: Dict) -> Dict:
        """准备请求头"""
        filtered = {}
        skip_headers = {
            'host', 'content-length', 'connection', 
            'accept-encoding', 'content-encoding'
        }
        
        for key, value in headers.items():
            key_lower = key.lower()
            if key_lower not in skip_headers:
                # 过滤被阻止的 User-Agent
                if key_lower == 'user-agent' and self.is_blocked_user_agent(value):
                    filtered[key] = "FastAPI-Proxy/1.0"
                else:
                    filtered[key] = value
        
        return filtered
    
    def is_blocked_user_agent(self, user_agent: str) -> bool:
        """检查 User-Agent 是否被阻止"""
        if not user_agent:
            return False
        
        ua_lower = user_agent.lower()
        for blocked in self.config.blocked_user_agents:
            if blocked.lower() in ua_lower:
                return True
        return False
    
    async def apply_request_filters(self, url: str, request: Request) -> Optional[Response]:
        """应用请求过滤器"""
        for filter_func in self.request_filters:
            result = await filter_func(url, request)
            if result:
                return result
        return None
    
    async def apply_response_filters(self, response: httpx.Response) -> Response:
        """应用响应过滤器"""
        content = response.content
        headers = dict(response.headers)
        status_code = response.status_code
        
        for filter_func in self.response_filters:
            content, headers, status_code = await filter_func(content, headers, status_code)
        
        return Response(
            content=content,
            status_code=status_code,
            headers=headers
        )
    
    async def check_rate_limit(self, request: Request) -> bool:
        """检查速率限制"""
        if not self.config.rate_limit:
            return False
        
        client_ip = request.client.host
        current_time = time.time()
        window_start = current_time - 60  # 1分钟窗口
        
        # 清理旧记录
        self.rate_limits = {
            ip: [t for t in times if t > window_start]
            for ip, times in self.rate_limits.items()
        }
        
        if client_ip not in self.rate_limits:
            self.rate_limits[client_ip] = []
        
        requests_in_window = self.rate_limits[client_ip]
        
        if len(requests_in_window) >= self.config.rate_limit:
            return True
        
        requests_in_window.append(current_time)
        return False
    
    def add_request_filter(self, filter_func: Callable):
        """添加请求过滤器"""
        self.request_filters.append(filter_func)
    
    def add_response_filter(self, filter_func: Callable):
        """添加响应过滤器"""
        self.response_filters.append(filter_func)
    
    def _generate_request_id(self) -> str:
        """生成请求ID"""
        return f"req_{int(time.time() * 1000)}_{len(self.request_log)}"
    
    def log_request(self, request_id: str, method: str, url: str, 
                   status_code: int, duration: float, success: bool):
        """记录请求日志"""
        log_entry = {
            "id": request_id,
            "method": method,
            "url": url,
            "status_code": status_code,
            "duration": round(duration, 3),
            "success": success,
            "timestamp": time.time()
        }
        self.request_log.append(log_entry)
        
        # 保持日志大小可控
        if len(self.request_log) > 1000:
            self.request_log = self.request_log[-500:]

    async def bind_proxy(self, request: Request, url: str, header_callback:Callable = None):
        """通用代理端点"""
        if not url:
            raise HTTPException(status_code=400, detail="URL parameter is required")
    
        # 确保URL有协议
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        return await self.proxy_request(url, request, None, header_callback)
    
    async def bind_streaming_proxy(self, request: Request, url: str, header_callback:Callable = None): 
        """流式代理（用于大文件或流媒体）"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        async with self.get_client() as client:
            try:
                # 创建流式请求
                headers = self.prepare_headers(dict(request.headers))
                
                if callable(header_callback):
                    _tmp = header_callback(headers)
                    if _tmp is not None:
                        headers = _tmp
                        
                
                headers.update({
                    "Accept": "text/event-stream",
                    "Connection": "keep-alive",
                    "Cache-Control": "no-cache"
                })
                
                body_content = await request.body()
                    
                async with client.stream(
                    method=request.method,
                    url=url,
                    headers=headers,
                    params=dict(request.query_params),
                    content=body_content
                ) as response:
                    
                    async def generate():
                        try:
                            async for chunk in response.aiter_bytes():
                                yield chunk                            
                        except asyncio.CancelledError:
                            # 当客户端断开时，会在这里抛出 CancelledError
                            _logger.DEBUG("Client disconnected, closing stream")
                            # 我们可以在这里做一些清理工作，但是 resp 的上下文管理器会帮我们关闭连接
                            raise
                        except Exception as e:
                            raise e
                    
                    return StreamingResponse(
                        generate(),
                        status_code=response.status_code,
                        headers=dict(response.headers)
                    )
                    
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        

# 创建高级代理应用
def get_default_config():
    _default_config = ProxyConfig(
        timeout=30.0,
        max_connections=100,
        enable_caching=False,
        rate_limit=100  # 每分钟100个请求
    )
    return _default_config


def sync_stream_with_requests(url, method='GET', data=None, headers=None, func:Callable=None)->int:
    """使用 requests 进行流式请求"""
    try:
        response = requests.request(
            method=method,
            url=url,
            data=data,
            headers=headers,
            stream=True,  # 关键参数
            timeout=30
        )
        
        # 检查响应状态
        if response.status_code != 200:
            # print(f"请求失败，状态码: {response.status_code}")
            raise Exception(f"请求失败，状态码: {response.status_code}")
        
        # print(f"开始接收流式数据 (状态码: {response.status_code})")
        # print(f"Content-Type: {response.headers.get('content-type')}")
        
        # 逐块读取数据
        bytes_received = 0
        # start_time = time.time()
        
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:  # 过滤掉 keep-alive 新块
                bytes_received += len(chunk)
                
                if callable(func):
                    rtn = func(chunk, bytes_received)
                    if rtn:
                        return
                
                # elapsed = time.time() - start_time
                
                # # 尝试解码为文本
                # try:
                #     text = chunk.decode('utf-8')
                #     print(f"[{elapsed:.2f}s] 收到数据: {text.strip()}")
                # except UnicodeDecodeError:
                #     print(f"[{elapsed:.2f}s] 收到二进制数据: {len(chunk)} 字节")
                
        # print(f"流式传输完成，总共接收: {bytes_received} 字节")
        return bytes_received
    except requests.exceptions.RequestException as e:
        raise e
    except Exception as e:
        raise e
        
if __name__ == "__main__":
    # 测试 Server-Sent Events
    url = "http://localhost:8080/v3/stream"
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream"
    }
    data = json.dumps({
        "create_request": {
            "key": "dGVzdA=="
        }
    })
    
    def callback_print(chunk, read_size):
        try:
            text = chunk.decode('utf-8')
            print(f"{read_size} 收到数据: {text.strip()}")
        except UnicodeDecodeError:
            print(f"{read_size} 收到二进制数据: {len(chunk)} 字节")
    
    sync_stream_with_requests(url, 'POST', data, headers, callback_print)
    
    