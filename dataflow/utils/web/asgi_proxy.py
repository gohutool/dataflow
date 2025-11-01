from fastapi import Request, HTTPException
from fastapi.responses import Response, JSONResponse,StreamingResponse
import httpx
import time
from typing import Dict, Optional, Callable,Generator,AsyncGenerator
from dataclasses import dataclass
from contextlib import asynccontextmanager,contextmanager
from dataflow.utils.log import Logger
import asyncio
# from asyncio import AsynGenerator
import json

_logger = Logger('dataflow.utils.web.asgi_proxy')

@dataclass
class ProxyConfig:
    """代理配置"""
    timeout: float = 30.0
    stream_timeout: float = None
    max_connections: int = 100
    enable_caching: bool = False
    cache_ttl: int = 300
    rate_limit: Optional[int] = None
    blocked_user_agents: list[str] = None

class StreamResponse:
    @staticmethod
    async def _convert(gen1):
        async for one in gen1:
            yield one[0]
    
    def __init__(self, response, gen:Generator|AsyncGenerator):
        self.response = response
        self._gen = gen
        
    def getResponse(self):
        return self.response
    
    def streams(self):
        return self._gen
    
    async def chunkstreams(self):
        return StreamResponse._convert(self._gen)
        

class AdvancedProxyService:
    """高级代理服务"""
    
    def __init__(self, config: ProxyConfig = None):
        self.config = config or ProxyConfig()
        self.client = None
        self.stream_client = None
        self.client_sync = None
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
    
    @contextmanager
    def get_client_sync(self):
        """获取HTTP客户端"""
        if self.client_sync is None:
            limits = httpx.Limits(
                max_connections=self.config.max_connections,
                max_keepalive_connections=20
            )
            self.client_sync = httpx.Client(
                timeout=self.config.timeout,
                limits=limits,
                follow_redirects=True
            )
        
        try:
            yield self.client_sync
        except Exception:
            self.client_sync.close()
            self.client_sync = None
            raise
        
    @asynccontextmanager
    async def get_stream_client(self):
        """获取HTTP客户端"""
        if self.stream_client is None:
            limits = httpx.Limits(
                max_connections=self.config.max_connections,
                max_keepalive_connections=20
            )
            self.stream_client = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=None, read=None, write=None, pool=None) if self.config.stream_timeout is None or not self.config.stream_timeout else self.config.stream_timeout,
                # timeout=self.config.timeout,
                limits=limits,
                follow_redirects=True
            )
            
        try:
            yield self.stream_client
        except Exception:
            await self.stream_client.aclose()
            self.stream_client = None
            raise
            
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
            
            streamreponse:StreamResponse = await self.async_stream_http_request(url=url,method=request.method,data=None, content=body_content,headers=headers,params=dict(request.query_params))
            response = streamreponse.getResponse()
            return StreamingResponse(
                        await streamreponse.chunkstreams(),
                        status_code=response.status_code,
                        headers=dict(response.headers)
            )            
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        # async with self.get_client() as client:
        #     try:
        #         # 创建流式请求
        #         headers = self.prepare_headers(dict(request.headers))
                
        #         if callable(header_callback):
        #             _tmp = header_callback(headers)
        #             if _tmp is not None:
        #                 headers = _tmp
                        
                
        #         headers.update({
        #             "Accept": "text/event-stream",
        #             "Connection": "keep-alive",
        #             "Cache-Control": "no-cache"
        #         })
                
        #         body_content = await request.body()
                    
        #         # async with client.stream(
        #         #     method=request.method,
        #         #     url=url,
        #         #     headers=headers,
        #         #     params=dict(request.query_params),
        #         #     content=body_content
        #         # ) as response:
                    
        #         #     async def generate():
        #         #         try:
        #         #             async for chunk in response.aiter_bytes():
        #         #                 yield chunk                            
        #         #         except asyncio.CancelledError:
        #         #             # 当客户端断开时，会在这里抛出 CancelledError
        #         #             _logger.DEBUG("Client disconnected, closing stream")
        #         #             # 我们可以在这里做一些清理工作，但是 resp 的上下文管理器会帮我们关闭连接
        #         #             raise
        #         #         except Exception as e:
        #         #             raise e
                    
        #         #     return StreamingResponse(
        #         #         generate(),
        #         #         status_code=response.status_code,
        #         #         headers=dict(response.headers)
        #         #     )
                    
            # except Exception as e:
            #     raise HTTPException(status_code=500, detail=str(e))

    def sync_read_stream_with_requests(self, url, method='GET', data=None, content=None, headers=None, params=None)->StreamResponse:
        gen = self._sync_read_stream_with_requests(url, method, data, content, headers, params)
        return StreamResponse(next(gen), gen)
    
    def _sync_read_stream_with_requests(self, url, method='GET', data=None, content=None, headers=None, params=None)->Generator[bytes, None, int]:
        """
        同步流式HTTP请求生成器
        
        Args:
            url: 请求URL
            method: HTTP方法
            headers: 请求头
        """
        with self.get_client_sync() as client:
            client:httpx.Client = client            
            with client.stream(method, url, headers=headers or {}, data=data, content=content, params=params) as response:                
                # 先检查响应状态
                response.raise_for_status()
                yield response
                
                # 然后返回数据流
                bytes_received = 0
                times = 0
                for chunk in response.iter_bytes():
                    if chunk:  # 只返回非空数据块
                        bytes_received += len(chunk)
                        times += 1
                        yield chunk, bytes_received, times
        # try:
        #     response = requests.request(
        #         method=method,
        #         url=url,
        #         data=data,
        #         headers=headers,
        #         stream=True,  # 关键参数
        #         timeout=30
        #     )
            
        #     # 检查响应状态
        #     if response.status_code != 200:
        #         # print(f"请求失败，状态码: {response.status_code}")
        #         raise Exception(f"请求失败，状态码: {response.status_code}")
            
        #     # print(f"开始接收流式数据 (状态码: {response.status_code})")
        #     # print(f"Content-Type: {response.headers.get('content-type')}")
            
        #     # 逐块读取数据
        #     bytes_received = 0
        #     # start_time = time.time()
        #     times =  0
        #     for chunk in response.iter_content(chunk_size=1024):
        #         times += 1
        #         if chunk:  # 过滤掉 keep-alive 新块
        #             bytes_received += len(chunk)
        #             yield chunk,bytes_received,times 
                    
        #             # elapsed = time.time() - start_time
                    
        #             # # 尝试解码为文本
        #             # try:
        #             #     text = chunk.decode('utf-8')
        #             #     print(f"[{elapsed:.2f}s] 收到数据: {text.strip()}")
        #             # except UnicodeDecodeError:
        #             #     print(f"[{elapsed:.2f}s] 收到二进制数据: {len(chunk)} 字节")
                    
        #     # print(f"流式传输完成，总共接收: {bytes_received} 字节")
        #     return bytes_received
        # except requests.exceptions.RequestException as e:
        #     raise e
        # except Exception as e:
        #     raise e
        
    def sync_stream_with_requests(self, url, method='GET', data=None, content=None, headers=None, func:Callable=None, params=None)->int:
        """
        同步流式HTTP请求生成器
        
        Args:
            url: 请求URL
            method: HTTP方法
            headers: 请求头
        """
        with self.get_client_sync() as client:
            client:httpx.Client = client            
            with client.stream(method, url, headers=headers or {}, data=data, content=content, params=params) as response:                
                # 先检查响应状态
                response.raise_for_status()
                print(f"请求失败，状态码: {response.status_code} {response.headers}")                
                # 检查响应状态
                if response.status_code != 200:
                    # print(f"请求失败，状态码: {response.status_code}")
                    raise Exception(f"请求失败，状态码: {response.status_code}")
                
                # 然后返回数据流
                bytes_received = 0
                times = 0
                for chunk in response.iter_bytes():
                    if chunk:  # 过滤掉 keep-alive 新块
                        bytes_received += len(chunk)
                        times += 1
                        if callable(func):
                            rtn = func(chunk, bytes_received, times)
                            if rtn:
                                break
        # """使用 requests 进行流式请求"""
        # try:
        #     response = requests.request(
        #         method=method,
        #         url=url,
        #         data=data,
        #         headers=headers,
        #         stream=True,  # 关键参数
        #         timeout=30
        #     )
            
        #     # 检查响应状态
        #     if response.status_code != 200:
        #         # print(f"请求失败，状态码: {response.status_code}")
        #         raise Exception(f"请求失败，状态码: {response.status_code}")
            
        #     # print(f"开始接收流式数据 (状态码: {response.status_code})")
        #     # print(f"Content-Type: {response.headers.get('content-type')}")
            
        #     # 逐块读取数据
        #     bytes_received = 0
        #     times = 0
        #     # start_time = time.time()
            
        #     for chunk in response.iter_content(chunk_size=1024):
        #         if chunk:  # 过滤掉 keep-alive 新块
        #             bytes_received += len(chunk)
        #             times += 1
        #             if callable(func):
        #                 rtn = func(chunk, bytes_received, times)
        #                 if rtn:
        #                     break
                    
        #             # elapsed = time.time() - start_time
                    
        #             # # 尝试解码为文本
        #             # try:
        #             #     text = chunk.decode('utf-8')
        #             #     print(f"[{elapsed:.2f}s] 收到数据: {text.strip()}")
        #             # except UnicodeDecodeError:
        #             #     print(f"[{elapsed:.2f}s] 收到二进制数据: {len(chunk)} 字节")
                    
        #     # print(f"流式传输完成，总共接收: {bytes_received} 字节")
        #     return bytes_received
        # except requests.exceptions.RequestException as e:
        #     raise e
        # except Exception as e:
        #     raise e
        
    async def async_response_stream(self,url:str, method:str='GET',data=None, content=None, headers: dict = None, params=None):
        async def gen_stream():
            async with self.get_stream_client() as client:
                async with client.stream(method=method, url=url, content=content, params=params,
                                         data=data, headers=headers or {}, timeout=self.config.stream_timeout) as response: 
                    response.raise_for_status()
                    yield response
                    # 创建 StreamingResponse
                    async for chunk in response.aiter_bytes():
                        try:
                            _logger.DEBUG(f'RECV={chunk}')
                            yield chunk
                        except Exception as e:
                            _logger.ERROR(f'客户端断开连接={e}')
                            # 客户端断开连接
                            break                        
                    _logger.DEBUG('流响应结束')
        _sr = gen_stream()
        _response = await _sr.__anext__()
        return _response, _sr
        
    async def async_stream_http_request(self, url: str, method: str = "GET", data=None, content=None, headers: dict = None, params=None, checkfunc:Callable=None)->StreamResponse:
        gen = self._async_stream_http_request(url, method, data, content, headers, params, checkfunc)
        response_info = await gen.__anext__()
        return StreamResponse(response_info, gen)
        
    async def _async_stream_http_request(self, url: str, method: str = "GET", data=None, content=None, headers: dict = None, params=None, checkfunc:Callable=None)->AsyncGenerator[tuple[bytes, None, int]|Response,None]:
        """
        异步流式HTTP请求生成器
        
        Args:
            url: 请求URL
            method: HTTP方法
            headers: 请求头
        """    
        async with self.get_stream_client() as client:
            client:httpx.AsyncClient = client
            async with client.stream(method, url, headers=headers or {}, data=data, 
                                     content=content, params=params, timeout=self.config.stream_timeout) as response:  
                # _logger.DEBUG(f'response.status_code={response.status_code} header={dict(response.headers)}')                    
                # 先检查响应状态
                response.raise_for_status()
                yield response
                
                bytes_received = 0
                times = 0
                async for chunk in response.aiter_bytes():
                    try:
                        bytes_received += len(chunk)   
                        times += 1
                        if callable(checkfunc):
                            checked = await checkfunc(chunk)
                            if checked is not None and checked is False:                            
                                break
                                
                        if chunk:  # 只返回非空数据块
                            yield chunk,bytes_received,times 
                    except Exception as e:
                        _logger.ERROR(f'客户端断开连接={e}')
                        # 客户端断开连接
                        break
                    
                _logger.ERROR('流响应结束')
            

# 创建高级代理应用
def get_default_config():
    _default_config = ProxyConfig(
        timeout=30.0,
        max_connections=100,
        enable_caching=False,
        rate_limit=100  # 每分钟100个请求
    )
    return _default_config

        
if __name__ == "__main__":
    # 测试 Server-Sent Events
    url = "http://localhost:8080/v3/watch"
    # url = "http://localhost:12379/v3/watch"
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream"
    }
    data = json.dumps({
        "create_request": {
            "key": "dGVzdA=="
        }
    })
    
    def callback_print(chunk, read_size, times):
        try:
            text = chunk.decode('utf-8')
            print(f"{times}  {read_size} 收到数据: {text.strip()}")
        except UnicodeDecodeError:
            print(f"{times}  {read_size} 收到二进制数据: {len(chunk)} 字节")
            
        if times > 20:
            return True
    
    aps = AdvancedProxyService()
    
    # aps.sync_stream_with_requests(url, 'POST', data, None, headers, callback_print)
    # exit()
    
    # stream_generator = aps.sync_read_stream_with_requests(url, 'POST', data, None, headers)
    
    # res:Response = next(stream_generator)
    # _logger.DEBUG(f'response.status_code={res.status_code} header={dict(res.headers)}')
    # for chunk, read_size,times in stream_generator:
    #     if callback_print(chunk, read_size, times):
    #         break
    
    # streamresponse = aps.sync_read_stream_with_requests(url, 'POST', data, None, headers)
    
    # res:Response = streamresponse.getResponse()
    # _logger.DEBUG(f'response.status_code={res.status_code} header={dict(res.headers)}')
    # for chunk, read_size,times in streamresponse.streams():
    #     if callback_print(chunk, read_size, times):
    #         break
        
    async def main():
        """测试函数"""
        print("开始测试HTTP流式请求...")
        
        # try:
        #     stream_generator = aps.async_stream_http_request(url, 'POST')
            
        #     res:Response = await stream_generator.__anext__()
        #     _logger.DEBUG(f'response.status_code={res.status_code} header={dict(res.headers)}')
        #     # 使用异步生成器
        #     async for chunk, read_size,times in stream_generator:
        #         if callback_print(chunk, read_size, times):
        #             break                
        # except Exception as e:
        #     print(f"请求失败: {e}")
        try:
            streamresponse = await aps.async_stream_http_request(url, 'POST', data, None, headers)
            
            res:Response = streamresponse.getResponse()
            _logger.DEBUG(f'response.status_code={res.status_code} header={dict(res.headers)}')
            # 使用异步生成器
            async for chunk, read_size,times in streamresponse.streams():
                if callback_print(chunk, read_size, times):
                    break                
        except Exception as e:
            print(f"请求失败: {e}")
            
    asyncio.run(main())
    