#coding=utf-8
"""
基于python 自身 Queue
"""
from scrapy.downloadermiddlewares.retry import RetryMiddleware
import logging
from scrapy.exceptions import NotConfigured
from scrapy.utils.response import response_status_message
import base64
try:
    from Queue import Queue,Empty,Full
except ImportError:
    # py3
    from queue import Queue,Empty,Full
import random
logger = logging.getLogger(__name__)
queue=Queue(maxsize=10)  # 用于存储代理  获取代理爬虫暂时没写

class Proxy(object):
    def __init__(self,ip,port,user_pass=None,proxy_retry_times=0):
        self._ip=ip
        self._port=port
        self._proxy_retry_times=proxy_retry_times # 使用重试的次数
        self._user_pass=user_pass

    @property
    def get_ip(self):
        return self._ip

    @property
    def get_port(self):
        return self._port

    @property
    def get_proxy_retry_times(self):
        return self._proxy_retry_times

    @property
    def join_ip_port(self):
        return '%s:%s'%(self._ip,self._port)

    def proxy_format(self):
        return 'http://'+self.join_ip_port

    def update_proxy_retry_times(self):
        self._proxy_retry_times+=1

    def proxy_authorization(self):
        if not self._user_pass:
            self.encoded_user_pass='Basic '
        else:
            self.encoded_user_pass = 'Basic '+base64.encodestring(self._user_pass)



class SetProxyMiddleware(object):

    def __init__(self, settings):
        self.queue_block=settings.getbool('QUEUE_BLOCK',default=False)
        self.queue_timeout=settings.getint('QUEUE_TIMEOUT',default=0)
        self.standby_proxy_list=settings.getlist('STANDBY_PROXY') # 备用的代理
        # 格式 [(ip,port,user_pass),]   没有 user_pass 则为 None

        if not self.standby_proxy_list:
            logging.warning("STANDBY_PROXY is empty , This is bad")  # 没有备用代理  无法在队列为空时 再次使用代理
        if self.queue_block and self.queue_timeout>0.5:
            logging.warning("QUEUE_TIMEOUT is too large , This is bad")  # 代理队列等待时间过长 影响爬虫的并发效率

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        # 给请求设置代理
        try:
            proxy = queue.get(block=self.queue_block,timeout=self.queue_timeout)
            self._set_proxy(request,proxy)

        except Empty:
            logging.warning("Proxy Queue is empty")  # 代理队列为空的
            try:
                ip,port,user_pass=random.choice(self.standby_proxy_list)
                user_pass=user_pass or None
                proxy=Proxy(ip,port,user_pass)
                self._set_proxy(request,proxy)

            except ValueError:
                logging.warning("STANDBY_PROXY formal error . example : [(ip,port,user_pass), (ip,port,user_pass)]")

        except Exception as e:
            logging.error("Unknown Error : %s"%e)   # 产生未知的错误

    def _set_proxy(self,request,proxy):
        request.meta['proxy'] = proxy.proxy_format()
        request.meta['proxy_instantiation']=proxy # 用于存储代理的实例  后期会有 pick_up_proxy 操作
        request.headers['Proxy-Authorization'] = proxy.proxy_authorization()
        logging.debug("User Proxy : %s"%proxy)

class ProxyQueueRetryMiddleware(RetryMiddleware):
    # EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
    #                        ConnectionRefusedError, ConnectionDone, ConnectError,
    #                        ConnectionLost, TCPTimedOutError, ResponseFailed,
    #                        IOError, TunnelError)

    def __init__(self, settings):
        if not settings.getbool('RETRY_ENABLED'):
            raise NotConfigured
        self.max_retry_times = settings.getint('RETRY_TIMES')
        self.retry_http_codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES'))
        self.priority_adjust = settings.getint('RETRY_PRIORITY_ADJUST')

        self.max_proxy_retry_times = settings.getint('PROXY_RETRY_TIMES')  # 每个代理最多重试次数
        super(ProxyQueueRetryMiddleware,self).__init__(settings)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            self._pick_up_proxy(request)
            return response

        if response.status in self.retry_http_codes:
            self._proxy_retry(request)
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, self.EXCEPTIONS_TO_RETRY) \
                and not request.meta.get('dont_retry', False):
            return self._retry(request, exception, spider)

    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1
        if retries <= self.max_retry_times:
            logger.debug("Retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra={'spider': spider})
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            retryreq.dont_filter = True
            retryreq.priority = request.priority + self.priority_adjust
            return retryreq
        else:
            logger.debug("Gave up retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra={'spider': spider})


    def _pick_up_proxy(self,request):
        try:
            queue.put(request.meta['proxy_instantiation'],block=False)
            logging.debug("pick up proxy %s to queue"%request.meta['proxy'])
        except Full:
            logging.warning('proxy queue is full')
        except KeyError:
            logging.warning("didn't use to proxy : %s"%request.url)
        except Exception as e:
            logging.error("unknown error : %s"%e)

    def _proxy_retry(self,request):
        try:
            proxy=request.meta['proxy_instantiation']
            proxy.update_proxy_retry_times()
            if proxy.get_proxy_retry_times()<=self.max_proxy_retry_times:
                logger.debug("retrying proxy : %s  ,  number : %s"%(proxy.proxy_format(),proxy.get_proxy_retry_times))
                self._pick_up_proxy(request)
            else:
                logger.debug("gave up retrying proxy : %s"%proxy.proxy_format())
        except KeyError:
            logging.warning("didn't use to proxy : %s"%request.url)
        except Exception as e:
            logging.error("unknown error : %s"%e)

