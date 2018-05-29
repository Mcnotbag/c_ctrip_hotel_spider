# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import random

import requests
import scrapy_redis
import time
from scrapy import signals
import redis
# from xiecheng.xiecheng import settings
from fake_useragent import UserAgent
from threading import Lock

class XiechengSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class XiechengDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class ProxyDownloaderMiddleware:
    def __init__(self):
        self.redis_server = redis.StrictRedis(host="localhost",port=6379)
    #     self.redis_server = scrapy_redis.get_redis_from_settings(settings=settings)
        self.ua = UserAgent()
    def process_request(self,request,spider):
        # response = requests.get("http://dynamic.goubanjia.com/dynamic/get/30f846d90bf66fccdc0448bd294bb2c3.html?sep=3")
        # request.meta["proxy"] = "http://" + response.content.decode()
        user_agent = self.ua.ie
        request.headers["User_Agent"] = user_agent
            # else:
            #     print("_"*100)
            #     print(ip)
            #     print(user_agent)
            #     print("_"*100)














# lock = Lock()
# def ippool():
#     lock.acquire(blocking=True)
#     ip_response = requests.get(
#         "http://dev.kuaidaili.com/api/getproxy/?orderid=982601525353147&num=100&b_pcchrome=1&b_pcie=1&b_pcff=1&protocol=1&method=2&an_an=1&an_ha=1&sp1=1&sp2=1&sep=2")
#     ip_list = ip_response.content.decode().split("\n")
#     n = 0
#     for t in range(5):
#         n += 1
#         time.sleep(1)
#     if n > 5:
#         lock.release()
#     return ip_list



