# -*- coding: utf-8 -*-
import scrapy

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.shell import inspect_response



import datetime
from zhQuestion import settings

from zhQuestion.items import QuesInfoItem

import re
import redis
import requests
import logging
import happybase
from pymongo import MongoClient


class QuesinfoerSpider(scrapy.Spider):
    name = "quesInfo"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    handle_httpstatus_list = [401,429,500,502,503,504]
    baseUrl = "http://www.zhihu.com/question/"

    quesIndex =0


    def __init__(self,stats,spider_type='Master',spider_number=0,partition=1,**kwargs):
        self.stats = stats
        self.redis1 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=1)
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_log = db['log']

        crawler_log = {'project':settings.BOT_NAME,
                       'spider':self.name,
                       'spider_type':spider_type,
                       'spider_number':spider_number,
                       'partition':partition,
                       'type':'start',
                       'updated_at':datetime.datetime.now()}

        self.col_log.insert_one(crawler_log)
        try:
            self.spider_type = str(spider_type)
            self.spider_number = int(spider_number)
            self.partition = int(partition)
            # self.email= settings.EMAIL_LIST[self.spider_number]
            # self.password=settings.PASSWORD_LIST[self.spider_number]

        except:
            self.spider_type = 'Master'
            self.spider_number = 0
            self.partition = 1
            # self.email= settings.EMAIL_LIST[self.spider_number]
            # self.password=settings.PASSWORD_LIST[self.spider_number]
    @classmethod
    def from_crawler(cls, crawler,spider_type='Master',spider_number=0,partition=1,**kwargs):
        return cls(crawler.stats,spider_type=spider_type,spider_number=spider_number,partition=partition)

    def start_requests(self):

        self.questionIdList = self.redis1.keys()
        totalLength = len(self.questionIdList)

        if self.spider_type=='Master':
            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master non 1 partition is '+str(self.partition))
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                for index in range(1,self.partition):
                    payload ={
                        'project':settings.BOT_NAME
                        ,'spider':self.name
                        ,'spider_type':'Slave'
                        ,'spider_number':index
                        ,'partition':self.partition
                        ,'setting':'JOBDIR=/tmp/scrapy/'+self.name+str(index)
                    }
                    logging.warning('Begin to request'+str(index))
                    response = requests.post('http://'+settings.SCRAPYD_HOST_LIST[0]+':'+settings.SCRAPYD_PORT_LIST[0]+'/schedule.json',data=payload)
                    logging.warning('Response: '+str(index)+' '+str(response))
            else:
                logging.warning('Master number is '+str(self.spider_number) + ' partition is '+str(self.partition))

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
            else:
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:]
        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))
        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.questionIdList)))

        yield Request(url ='http://www.zhihu.com',
                      cookies=settings.COOKIES_LIST[self.spider_number],
                      callback =self.after_login)

    #     yield Request("http://www.zhihu.com",callback = self.post_login)
    #
    #
    # def post_login(self,response):
    #     xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
    #     yield FormRequest.from_response(response,
    #                                       formdata={
    #                                           '_xsrf':xsrfValue,
    #                                           'email':self.email,
    #                                           'password':self.password,
    #                                           'remember_me': 'true'
    #                                       },
    #                                       dont_filter = True,
    #                                       callback = self.after_login,
    #                                       )

    def after_login(self,response):
        # try:
        #     loginUserLink = response.xpath('//div[@id="zh-top-inner"]/div[@class="top-nav-profile"]/a/@href').extract()[0]
        #     logging.warning('Successfully login with %s  %s  %s',str(loginUserLink),str(self.email),str(self.password))
        # except:
        #     logging.error('Login failed! %s   %s',self.email,self.password)
        try:
            loginUserLink = response.xpath('//div[@id="zh-top-inner"]/div[@class="top-nav-profile"]/a/@href').extract()[0]
            # logging.warning('Successfully login with %s  %s  %s',str(loginUserLink),str(self.email),str(self.password))
            logging.warning('Successfully login with %s  ',str(loginUserLink))

        except:
            logging.error('Login failed with spider_number %s',str(self.spider_number))

        #inspect_response(response,self)
        #self.urls = ['http://www.zhihu.com/question/28626263','http://www.zhihu.com/question/22921426','http://www.zhihu.com/question/20123112']
        for questionId in self.questionIdList:
            yield FormRequest(url=self.baseUrl +str(questionId)+'?nr=1',
                              meta={'proxy':settings.HTTP_PROXY_LIST[self.spider_number]})


    def parse(self,response):
        if response.status != 200:
#            print "ParsePage HTTPStatusCode: %s Retrying !" %str(response.status)
#             yield  self.make_requests_from_url(response.url)
            yield FormRequest(url=response.request.url,
                              meta={'proxy':settings.HTTP_PROXY_LIST[self.spider_number]})

        else:
            item =  QuesInfoItem()
            item['spiderName'] = self.name

            item['questionId'] = re.split('http://www.zhihu.com/question/(\d*)',response.url)[1]
            item['idZnonceContent'] = response.xpath('//*[@id="znonce"]/@content').extract()[0]  #right
            item['dataUrlToken'] = response.xpath('//*[@id="zh-single-question-page"]/@data-urltoken').extract()[0] #right
            item['isTopQuestion'] = response.xpath('//*[@id="zh-single-question-page"]/meta[@itemprop="isTopQuestion"]/@content').extract()[0]    #right
            item['visitsCount'] = int(response.xpath('//*[@id="zh-single-question-page"]/meta[@itemprop="visitsCount"]/@content').extract()[0])    #right
            try:
                item['tagLabelIdList'] = response.xpath('//div[@id="zh-single-question-page"]//div[@class="zm-tag-editor-labels zg-clear"]/a/@href').re(r'/topic/(\d*)')  #right
                item ['tagLabelDataTopicIdList'] = response.xpath('//div[@id="zh-single-question-page"]//div[@class="zm-tag-editor-labels zg-clear"]/a/@data-topicid').extract()   #right
            except IndexError,e:
                item['tagLabelIdList'] = []
                item['tagLabelDataTopicIdList'] =[]

            item['questionTitle'] = response.xpath('//div[@id="zh-question-title"]/h2/text()').extract()[0] #right

            try:
                item['questionDetail'] = response.xpath('//div[@id="zh-question-detail"]/div[@class="zm-editable-content"]/text()').extract()
                if  item['questionDetail'] :
                    item['questionDetail'] = item['questionDetail'][0]
                else:
                    item['questionDetail'] = ''
            except:
                item['questionDetail'] = response.xpath('//div[@id="zh-question-detail"]/textarea[@class="content hidden"]/text()').extract()[0]

            item['dataResourceId'] = response.xpath('//div[@id="zh-question-detail"]/@data-resourceid').extract()[0]    #right


            try:
                item['quesCommentCount'] = response.xpath('//div[@id="zh-question-meta-wrap"]//a[@name="addcomment"]/text()[2]').re('\d*')[0]   # should try
                if item['quesCommentCount']:
                    item['quesCommentCount'] = int (item['quesCommentCount'])
                else:
                    item['quesCommentCount'] = 0

            except IndexError,e:
                item['quesCommentCount'] = 0

            try:
                item['questionAnswerCount'] = int(response.xpath('//*[@id="zh-question-answer-num"]/@data-num').extract()[0])  #right
            except IndexError,e:
                item['questionAnswerCount'] = 0

            try:
                item['questionFollowerCount'] = int(response.xpath('//*[@id="zh-question-side-header-wrap"]/div[@class="zh-question-followers-sidebar"]/div[1]/a/strong/text()').extract()[0])  #right
            except IndexError,e:
                item['questionFollowerCount'] = 0


            try:
                item['questionLatestActiveTime'] = response.xpath('//*[@id="zh-single-question-page"]//span[@class="time"]/text()').extract()[0]
            except:
                try:
                    item['questionLatestActiveTime'] = response.xpath('//*[@id="zh-single-question-page"]//span[@class="time"]/text()').extract()[0]
                except:
                    item['questionLatestActiveTime'] =''
                    print "Error in questionLatestActiveTime : %s" %response.url

            try:
                item['questionShowTimes'] = int(response.xpath('//*[@id="zh-single-question-page"]/div[@class="zu-main-sidebar"]/div[last()-1]//div[@class="zg-gray-normal"][2]/strong[1]/text()').extract()[0])
            except:
                try:
                    item['questionShowTimes'] = int(response.xpath('//*[@id="zh-single-question-page"]/div[@class="zu-main-sidebar"]/div[last()]//div[@class="zg-gray-normal"][2]/strong[1]/text()').extract()[0])
                except:
                    logging.error('Error in questionShowTimes : %s',response.xpath('//*[@id="zh-single-question-page"]/div[@class="zu-main-sidebar"]/div[last()]//div[@class="zg-gray-normal"][2]/strong[1]/text()').extract())
            try:
                item['topicRelatedFollowerCount'] = int(response.xpath('//*[@id="zh-single-question-page"]/div[@class="zu-main-sidebar"]/div[last()-1]//div[@class="zg-gray-normal"][2]/strong[2]/text()').extract()[0])
            except:
                item['topicRelatedFollowerCount'] = int(response.xpath('//*[@id="zh-single-question-page"]/div[@class="zu-main-sidebar"]/div[last()]//div[@class="zg-gray-normal"][2]/strong[2]/text()').extract()[0])


            try:
                item['relatedQuestionLinkList'] = response.xpath('//*[@id="zh-question-related-questions"]//ul//li//a/@href').extract()     #should try
            except IndexError,e:
                item['relatedQuestionLinkList'] = []

            yield item

        # for index,url in enumerate(self.urls):
        #     yield Request(url,meta = {'cookiejar':index},callback = self.parse_page)




    def closed(self,reason):

        redis15 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=15)
        # redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)

        #这样的顺序是为了防止两个几乎同时结束
        p15=redis15.pipeline()
        p15.lpush(str(self.name),self.spider_number)
        p15.llen(str(self.name))
        finishedCount= p15.execute()[1]
        pipelineLimit = 10000
        batchLimit = 1000
        crawler_log = {'project':settings.BOT_NAME,
                       'spider':self.name,
                       'spider_type':self.spider_type,
                       'spider_number':self.spider_number,
                       'partition':self.partition,
                       'type':'close',
                       'stats':self.stats.get_stats(),
                       'updated_at':datetime.datetime.now()}
        self.col_log.insert_one(crawler_log)
        if int(self.partition)==int(finishedCount):
            #删除其他标记
            redis15.ltrim(str(self.name),0,0)
            #清空队列
            redis15.rpop(self.name)
            #清空缓存数据的redis11数据库
            # redis11.flushdb()

            logging.warning('Begin to request next schedule')
            response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST[self.name]+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT[self.name]+'/schedule.json',data=settings.NEXT_SCHEDULE_PAYLOAD[self.name])
            logging.warning('Response: '+' '+str(response))
        logging.warning('finished close.....')


