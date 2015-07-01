# -*- coding: utf-8 -*-
import scrapy

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.selector import Selector
from scrapy.shell import inspect_response

from datetime import datetime
from zhQuestion import settings

from zhQuestion.items import QuesFollowerItem

import re

import json
import redis
import happybase
import requests
import logging

class QuesfollowerSpider(scrapy.Spider):
    name = "quesFollower"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    handle_httpstatus_list = [401,429,500,502,504]
    baseUrl = "http://www.zhihu.com/question/"
    questionIdList = []
    questionFollowerCountList = []
    questionInfoList = []
    quesIndex =0
    reqLimit =20
    pipelineLimit = 100000
    threhold = 100


    def __init__(self,spider_type='Master',spider_number=0,partition=1,**kwargs):

        self.redis0 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=0)
        self.redis2 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=2)

        self.spider_type = str(spider_type)
        self.spider_number = int(spider_number)
        self.partition = int(partition)
        self.email= settings.EMAIL_LIST[self.spider_number]
        self.password=settings.PASSWORD_LIST[self.spider_number]

    def start_requests(self):

        self.questionIdList = self.redis2.keys()
        totalLength = len(self.questionIdList)

        p2 = self.redis2.pipeline()

        if self.spider_type=='Master':
            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master partition is '+str(self.partition))
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]

                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),6)
                    if index%self.pipelineLimit ==0:
                        self.questionFollowerCountList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionFollowerCountList.extend(p2.execute())

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
                    response = requests.post('http://'+settings.SCRAPYD_HOST_LIST[index]+':'+settings.SCRAPYD_PORT_LIST[index]+'/schedule.json',data=payload)
                    logging.warning('Response: '+str(index)+' '+str(response))
            else:
                logging.warning('Master  partition is '+str(self.partition))
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),6)
                    if index%self.pipelineLimit ==0:
                        self.questionFollowerCountList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionFollowerCountList.extend(p2.execute())

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]

                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),6)
                    if index%self.pipelineLimit ==0:
                        self.questionFollowerCountList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionFollowerCountList.extend(p2.execute())


            else:
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:]
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),6)
                    if index%self.pipelineLimit ==0:
                        self.questionFollowerCountList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionFollowerCountList.extend(p2.execute())

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.questionIdList)))
        yield Request("http://www.zhihu.com/",callback = self.post_login)

    def post_login(self,response):

        logging.warning('post_login ing ......')
        xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
        yield FormRequest.from_response(response,
                                          formdata={
                                              '_xsrf':xsrfValue,
                                              'email':self.email,
                                              'password':self.password,
                                              'rememberme': 'y'
                                          },
                                          dont_filter = True,
                                          callback = self.after_login,
                                          )

    def after_login(self,response):
        try:
            loginUserLink = response.xpath('//div[@id="zh-top-inner"]/div[@class="top-nav-profile"]/a/@href').extract()[0]
            logging.warning('Successfully login with %s  %s  %s',str(loginUserLink),str(self.email),str(self.password))
        except:
            logging.error('Login failed! %s   %s',self.email,self.password)

        for index ,questionId in enumerate(self.questionIdList):
            if self.questionFollowerCountList[index]:
                xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
                reqUrl = self.baseUrl+str(questionId)+'/followers'
                reqTimes = (int(self.questionFollowerCountList[index])+self.reqLimit-1)/self.reqLimit
                for index in reversed(range(reqTimes)):
                    offset =str(self.reqLimit*index)
                    yield FormRequest(url =reqUrl
                                      ,meta={'xsrfValue':xsrfValue,'offset':str(offset)}
                                      , formdata={
                                            '_xsrf': xsrfValue,
                                            'start': '0',
                                            'offset': str(offset),
                                        }
                                      ,dont_filter=True
                                      ,callback=self.parsePage
                                      )


    def parsePage(self,response):
        if response.status != 200:
            yield FormRequest(url =response.request.url
                                      ,meta={'xsrfValue':response.meta['xsrfValue'],'offset':response.meta['offset']}
                                      , formdata={
                                            '_xsrf': response.meta['xsrfValue'],
                                            'start': '0',
                                            'offset':str(response.meta['offset']),
                                        }
                                      ,dont_filter=True
                                      ,callback=self.parsePage
                                      )
        else:
            item =  QuesFollowerItem()
            data = json.loads(response.body)
            userCountRet = data['msg'][0]

            item['offset'] = response.meta['offset']
            item['questionId'] = re.split('http://www.zhihu.com/question/(\d*)/followers',response.url)[1]
            #这里注意要处理含有匿名用户的情况
            if userCountRet:
                res = Selector(text = data['msg'][1])
                for sel in res.xpath('//div[contains(@class,"zm-profile-card")]'):
                    item['userDataId'] = sel.xpath('button/@data-id').extract()[0]
                    # 注意userLinkId中可能有中文
                    item['userLinkId'] = sel.xpath('a[@class="zm-item-link-avatar"]/@href').re(r'/people/(.*)')[0]

                    # item['userImgUrl'] = sel.xpath('a[@class="zm-item-link-avatar"]/img/@src').extract()[0]
                    # item['userName'] = sel.xpath('h2/a/text()').extract()[0]
                    # item['userFollowerCount'] = sel.xpath('div[@class="details zg-gray"]/a[1]//text()').re(r'(\-?\d+)')[0]
                    # item['userAskCount'] = sel.xpath('div[@class="details zg-gray"]/a[2]//text()').re(r'(\-?\d+)')[0]
                    # item['userAnswerCount'] = sel.xpath('div[@class="details zg-gray"]/a[3]//text()').re(r'(\-?\d+)')[0]
                    # item['userUpCount'] = sel.xpath('div[@class="details zg-gray"]/a[4]//text()').re(r'(\-?\d+)')[0]

                    #如果是匿名用户，则标记之
                    if not item['userDataId']:
                        item['userDataId']=''

                    yield item

            else:
                #没有用户
                item['userDataIdList']=''
                yield item





    #
    #
    def closed(self,reason):
        redis15 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=15)
        redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)


        #这样的顺序是为了防止两个几乎同时结束
        p15=redis15.pipeline()
        p15.lpush(str(self.name),self.spider_number)
        p15.llen(str(self.name))
        finishedCount= p15.execute()[1]
        pipelineLimit = 10000
        batchLimit = 1000

        if int(self.partition)==int(finishedCount):
            #删除其他标记
            redis15.ltrim(str(self.name),0,0)
            connection = happybase.Connection(settings.HBASE_HOST)

            questionTable = connection.table('question')

            questionIdList = redis11.keys()
            p11 = redis11.pipeline()
            tmpQuestionList = []
            for index, questionId in enumerate(questionIdList):
                p11.smembers(str(questionId))
                tmpQuestionList.append(str(questionId))

                if (index + 1) % pipelineLimit == 0:
                    questionFollowerDataIdSetList = p11.execute()
                    with  questionTable.batch(batch_size=batchLimit, transaction=True):
                        for innerIndex, questionFollowerDataIdSet in enumerate(questionFollowerDataIdSetList):
                            questionTable.put(str(tmpQuestionList[innerIndex]),
                                              {'follower:dataIdList': str(list(questionFollowerDataIdSet))})
                        tmpQuestionList=[]


                elif len(questionIdList) - index == 1:
                    questionFollowerDataIdSetList = p11.execute()
                    with  questionTable.batch(batch_size=batchLimit, transaction=True):
                        for innerIndex, questionFollowerDataIdSet in enumerate(questionFollowerDataIdSetList):
                            questionTable.put(str(tmpQuestionList[innerIndex]),
                                              {'follower:dataIdList': str(list(questionFollowerDataIdSet))})
                        tmpQuestionList=[]
            #清空队列
            redis15.rpop(self.name)
            #清空缓存数据的redis11数据库
            redis11.flushdb()

            payload=settings.NEXT_SCHEDULE_PAYLOAD
            logging.warning('Begin to request next schedule')
            response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT+'/schedule.json',data=payload)
            logging.warning('Response: '+' '+str(response))
        logging.warning('finished close.....')


    # def closed(self,reason):
    #
    #     redis5 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=5)
    #     redis15 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=15)
    #     connection = happybase.Connection(settings.HBASE_HOST)
    #     questionTable = connection.table('question')
    #
    #     logging.warning('Begin to transfer question follower data from redis to hbase')
    #     p5 = redis5.pipeline()
    #     for index ,questionId in enumerate(self.questionIdList):
    #         p5.smembers(str(questionId))
    #         if index%self.pipelineLimit ==0:
    #             for followerList in p5.execute():
    #                 try:
    #                     questionTable.put(str(questionId),{'follower:list':str(list(followerList))})
    #                 except Exception,e:
    #                     logging.error('Error with put questionId into hbase: '+str(e)+' try again......')
    #                     try:
    #                         questionTable.put(str(questionId),{'follower:list':str(list(followerList))})
    #                         logging.error(' tried again and successfully put data into hbase ......')
    #                     except Exception,e:
    #                         logging.error('Error with put questionId into hbase: '+str(e)+' '+str(questionId)+' tried again and failed')
    #
    #
    #         elif len(self.questionIdList)-index==1:
    #             for followerList in p5.execute():
    #                 try:
    #                     questionTable.put(str(questionId),{'follower:list':str(list(followerList))})
    #                 except Exception,e:
    #                     logging.error('Error with put questionId into hbase: '+str(e)+' try again......')
    #                     try:
    #                         questionTable.put(str(questionId),{'follower:list':str(list(followerList))})
    #                         logging.error(' tried again and successfully put data into hbase ......')
    #                     except Exception,e:
    #                         logging.error('Error with put questionId into hbase: '+str(e)+' '+str(questionId)+' tried again and failed')
    #
    #     logging.warning('Finished  transferring question follower data from redis to hbase')
    #     #这样的顺序是为了防止两个几乎同时结束
    #     p15=redis15.pipeline()
    #     p15.lpush(str(self.name),self.spider_number)
    #     p15.llen(str(self.name))
    #     finishedCount= p15.execute()[1]
    #
    #     if int(self.partition)==int(finishedCount):
    #         payload=settings.NEXT_SCHEDULE_PAYLOAD
    #         logging.warning('Begin to request next schedule')
    #         response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT+'/schedule.json',data=payload)
    #         logging.warning('Response: '+' '+str(response))
    #     logging.warning('finished close.....')



                    #     #f = open('../../nohup.out')
    #     #print f.read()
    #     leancloud.init(settings.APP_ID, master_key=settings.MASTER_KEY)
    #
    #
    #     CrawlerLog = Object.extend('CrawlerLog')
    #     crawlerLog = CrawlerLog()
    #
    #     crawlerLog.set('crawlerName',self.name)
    #     crawlerLog.set('closedReason',reason)
    #     crawlerLog.set('crawlerStats',self.stats.get_stats())
    #     try:
    #         crawlerLog.save()
    #     except:
    #         pass





        #log.start()
        # leancloud.init(settings.APP_ID_S, master_key=settings.MASTER_KEY_S)


        # client_2 = bmemcached.Client(settings.CACHE_SERVER_2,settings.CACHE_USER_2,settings.CACHE_PASSWORD_2)
        # client_4 = bmemcached.Client(settings.CACHE_SERVER_4,settings.CACHE_USER_4,settings.CACHE_PASSWORD_4)
        #


        # dbPrime = 97


        # for questionId in self.questionIdList:
        #     print "askfor followerCount %s"  %str(questionId)
        #     self.questionFollowerCountList.ex(redis2.lindex(str(questionId),4))

        # dbPrime = 97
        # totalCount = int(client_2.get('totalCount'))
        # for questionIndex in range(0,totalCount):
        #     self.questionIdSet.add(int(client_2.get(str(questionIndex))[0]))

            #貌似这样占用的内存太多了
        #这里要获得问题的id及其关注者的数量
        #因为不能一次获得所有的列表，所以需要分次

                # print "length of questionFollowerCountList: %s\n" %str(len(self.questionFollowerCountList))





            # if questionInfo:
            #     if int(questionInfo[4])>self.threhold:
            #
            #         self.questionIdList.append([questionId,questionInfo[4]])
            #     else:
            #         pass
            # else:
            #     pass

        # self.questionInfoList.append([20769127,838])


    # @classmethod
    # def from_crawler(cls, crawler):
    #     return cls(crawler.stats)





                # item['offset'] = response.meta['offset']
                # item['questionId'] = re.split('http://www.zhihu.com/question/(\d*)/followers',response.url)[1]
                # item['userDataIdList'] = sel.xpath('//button/@data-id').extract()
                # item['userLinkList'] = sel.xpath('//a[@class="zm-item-link-avatar"]/@href').extract()
                # item['userImgUrlList'] = sel.xpath('//a[@class="zm-item-link-avatar"]/img/@src').extract()
                # item['userNameList'] = sel.xpath('//h2/a/text()').extract()
                # item['userFollowersList'] = sel.xpath('//div[@class="details zg-gray"]/a[1]//text()').re(r'(\-?\d+)')
                # item['userAskList'] = sel.xpath('//div[@class="details zg-gray"]/a[2]//text()').re(r'(\-?\d+)')
                # item['userAnswerList'] = sel.xpath('//div[@class="details zg-gray"]/a[3]//text()').re(r'(\-?\d+)')
                # item['userUpList'] = sel.xpath('//div[@class="details zg-gray"]/a[4]//text()').re(r'(\-?\d+)')