# -*- coding: utf-8 -*-
import scrapy
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request

import logging

import redis
import requests

from zhQuestion.items import QuesRootItem
from zhQuestion import settings

import happybase

class QuesfrontSpider(scrapy.Spider):
    name = "quesRoot"
    allowed_domains = ["zhihu.com"]
    start_urls = ["http://www.zhihu.com/topic/19776749/questions"]
    handle_httpstatus_list = [401,429,500,502,503,504]
    baseUrl = 'http://www.zhihu.com/topic/19776749/questions?page=%s'


    def __init__(self,spider_type='Master',spider_number=0,partition=1,**kwargs):

        try:
            self.spider_type = str(spider_type)
            self.spider_number = int(spider_number)
            self.partition = int(partition)
            self.email= settings.EMAIL_LIST[self.spider_number]
            self.password=settings.PASSWORD_LIST[self.spider_number]

        except:
            self.spider_type = 'Master'
            self.spider_number = 0
            self.partition = 1
            self.email= settings.EMAIL_LIST[self.spider_number]
            self.password=settings.PASSWORD_LIST[self.spider_number]

    def parse(self, response):

        totalLength = int(response.xpath('//div[@class="border-pager"]//span[last()-1]/a/text()').extract()[0])
        logging.warning('Total page number is %s',str(totalLength))
        self.requestPageList = range(1,totalLength+1)
        if self.spider_type=='Master':
            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master non 1 partition is '+str(self.partition))
                self.requestPageList = self.requestPageList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]

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

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.requestPageList = self.requestPageList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]

            else:
                self.requestPageList = self.requestPageList[self.spider_number*totalLength/self.partition:]

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.requestPageList)))



        for pageIndex in self.requestPageList:
            reqUrl = self.baseUrl %str(pageIndex)
            # logging.warning('reqUrl: %s',reqUrl)
            yield  Request(url = reqUrl,callback=self.parsePage)



    def parsePage(self,response):
        # logging.warning('papapa')
        if response.status != 200:
            yield Request(response.url,callback=self.parsePage)
        else:
            item = QuesRootItem()
            # logging.warning('response.url: %s \n response.body: %s',response.url,response.body)
            item['spiderName'] = self.name
            for sel in response.xpath('//div[@id="zh-topic-questions-list"]//div[@itemprop="question"]'):

                item['questionTimestamp'] = sel.xpath('h2[@class="question-item-title"]/span[@class="time"]/@data-timestamp').extract()[0]
                item['questionId'] = sel.xpath('h2[@class="question-item-title"]/a[@class="question_link"]/@href').re(r'/question/(\d*)')[0]
                try:
                    # item['subTopicName'] = sel.xpath('div[@class="subtopic"]/a/text()').extract()[0]
                    item['subTopicId'] = sel.xpath('div[@class="subtopic"]/a/@href').re(r'/topic/(\d*)')[0]

                except IndexError,e:
                    # item['subTopicName'] = ''
                    item['subTopicId'] = ''

                yield item

                #暂时不需要的多余信息
                # try:
                #     item['questionName'] = sel.xpath('h2[@class="question-item-title"]/a[@class="question_link"]/text()').extract()[0]
                # except IndexError,e:
                #     item['questionName'] = ''
                # item['answerCount'] = int(sel.xpath('meta[@itemprop="answerCount"]/@content').extract()[0])
                # item['isTopQuestion'] = sel.xpath('meta[@itemprop="isTopQuestion"]/@content').extract()[0]



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



            # connection = happybase.Connection(settings.HBASE_HOST)
            # questionTable = connection.table('question')
            #
            #
            # questionIdList = redis11.keys()
            # p11 = redis11.pipeline()
            #
            #
            # for index ,questionId in enumerate(questionIdList):
            #     p11.hgetall(str(questionId))
            #
            #     if index%pipelineLimit == 0:
            #         quesBasicDictList = p11.execute()
            #         with  questionTable.batch(batch_size=batchLimit,transaction=True):
            #             for innerIndex,quesBasicDict in enumerate(quesBasicDictList):
            #
            #                 questionTable.put(str(quesBasicDict['basic:quesId']),{'basic:quesId':quesBasicDict['basic:quesId'],
            #                                                        # 'basic:answerCount':str(item['answerCount']),
            #                                                        # 'basic:isTopQues':str(isTopQuestion),
            #                                                        # 'basic:subTopicName':item['subTopicName'].encode('utf-8'),
            #                                                        'basic:subTopicId':quesBasicDict['basic:subTopicId'],
            #                                                        'basic:quesTimestamp':quesBasicDict['basic:quesTimestamp'],
            #                                                        # 'basic:quesName':item['questionName'].encode('utf-8'),
            #                                                        'basic:quesIndex':quesBasicDict['basic:quesIndex']})
            #
            #
            #     elif len(questionIdList)-index==1:
            #         quesBasicDictList = p11.execute()
            #         with  questionTable.batch(batch_size=batchLimit,transaction=True):
            #             for innerIndex,quesBasicDict in enumerate(quesBasicDictList):
            #
            #                 questionTable.put(str(quesBasicDict['basic:quesId']),{'basic:quesId':quesBasicDict['basic:quesId'],
            #                                                        # 'basic:answerCount':str(item['answerCount']),
            #                                                        # 'basic:isTopQues':str(isTopQuestion),
            #                                                        # 'basic:subTopicName':item['subTopicName'].encode('utf-8'),
            #                                                        'basic:subTopicId':quesBasicDict['basic:subTopicId'],
            #                                                        'basic:quesTimestamp':quesBasicDict['basic:quesTimestamp'],
            #                                                        # 'basic:quesName':item['questionName'].encode('utf-8'),
            #                                                        'basic:quesIndex':quesBasicDict['basic:quesIndex']})


 # self.stats = stats
        #print "Initianizing ....."
        # self.redis0 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=0)




    #
    # @classmethod
    # def from_crawler(cls, crawler):
    #     return cls(crawler.stats)



                        #
                # for index ,questionId in enumerate(self.questionIdList):
                #     p2.lindex(str(questionId),6)
                #     if index%self.pipelineLimit ==0:
                #         self.questionFollowerCountList.extend(p2.execute())
                #     elif questionIdListLength-index==1:
                #         self.questionFollowerCountList.extend(p2.execute())
                #     # p2 = self.redis2.pipeline()
