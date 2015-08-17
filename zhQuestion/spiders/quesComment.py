# -*- coding: utf-8 -*-
import scrapy
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest


import logging

import redis
import requests

from zhQuestion.items import QuesCommentItem
from zhQuestion import settings

import happybase
from pymongo import MongoClient
import datetime


class QuescommentSpider(scrapy.Spider):
    name = "quesComment"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    handle_httpstatus_list = [401,429,500,502,503,504]
    baseUrl = 'http://www.zhihu.com/node/QuestionCommentListV2?params={"question_id":%s}'
    questionIdList=[]
    questionDataResourceIdList=[]

    quesIndex =0
    reqLimit =20
    pipelineLimit = 100000
    threhold = 100

    def __init__(self,stats,spider_type='Master',spider_number=0,partition=1,**kwargs):
        self.redis2 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=2)
        self.stats = stats
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_log = db['log']


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

        self.questionIdList = self.redis2.keys()
        totalLength = len(self.questionIdList)

        p2 = self.redis2.pipeline()
        if self.spider_type=='Master':
            redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
            redis11.flushdb()

            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master partition is '+str(self.partition))
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.questionIdList)
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),-1)
                    if (index+1)%self.pipelineLimit == 0:
                        self.questionDataResourceIdList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionDataResourceIdList.extend(p2.execute())

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
                    p2.lindex(str(questionId),-1)
                    if (index+1)%self.pipelineLimit == 0:
                        self.questionDataResourceIdList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionDataResourceIdList.extend(p2.execute())

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.questionIdList)
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),-1)
                    if (index+1)%self.pipelineLimit == 0:
                        self.questionDataResourceIdList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionDataResourceIdList.extend(p2.execute())


            else:
                self.questionIdList = self.questionIdList[self.spider_number*totalLength/self.partition:]
                totalLength = len(self.questionIdList)
                for index ,questionId in enumerate(self.questionIdList):
                    p2.lindex(str(questionId),-1)
                    if (index+1)%self.pipelineLimit == 0:
                        self.questionDataResourceIdList.extend(p2.execute())
                    elif totalLength-index==1:
                        self.questionDataResourceIdList.extend(p2.execute())

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.questionIdList)))
        logging.warning('totalDataIdCount to request is :'+str(len(self.questionDataResourceIdList)))

        crawler_log = {'project':settings.BOT_NAME,
                       'spider':self.name,
                       'spider_type':self.spider_type,
                       'spider_number':self.spider_number,
                       'partition':self.partition,
                       'type':'start',
                       'total_count':len(self.questionIdList),
                       'updated_at':datetime.datetime.now()}

        self.col_log.insert_one(crawler_log)
        for index ,questionDataResourceId in enumerate(self.questionDataResourceIdList):
                reqUrl = self.baseUrl %str(questionDataResourceId)
                yield Request(url =reqUrl
                                      ,meta={'questionId':self.questionIdList[index],
                                             'proxy':settings.HTTP_PROXY_LIST[self.spider_number]}

                                      ,dont_filter=True
                                      ,callback=self.parsePage
                                      )







    def parsePage(self,response):
        if response.status != 200:
            yield Request(response.url,meta={'questionId':response.meta['questionId'],
                                             'proxy':settings.HTTP_PROXY_LIST[self.spider_number]},callback=self.parsePage)
        else:
            item = QuesCommentItem()
            item['spiderName'] = self.name
            sels=response.xpath('//div[@class="zm-item-comment"]')
            if len(sels):

                item['questionId'] = response.meta['questionId']
                # 注意，这里因为没有userDataId,因此其他的辅助信息也是有价值的
                for sel in sels:
                    item['commentDataId'] = sel.xpath('@data-id').extract()[0]
                    item['commentContent'] =sel.xpath('div[@class="zm-comment-content-wrap"]/div[@class="zm-comment-content"]/text()').extract()[0]
                    item['commentDate'] = sel.xpath('div[@class="zm-comment-content-wrap"]/div[@class="zm-comment-ft"]/span[@class="date"]/text()').extract()[0]
                    item['commentUpCount'] = sel.xpath('div[@class="zm-comment-content-wrap"]/div[@class="zm-comment-ft"]/span[contains(@class,"like-num")]/em/text()').extract()[0]

                    try:
                        item['userLinkId'] = sel.xpath('a[@class="zm-item-link-avatar"]/@href').re(r'/people/(.*)')[0]
                    except:
                        item['userLinkId']=''

                    try:
                        item['userName'] = sel.xpath('a[@class="zm-item-link-avatar"]//@title').extract()[0]
                    except:
                        item['userName'] =''

                    try:
                        item['userImgLink'] = sel.xpath('a[@class="zm-item-link-avatar"]/img/@src').extract()[0]
                    except:
                        item['userImgLink']=''


                    yield item
            else:
                item['questionId'] =''
                yield  item

    def closed(self,reason):
        redis15 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=15)
        redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)


        #这样的顺序是为了防止两个几乎同时结束
        p15=redis15.pipeline()
        p15.lpush(str(self.name),self.spider_number)
        p15.llen(str(self.name))
        finishedCount= p15.execute()[1]
        pipelineLimit = 100000
        batchLimit = 1000
        crawler_log = {'project':settings.BOT_NAME,
                       'spider':self.name,
                       'spider_type':self.spider_type,
                       'spider_number':self.spider_number,
                       'partition':self.partition,
                       'type':'close',
                       'total_count':len(self.questionIdList),
                       'stats':self.stats,
                       'updated_at':datetime.datetime.now()}
        self.col_log.insert_one(crawler_log)

        if int(self.partition)==int(finishedCount):
            #删除其他标记
            redis15.ltrim(str(self.name),0,0)
            # connection = happybase.Connection(settings.HBASE_HOST)
            #
            # questionTable = connection.table('question')



            # commentDataIdList = redis11.keys()
            # p11 = redis11.pipeline()
            #
            # for index ,commentDataId in enumerate(commentDataIdList):
            #     p11.hgetall(str(commentDataId))
            #
            #     if index % pipelineLimit == 0:
            #         quesCommentDictList = p11.execute()
            #         with  commentTable.batch(batch_size=batchLimit, transaction=True):
            #             for innerIndex, quesCommentDict in enumerate(quesCommentDictList):
            #                 commentTable.put(str(quesCommentDict['detail::quesId']),
            #                                  {'detail:quesId': quesCommentDict['detail:quesId']
            #                                      , 'detail:commentDataId': str(quesCommentDict['detail:commentDataId']),
            #                                   'detail:commentContent': str(quesCommentDict['detail:commentContent']),
            #                                   'detail:commentDate': str(quesCommentDict['detail:commentDate']),
            #                                   'detail:commentUpCount': str(quesCommentDict['detail:commentUpCount']),
            #                                   'detail:userName': str(quesCommentDict['detail:userName']),
            #                                   'detail:userLinkId': quesCommentDict['detail:userLinkId'],
            #                                   'detail:userImgLink': str(quesCommentDict['detail:userImgLink']),
            #                                   })
            #
            #
            #
            #     elif len(commentDataIdList) - index == 1:
            #         quesCommentDictList = p11.execute()
            #         with  commentTable.batch(batch_size=batchLimit, transaction=True):
            #             for innerIndex, quesCommentDict in enumerate(quesCommentDictList):
            #                 commentTable.put(str(quesCommentDict['detail::quesId']),
            #                                  {'detail:quesId': quesCommentDict['detail:quesId']
            #                                      , 'detail:commentDataId': str(quesCommentDict['detail:commentDataId']),
            #                                   'detail:commentContent': str(quesCommentDict['detail:commentContent']),
            #                                   'detail:commentDate': str(quesCommentDict['detail:commentDate']),
            #                                   'detail:commentUpCount': str(quesCommentDict['detail:commentUpCount']),
            #                                   'detail:userName': str(quesCommentDict['detail:userName']),
            #                                   'detail:userLinkId': quesCommentDict['detail:userLinkId'],
            #                                   'detail:userImgLink': str(quesCommentDict['detail:userImgLink']),
            #                                   })

            # questionIdList = redis11.keys()
            # p11 = redis11.pipeline()
            # tmpQuestionList = []
            # totalLength = len(questionIdList)
            # for index, questionId in enumerate(questionIdList):
            #     p11.smembers(str(questionId))
            #     tmpQuestionList.append(str(questionId))
            #
            #     if (index + 1) % pipelineLimit == 0:
            #         questionCommentDataIdSetList = p11.execute()
            #         with  questionTable.batch(batch_size=batchLimit):
            #             for innerIndex, questionCommentDataIdSet in enumerate(questionCommentDataIdSetList):
            #
            #                 questionTable.put(str(tmpQuestionList[innerIndex]),
            #                                   {'comment:dataIdList': str(list(questionCommentDataIdSet))})
            #             tmpQuestionList=[]
            #
            #
            #     elif  totalLength - index == 1:
            #         questionCommentDataIdSetList = p11.execute()
            #         with  questionTable.batch(batch_size=batchLimit):
            #             for innerIndex, questionCommentDataIdSet in enumerate(questionCommentDataIdSetList):
            #
            #                 questionTable.put(str(tmpQuestionList[innerIndex]),
            #                                   {'comment:dataIdList': str(list(questionCommentDataIdSet))})
            #             tmpQuestionList=[]
            #清空队列
            redis15.rpop(self.name)
            #清空缓存数据的redis11数据库
            redis11.flushdb()
            logging.warning('Begin to request next schedule')
            response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST[self.name]+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT[self.name]+'/schedule.json',data=settings.NEXT_SCHEDULE_PAYLOAD[self.name])
            logging.warning('Response: '+' '+str(response))
        logging.warning('finished close.....')
