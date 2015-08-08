# -*- coding: utf-8 -*-
import scrapy

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.shell import inspect_response



from datetime import datetime
from zhQuestion import settings

from zhQuestion.items import QuesInfoItem

import re
import redis
import requests
import logging
import happybase

class QuesinfoerSpider(scrapy.Spider):
    name = "quesInfo"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    handle_httpstatus_list = [401,429,500,502,503,504]
    baseUrl = "http://www.zhihu.com/question/"

    quesIndex =0


    def __init__(self,spider_type='Master',spider_number=0,partition=1,**kwargs):
        self.redis1 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=1)

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
                    response = requests.post('http://'+settings.SCRAPYD_HOST_LIST[index]+':'+settings.SCRAPYD_PORT_LIST[index]+'/schedule.json',data=payload)
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
            logging.error('Login failed! %s',self.email)

        #inspect_response(response,self)
        #self.urls = ['http://www.zhihu.com/question/28626263','http://www.zhihu.com/question/22921426','http://www.zhihu.com/question/20123112']
        for questionId in self.questionIdList:
            yield self.make_requests_from_url(self.baseUrl +str(questionId)+'?nr=1')


    def parse(self,response):
        if response.status != 200:
#            print "ParsePage HTTPStatusCode: %s Retrying !" %str(response.status)
            yield  self.make_requests_from_url(response.url)

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

        if int(self.partition)==int(finishedCount):
            #删除其他标记
            redis15.ltrim(str(self.name),0,0)

            # connection = happybase.Connection(settings.HBASE_HOST)
            # questionTable = connection.table('question')
            #
            #
            # questionIdList = redis11.keys()
            # p11 = redis11.pipeline()
            #
            #
            #
            #
            # for index ,questionId in enumerate(questionIdList):
            #     p11.hgetall(str(questionId))
            #
            #     if index%pipelineLimit == 0:
            #         quesDetailDictList = p11.execute()
            #         with  questionTable.batch(batch_size=batchLimit,transaction=True):
            #             for innerIndex,quesDetailDict in enumerate(quesDetailDictList):
            #
            #                 questionTable.put(str(quesDetailDict['detail::quesId']),
            #                                   {'detail:quesId':quesDetailDict['detail:quesId']
            #                                   ,'detail:idZnonceContent':str(quesDetailDict['detail:idZnonceContent']),
            #                                        'detail:dataUrlToken':str(quesDetailDict['detail:dataUrlToken']),
            #                                        'detail:isTopQues':str(quesDetailDict['detail:isTopQues']),
            #                                        'detail:tagLabelIdList': str(quesDetailDict['detail:tagLabelIdList']),
            #                                        'detail:tagLabelDataTopicIdList': str(quesDetailDict['detail:tagLabelDataTopicIdList']),
            #                                        'detail:quesTitle': quesDetailDict['detail:quesTitle'],
            #                                        'detail:dataResourceId': str(quesDetailDict['detail:dataResourceId']),
            #                                        'detail:quesAnswerCount': str(quesDetailDict['detail:quesAnswerCount']),
            #                                        'detail:quesFollowerCount': str(quesDetailDict['detail:quesFollowerCount']),
            #                                        'detail:quesLatestActiveTime': quesDetailDict['detail:quesLatestActiveTime'],
            #                                        'detail:quesShowTimes': str(quesDetailDict['detail:quesShowTimes']),
            #                                        'detail:topicRelatedFollowerCount': str(quesDetailDict['detail:topicRelatedFollowerCount']),
            #                                        'detail:quesContent': quesDetailDict['detail:quesContent'],
            #                                        'detail:relatedQuesLinkList': str(quesDetailDict['detail:relatedQuesLinkList']),
            #                                        'detail:quesCommentCount': str(quesDetailDict['detail:quesCommentCount']),
            #                                        'detail:visitsCount': str(quesDetailDict['detail:visitsCount'])})
            #
            #
            #     elif len(questionIdList)-index==1:
            #         quesDetailDictList = p11.execute()
            #         with  questionTable.batch(batch_size=batchLimit,transaction=True):
            #             for innerIndex,quesDetailDict in enumerate(quesDetailDictList):
            #
            #                 questionTable.put(str(quesDetailDict['detail::quesId']),
            #                                   {'detail:quesId':quesDetailDict['detail:quesId']
            #                                   ,'detail:idZnonceContent':str(quesDetailDict['detail:idZnonceContent']),
            #                                        'detail:dataUrlToken':str(quesDetailDict['detail:dataUrlToken']),
            #                                        'detail:isTopQues':str(quesDetailDict['detail:isTopQues']),
            #                                        'detail:tagLabelIdList': str(quesDetailDict['detail:tagLabelIdList']),
            #                                        'detail:tagLabelDataTopicIdList': str(quesDetailDict['detail:tagLabelDataTopicIdList']),
            #                                        'detail:quesTitle': quesDetailDict['detail:quesTitle'],
            #                                        'detail:dataResourceId': str(quesDetailDict['detail:dataResourceId']),
            #                                        'detail:quesAnswerCount': str(quesDetailDict['detail:quesAnswerCount']),
            #                                        'detail:quesFollowerCount': str(quesDetailDict['detail:quesFollowerCount']),
            #                                        'detail:quesLatestActiveTime': quesDetailDict['detail:quesLatestActiveTime'],
            #                                        'detail:quesShowTimes': str(quesDetailDict['detail:quesShowTimes']),
            #                                        'detail:topicRelatedFollowerCount': str(quesDetailDict['detail:topicRelatedFollowerCount']),
            #                                        'detail:quesContent': quesDetailDict['detail:quesContent'],
            #                                        'detail:relatedQuesLinkList': str(quesDetailDict['detail:relatedQuesLinkList']),
            #                                        'detail:quesCommentCount': str(quesDetailDict['detail:quesCommentCount']),
            #                                        'detail:visitsCount': str(quesDetailDict['detail:visitsCount'])})
            #清空队列
            redis15.rpop(self.name)
            #清空缓存数据的redis11数据库
            # redis11.flushdb()

            logging.warning('Begin to request next schedule')
            response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST[self.name]+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT[self.name]+'/schedule.json',data=settings.NEXT_SCHEDULE_PAYLOAD[self.name])
            logging.warning('Response: '+' '+str(response))
        logging.warning('finished close.....')



        # self.spider_number = spider_number
        # self.spider_number = spider_number
        # leancloud.init(settings.APP_ID_S, master_key=settings.MASTER_KEY_S)
        # client1 = bmemcached.Client(settings.CACHE_SERVER_1,settings.CACHE_USER_1,settings.CACHE_PASSWORD_1)
        # client2 = bmemcached.Client(settings.CACHE_SERVER_2,settings.CACHE_USER_2,settings.CACHE_PASSWORD_2)
       # redis0 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=0)

        # self.questionIdList = redis0.hvals('questionIndex')
        # questionIdListLength = len(self.questionIdList)



    # def __init__(self,spider_type='Master',spider_number=1,emailList=['heamon8@163.com'],passwordList=['heamon8@()'],**kwargs):
    #     # self.stats = stats
    #     print "Initianizing ....."
    #     scrapyd = ScrapydAPI('http://localhost:6800')
    #
    #     # leancloud.init(settings.APP_ID_S, master_key=settings.MASTER_KEY_S)
    #     # client1 = bmemcached.Client(settings.CACHE_SERVER_1,settings.CACHE_USER_1,settings.CACHE_PASSWORD_1)
    #     # client2 = bmemcached.Client(settings.CACHE_SERVER_2,settings.CACHE_USER_2,settings.CACHE_PASSWORD_2)
    #     redis0 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_USER+':'+settings.REDIS_PASSWORD,db=0)
    #     dbPrime = 97
    #     self.email= emailList[0]
    #     self.password=passwordList[0]
    #     self.questionIdList = redis0.hvals('questionIndex')
    #     questionIdListLength = len(self.questionIdList)
    #     if spider_type=='Master':
    #         if int(spider_number)!=1:
    #             self.questionIdList = self.questionIdList[0:1*questionIdListLength/spider_number]
    #             for index in range(1,int(spider_number)):
    #                 scrapyd.schedule('zhQuesInfo', 'quesInfoer'
    #                                  , settings='JOBDIR=/tmp/scrapy/zhihu/quesInfoer'+str(index)
    #                                  ,spider_type='Slave'
    #                                  ,spider_number=index
    #                                  ,emailList=emailList[index]
    #                                  ,passwordList=passwordList[index])

        # print "totalCount: %s\n" %str(totalCount)
        # for questionIndex in range(0,totalCount+1):
        #     self.questionIdSet.add(int(client2.get(str(questionIndex))))

        # for tableIndex in range(dbPrime):
        #     if tableIndex < 10:
        #         tableIndexStr = '0' + str(tableIndex)
        #     else:
        #         tableIndexStr = str(tableIndex)

            # Question = Object.extend('Question' + tableIndexStr)
            # query = Query(Question)
            # query.exists('questionId')
            #
            # # 避免在查询时，仍然有新的Question入库
            # # curTime = datetime.now()
            # # query.less_than('createdAt',curTime)
            #
            # questionNum = query.count()
            # print "[%s] total questionNums: %d in tableIndex: %s\n" %(datetime.now(),questionNum, tableIndexStr)
            # queryLimit = 700
            # queryTimes = questionNum/queryLimit + 1
            #
            # for index in range(queryTimes):
            #     query = Query(Question)
            #     # query.less_than('createdAt',Question)
            #     query.exists('questionId')
            #     query.descending('createdAt')
            #     query.limit(queryLimit)
            #     query.skip(index*queryLimit)
            #     query.select('questionId')
            #     query.select('tableIndex')
            #
            #     try:
            #         quesRet = query.find()
            #     except:
            #         try:
            #             quesRet = query.find()
            #         except:
            #             try:
            #                 quesRet = query.find()
            #             except:
            #                 quesRet = query.find()
            #
            #
            #     for ques in quesRet:
            #         quesInfoList =[]
            #         questionId = int(ques.get('questionId'))
            #         if questionId in self.questionIdSet :
            #             pass
            #         else:
            #
            #             client_s.incr('totalCount',1)
            #             client_s.incr('t'+tableIndexStr,1)
            #             quesInfoList.append(questionId)
            #             quesInfoList.append(int(ques.get('tableIndex')))
            #             self.questionIdSet.add(questionId)
            #             client_s.set(str(self.quesIndex),quesInfoList)
            #             self.quesIndex +=1


         # Questions = Object.extend('Questions')
         # query = Query(Questions)
         # query.exists('questionId')
         # curTime = datetime.now()
         # query.less_than('createdAt',curTime)
         #
         # questionNum = query.count()
         # print "questionNums: %s" %str(questionNum)
         # queryLimit = 500
         # queryTimes = questionNum/queryLimit + 1
         # self.urls = []
         # for index in range(queryTimes):
         #    query = Query(Questions)
         #    query.less_than('createdAt',curTime)
         #    query.exists('questionLinkHref')
         #    query.descending('createdAt')
         #    query.limit(queryLimit)
         #    query.skip(index*queryLimit)
         #    query.select('questionLinkHref')
         #    quesRet = query.find()
         #    for ques in quesRet:
         #        self.urls.append("http://www.zhihu.com"+ ques.get('questionLinkHref'))
    # @classmethod
    # def from_crawler(cls, crawler,**kwargs):
    #     return cls(crawler.stats)

    # def start_requests(self):
    #     #print "start_requests ing ......"
    #     yield Request("http://www.zhihu.com",callback = self.post_login)


# def testScrapyd(self,response):
    #     item =  ZhquesinfoItem()
    #     log.msg('spider_type: '+str(self.spider_type)
    #             +'\nspider_number: '+str(self.spider_number)
    #             +'\npartition: '+str(self.partition)
    #             +'\nemail: '+str(self.email)
    #             +'\npassword: '+str(self.password)
    #             +'\nquestionIdList: '+str(self.questionIdList)
    #             ,level=log.WARNING)
    #     yield item


        # print "spider_type: %s\nspider_number: %s\npartition: %email: %s\npassword: %s\nquestionIdList: %s" % (self.spider_type
        #                                                                                                        ,self.spider_number
        #                                                                                                        ,self.partition
        #                                                                                                        ,self.email
        #                                                                                                        ,self.password
        #                                                                                                        ,str(self.questionIdList))



            #item['dataPageSize'] = response.xpath('//*[@id="zh-question-answer-wrap"]').extract()
            #item['pageSize'] = response.xpath('').extract()
            #item['offset'] = response.xpath('').extract()
            #item['nodeName'] = response.xpath('//*[@id="zh-question-answer-wrap"]/@nodename').extract()


            # item['dataAid'] = response.xpath('//*[@id="zh-question-answer-wrap"]/div[1]').extract()
            #
            # item['dataAtoken'] = response.xpath('').extract()
            # item['dataCreated'] = response.xpath('').extract()
            # item['dataDeleted'] = response.xpath('').extract()
            # item['dataHelpful'] = response.xpath('').extract()
            # item['dataScore'] = response.xpath('').extract()

            #item['questionFollowDataId'] = response.xpath('//*[@id="zh-question-side-header-wrap"]/button').extract()

            #item['questionFollowerLink'] = response.xpath('//*[@id="zh-question-side-header-wrap"]/div[@class="zh-question-followers-sidebar"]/div[1]/a/@href').extract()[0]
            #item['quescommentcounttionFollowerList'] = response.xpath('//*[@id="zh-question-side-header-wrap"]/div[@class="zh-question-followers-sidebar"]/div[2]').extract()

            #item['sideSectionId'] = response.xpath('//*[@id="shameimaru-question-up-83594d68c"]').extract()


            #item['shareDataId'] = response.xpath('//*[@id="zh-question-webshare-container"]').extract()


            # item[''] = response.xpath('').extract()
            # item[''] = response.xpath('').extract()
            # item[''] = response.xpath('').extract()
