# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import logging
from scrapy.exceptions import DropItem

from zhQuestion import settings
import time
import re
import redis
import happybase


# 这里统一采取先将本爬虫本次爬到的数据存入redis，待本次爬取完毕后，统一将数据持久化到hbase，
# 而不是像之前那样，拿到一个数据就往hbase里存一次
class QuesRootPipeline(object):

    def __init__(self):
        self.redis0 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=0)
        self.redis1 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=1)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        connection = happybase.Connection(settings.HBASE_HOST)
        self.questionTable = connection.table('question')

    def process_item(self, item, spider):
        if item['spiderName'] == 'quesRoot':
        #保证此次操作的原子性,其实如果是分布式的话，因为有分割，应该是不会冲突的，但是随着时间的增加还是有可能交叉
        #如果正在更新当前这个问题的data 则直接跳过
            # if  self.redis0.hsetnx('questionLock',str(item['questionId']),1):
                #使用try，是为了确保锁会被解除。
                # try:
                #     currentTimestamp = int(time.time())
                #     # 如果之前有过该问题的记录，这里可以直接跳过，并不需要更新
                #     # but 为了防止第一次插入数据库失败，需要以后有更新操作，这里更新时间可以设置长一些
                #     result = self.redis1.lrange(str(item['questionId']),0,1)
                #
                #     if result:
                #
                #         [recordTimestamp,questionIndex]=result
                #     else:
                #         [recordTimestamp,questionIndex]=('','')
                #
                #     p0= self.redis0.pipeline()
                #     p1 = self.redis1.pipeline()
                #     if not recordTimestamp:
                #         questionIndex = self.redis0.incr('totalCount',1)
                #         #为了减少redis的使用空间
                #         # p0.hsetnx('questionIndex'
                #         #           ,str(questionIndex)
                #         #           ,str(item['questionId']))
                #         p0.hsetnx('questionIdIndex'
                #                   ,str(item['questionId'])
                #                   ,str(questionIndex))
                #         p0.execute()
                #
                #         p1.incr('totalCount',1)
                #         p1.lpush(str(item['questionId'])
                #                      ,str(item['questionTimestamp'])
                #                      ,str(item['subTopicId'])
                #                      ,str(questionIndex)
                #                      ,str(recordTimestamp))
                #         p1.execute()
                #使用try，是为了确保锁会被解除。
            try:
                currentTimestamp = int(time.time())
                # 如果之前有过该问题的记录，这里可以直接跳过，并不需要更新
                # but 为了防止第一次插入数据库失败，需要以后有更新操作，这里更新时间可以设置长一些
                # result = self.redis1.lindex(str(item['questionId']),0)
                recordTimestamp = self.redis1.lindex(str(item['questionId']),0)

                # if result:
                #
                #     recordTimestamp=result
                # else:
                #     recordTimestamp=''

                # p0= self.redis0.pipeline()


                # 为了防止第一次插入数据库失败，需要以后有更新操作，这里更新时间可以设置长一些
                if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.ROOT_UPDATE_PERIOD)):        # the latest record time in hbase
                    recordTimestamp = currentTimestamp
                    p1 = self.redis1.pipeline()
                    p1.lpush(str(item['questionId'])
                                 ,str(item['questionTimestamp'])
                                 ,str(item['subTopicId'])
                                 # ,str(questionIndex)
                                 ,str(recordTimestamp))
                    p1.ltrim(str(item['questionId']),0,2)
                    p1.execute()
                    # isTopQuestion = 1 if item['isTopQuestion'] == 'true' else 0
                    quesBasicDict={'basic:quesId':str(item['questionId']),
                                                           # 'basic:answerCount':str(item['answerCount']),
                                                           # 'basic:isTopQues':str(isTopQuestion),
                                                           # 'basic:subTopicName':item['subTopicName'].encode('utf-8'),
                                                           'basic:subTopicId':str(item['subTopicId']),
                                                           'basic:quesTimestamp':str(item['questionTimestamp']),
                                                           # 'basic:quesName':item['questionName'].encode('utf-8'),
                                                           # 'basic:quesIndex':str(questionIndex)
                                                           }
                    try:
                        # self.redis11.hsetnx(str(item['questionId']),quesBasicDict)


                        self.questionTable.put(str(item['questionId']),quesBasicDict)
                        # self.redis1.lset(str(item['questionId']),0,str(recordTimestamp))
                    except Exception,e:
                        logging.error('Error with put questionId into redis11: '+str(e)+' try again......')
                        try:

                            self.questionTable.put(str(item['questionId']),quesBasicDict)

                            # self.redis11.hsetnx(str(item['questionId']),quesBasicDict)
                            # self.redis1.lset(str(item['questionId']),0,str(recordTimestamp))

                            logging.error(' tried again and successfully put data into redis11 ......')
                        except Exception,e:
                            logging.error('Error with put questionId into redis11: '+str(e)+'tried again and failed')
                    #更新记录的时间戳

            except Exception,e:
                logging.error('Error in try 0 with exception: '+str(e))

                #解除锁
                # self.redis0.hdel('questionLock',str(item['questionId']))
            return item
        else:
            return item


class QuesInfoPipeline(object):
    def __init__(self):

        self.redis2 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=2)

        connection = happybase.Connection(settings.HBASE_HOST)
        self.questionTable = connection.table('question')

    def process_item(self, item, spider):
        if item['spiderName'] == 'quesInfo':

            questionId = str(item['questionId'])



            currentTimestamp = int(time.time())

            recordTimestamp = self.redis2.lindex(str(questionId),0)
            # if result:
            #     recordTimestamp =result
            # else:
            #     recordTimestamp=''


            #无论之前有无记录，都会更新redis里的数据



            if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.INFO_UPDATE_PERIOD)):        # the latest record time in hbase
                recordTimestamp = currentTimestamp
                p2 = self.redis2.pipeline()
                p2.lpush(str(questionId)
                         # ,int(questionId)
                         ,str(item['dataResourceId'])
                         # ,str(isTopQuestion)
                         ,str(item['questionFollowerCount'])

                         ,str(item['questionAnswerCount'])
                         # 其实commentCount也可以去掉
                         ,str(item['quesCommentCount'])
                         # ,str(item['questionShowTimes'])

                         # ,str(item['topicRelatedFollowerCount'])
                         # ,str(item['visitsCount'])
                         ,str(recordTimestamp))

                p2.ltrim(str(questionId),0,4)
                p2.execute()

                isTopQuestion = 1 if item['isTopQuestion'] == 'true' else 0

                quesDetailDict={'detail:quesId':str(questionId),
                                'detail:idZnonceContent':str(item['idZnonceContent']),
                               'detail:dataUrlToken':str(item['dataUrlToken']),
                               'detail:isTopQues':str(isTopQuestion),
                               'detail:tagLabelIdList': str(item['tagLabelIdList']),
                               'detail:tagLabelDataTopicIdList': str(item['tagLabelDataTopicIdList']),
                               'detail:quesTitle': item['questionTitle'].encode('utf-8'),
                               'detail:dataResourceId': str(item['dataResourceId']),
                               'detail:quesAnswerCount': str(item['questionAnswerCount']),
                               'detail:quesFollowerCount': str(item['questionFollowerCount']),
                               'detail:quesLatestActiveTime': item['questionLatestActiveTime'].encode('utf-8'),
                               'detail:quesShowTimes': str(item['questionShowTimes']),
                               'detail:topicRelatedFollowerCount': str(item['topicRelatedFollowerCount']),
                               'detail:quesContent': item['questionDetail'].encode('utf-8'),
                               'detail:relatedQuesLinkList': str(item['relatedQuestionLinkList']),
                               'detail:quesCommentCount': str(item['quesCommentCount']),
                               'detail:visitsCount': str(item['visitsCount'])}


                try:
                    self.questionTable.put(str(questionId),quesDetailDict)

                    # self.redis11.hsetnx(str(questionId),quesDetailDict)
                    # self.redis2.lset(str(item['questionId']),0,str(recordTimestamp))

                except Exception,e:
                    logging.warning('Error with put questionId into redis: '+str(e)+' try again......')
                    try:
                        self.questionTable.put(str(questionId),quesDetailDict)
                        # self.redis11.hsetnx(str(questionId),quesDetailDict)
                        # self.redis2.lset(str(item['questionId']),0,str(recordTimestamp))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put questionId into redis: '+str(e)+'tried again and failed')


            return item

        else:
            return item


class QuesCommentPipeline(object):
    def __init__(self):

        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        connection = happybase.Connection(settings.HBASE_HOST)
        self.commentTable = connection.table('comment')

    def process_item(self, item, spider):
        if item['spiderName'] == 'quesComment':
            questionId = str(item['questionId'])
            #如果有返回数据，即有评论
            if questionId:
                #存储一个问题的所有
                self.redis11.sadd(str(questionId),str(item['commentDataId']))
                if item['userLinkId']:
                    self.redis3.sadd('userLinkIdSet',item['userLinkId'])
                #无论之前有无记录，都会更新redis里的数据
                commentDict={'comment:srcId':str(questionId),
                                'comment:DataId':str(item['commentDataId']),
                               'comment:content':str(item['commentContent'].encode('utf-8')),
                                #日期可能含有中文
                               'comment:date': str(item['commentDate'].encode('utf-8')),
                               'comment:upCount': str(item['commentUpCount']),
                               'comment:userName': item['userName'].encode('utf-8'),
                               'comment:userLinkId': item['userLinkId'].encode('utf-8'),
                               'comment:userImgLink': str(item['userImgLink']),
                                'comment:type':'q'
                               }
                try:
                    self.commentTable.put(str(item['commentDataId']),commentDict)
                except Exception,e:
                    logging.warning('Error with put questionId into redis: '+str(e)+' try again......')
                    try:
                        self.commentTable.put(str(item['commentDataId']),commentDict)
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put questionId into redis: '+str(e)+'tried again and failed')
            return item
        else:
            return item
class QuesFollowerPipeline(object):

    def __init__(self):

        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        connection = happybase.Connection(settings.HBASE_HOST)
        self.userTable = connection.table('user')
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'quesFollower':
            if item['userDataId'] :
                #userLinkId可能有中文
                self.redis11.sadd(str(item['questionId']),str(item['userDataId']))
                self.redis3.sadd('userLinkIdSet',str(item['userLinkId'].encode('utf-8')))
            DropItem()
        else:
            DropItem()
