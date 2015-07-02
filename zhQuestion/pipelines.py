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
            if  self.redis0.hsetnx('questionLock',str(item['questionId']),1):
                #使用try，是为了确保锁会被解除。
                try:
                    currentTimestamp = int(time.time())
                    # 如果之前有过该问题的记录，这里可以直接跳过，并不需要更新
                    # but 为了防止第一次插入数据库失败，需要以后有更新操作，这里更新时间可以设置长一些
                    result = self.redis1.lrange(str(item['questionId']),0,1)

                    if result:

                        [recordTimestamp,questionIndex]=result
                    else:
                        [recordTimestamp,questionIndex]=('','')

                    p0= self.redis0.pipeline()
                    p1 = self.redis1.pipeline()
                    if not recordTimestamp:
                        questionIndex = self.redis0.incr('totalCount',1)
                        #为了减少redis的使用空间
                        # p0.hsetnx('questionIndex'
                        #           ,str(questionIndex)
                        #           ,str(item['questionId']))
                        p0.hsetnx('questionIdIndex'
                                  ,str(item['questionId'])
                                  ,str(questionIndex))
                        p0.execute()

                        p1.incr('totalCount',1)
                        p1.lpush(str(item['questionId'])
                                     ,str(item['questionTimestamp'])
                                     ,str(item['subTopicId'])
                                     ,str(questionIndex)
                                     ,str(recordTimestamp))
                        p1.execute()


                    # 为了防止第一次插入数据库失败，需要以后有更新操作，这里更新时间可以设置长一些
                    if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.UPDATE_PERIOD)):        # the latest record time in hbase
                        recordTimestamp = currentTimestamp
                        # isTopQuestion = 1 if item['isTopQuestion'] == 'true' else 0
                        quesBasicDict={'basic:quesId':str(item['questionId']),
                                                               # 'basic:answerCount':str(item['answerCount']),
                                                               # 'basic:isTopQues':str(isTopQuestion),
                                                               # 'basic:subTopicName':item['subTopicName'].encode('utf-8'),
                                                               'basic:subTopicId':str(item['subTopicId']),
                                                               'basic:quesTimestamp':str(item['questionTimestamp']),
                                                               # 'basic:quesName':item['questionName'].encode('utf-8'),
                                                               'basic:quesIndex':str(questionIndex)}
                        try:
                            # self.redis11.hsetnx(str(item['questionId']),quesBasicDict)


                            self.questionTable.put(str(item['questionId']),quesBasicDict)
                            self.redis1.lset(str(item['questionId']),0,str(recordTimestamp))
                        except Exception,e:
                            logging.error('Error with put questionId into redis11: '+str(e)+' try again......')
                            try:

                                self.questionTable.put(str(item['questionId']),quesBasicDict)

                                # self.redis11.hsetnx(str(item['questionId']),quesBasicDict)
                                self.redis1.lset(str(item['questionId']),0,str(recordTimestamp))

                                logging.error(' tried again and successfully put data into redis11 ......')
                            except Exception,e:
                                logging.error('Error with put questionId into redis11: '+str(e)+'tried again and failed')
                        #更新记录的时间戳

                except Exception,e:
                    logging.error('Error in try 0 with exception: '+str(e))

                #解除锁
                self.redis0.hdel('questionLock',str(item['questionId']))
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

            result = self.redis2.lindex(str(questionId),0)
            if result:
                recordTimestamp =result
            else:
                recordTimestamp=''

            isTopQuestion = 1 if item['isTopQuestion'] == 'true' else 0
            #无论之前有无记录，都会更新redis里的数据
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


            if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.UPDATE_PERIOD)):        # the latest record time in hbase
                recordTimestamp = currentTimestamp
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
                    self.redis2.lset(str(item['questionId']),0,str(recordTimestamp))

                except Exception,e:
                    logging.warning('Error with put questionId into redis: '+str(e)+' try again......')
                    try:
                        self.questionTable.put(str(questionId),quesDetailDict)
                        # self.redis11.hsetnx(str(questionId),quesDetailDict)
                        self.redis2.lset(str(item['questionId']),0,str(recordTimestamp))
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
        # self.redis12 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=12)
        connection = happybase.Connection(settings.HBASE_HOST)
        self.questionTable = connection.table('question')
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
                    # self.redis11.hsetnx(str(item['commentDataId']),quesCommentDict)

                except Exception,e:
                    logging.warning('Error with put questionId into redis: '+str(e)+' try again......')
                    try:
                        # self.redis11.hsetnx(str(questionId),quesCommentDict)
                        self.commentTable.put(str(item['commentDataId']),commentDict)
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put questionId into redis: '+str(e)+'tried again and failed')
            return item
        else:
            return item


class QuesFollowerPipeline(object):

    def __init__(self):

        # self.redis1 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=1)

        #redis3存放用户索引，linkid，dataid，index
        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        # #redis4存放用户的基础信息
        # self.redis4 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=4)
        # #redis5存放问题的关注者集合
        # self.redis5 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=5)
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

                # #如果成功赋值，返回1，说明该用户当前没有上锁，可更新
                # if  self.redis3.hsetnx('userLock','userDataId',1):
                #     #通过查询dataId，判断是否记录过该用户信息
                #     result = self.redis4.lrange(str(userDataId),0,1)
                #     if result:
                #         [recordTimestamp,userIndex]=result
                #     else:
                #         [recordTimestamp,userIndex]=('','')
                #     # 表示数据库并没有该用户的信息
                #     if not recordTimestamp:
                #         userIndex = self.redis3.incr('totalCount',1)
                #         #这里有一点小问题，假设了下面的p3不会失败，一旦失败可能会有问题，原子性质
                #         p3 = self.redis3.pipeline()
                #         p3.hset('userIndex',str(userIndex),userDataId)
                #         #为了方便一次取得所有用户的linkid，这个地方比较复杂，要考虑userLinkId在不同时期被不同用户重用的情况，以及改名
                #         #这里是为了方便用userIndex取代userDataId
                #         p3.hset('userIndexLinkId',userIndex,str(userLinkId))
                #         p3.hset('userDataLinkId',userDataId,str(userLinkId))
                #         #解除锁定
                #         p3.hdel('userLock',userDataId)
                #         p3.execute()
                #     else:
                #         #解除锁定
                #         self.redis3.hdel('userLock',userDataId)
                # else:
                #     #另外一个client可能已将用户上锁，但还没来得及更新用户的userIndex
                #
                #     result = self.redis4.lrange(str(userDataId),0,1)
                #     if result:
                #         [recordTimestamp,userIndex]=result
                #     else:
                #         result = self.redis4.lrange(str(userDataId),0,1)
                #         if result:
                #             [recordTimestamp,userIndex]=result
                #         else:
                #             result = self.redis4.lrange(str(userDataId),0,1)
                #             if result:
                #                 [recordTimestamp,userIndex]=result
                #             else:
                #                 #说明这个用户有异常，本次丢弃
                #                 [recordTimestamp,userIndex]=('','')
                #                 #这里是会直接返回吗
                #                 DropItem()
                # #这里在做的是一直想得到用户的userIndex，而避免使用字节数很长的userDataId
                # #将该用户添加到相应的问题集合中，这里使用userIndex未必合适
                # #可能最后是空值吗？
                # self.redis5.sadd(str(questionId),str(userIndex))
                #
                # #如果没有记录过该用户，或者上条Hbase里数据库记录的时间超过了5天
                # #这是为了避免几个爬虫爬到同一个用户，这个数据需要根据爬虫的效率更改
                # if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp)>int(settings.UPDATE_PERIOD)): # 最多5天更新一次
                #     recordTimestamp = currentTimestamp
                #     try:
                #         self.userTable.put(str(userDataId)
                #                        ,{'basic:dataId':str(userDataId)
                #                         ,'basic:linkId':str(userLinkId)
                #                          ,'basic:imgUrl':str(item['userImgUrl'])
                #                          ,'basic:name':item['userNamet'].encode('utf-8')
                #                          ,'basic:answerCount':str(item['userAnswerCount'])
                #                          ,'basic:askCount':str(item['userAskCount'])
                #                          ,'basic:followerCount':str(item['userFollowerCount'])
                #                          ,'basic:upCount':str(item['userUpCount'])})
                #     except Exception,e:
                #         logging.error('Error with put questionId into hbase: '+str(e)+' try again......')
                #         try:
                #             self.userTable.put(str(userDataId)
                #                        ,{'basic:dataId':str(userDataId)
                #                         ,'basic:linkId':str(userLinkId)
                #                          ,'basic:imgUrl':str(item['userImgUrl'])
                #                          ,'basic:name':item['userNamet'].encode('utf-8')
                #                          ,'basic:answerCount':str(item['userAnswerCount'])
                #                          ,'basic:askCount':str(item['userAskCount'])
                #                          ,'basic:followerCount':str(item['userFollowerCount'])
                #                          ,'basic:upCount':str(item['userUpCount'])})
                #             logging.error(' tried again and successfully put data into hbase ......')
                #         except Exception,e:
                #             logging.error('Error with put questionId into hbase: '+str(e)+'tried again and failed')
                #
                #
                #     p4 = self.redis4.pipeline()
                #     p4.lpush(str(userDataId)
                #
                #                       ,str(userLinkId)
                #                       ,item['userImgUrl']
                #                       ,item['userName']
                #                       ,item['userAnswerCount']
                #                       ,item['userAskCount']
                #                       ,item['userFollowerCount']
                #                       ,item['userUpCount']
                #                       ,str(userIndex)
                #                       ,str(recordTimestamp))
                #     #为了减少redis请求次数
                #     p4.ltrim(str(userDataId),0,8)
                #     p4.execute()



