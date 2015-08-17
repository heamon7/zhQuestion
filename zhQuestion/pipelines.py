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
from pymongo import MongoClient
import datetime

# 这里统一采取先将本爬虫本次爬到的数据存入redis，待本次爬取完毕后，统一将数据持久化到hbase，
# 而不是像之前那样，拿到一个数据就往hbase里存一次
class QuesRootPipeline(object):

    def __init__(self):
        self.redis0 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=0)
        self.redis1 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=1)

        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_question = db['question']
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        # connection = happybase.Connection(settings.HBASE_HOST,timeout=10000)
        # self.questionTable = connection.table('question')

    def process_item(self, item, spider):
        if item['spiderName'] == 'quesRoot':
            try:
                currentTimestamp = int(time.time())
                recordTimestamp = self.redis1.lindex(str(item['questionId']),0)
                # 这个地方其实已经做了去重的处理
                if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.ROOT_UPDATE_PERIOD)):        # the latest record time in hbase
                    recordTimestamp = currentTimestamp
                    p1 = self.redis1.pipeline()
                    p1.lpush(int(item['questionId'])
                                 ,int(item['questionTimestamp'])
                                 ,item['subTopicId']
                                 # ,str(questionIndex)
                                 ,int(recordTimestamp))
                    p1.ltrim(int(item['questionId']),0,2)
                    p1.execute()
                    # isTopQuestion = 1 if item['isTopQuestion'] == 'true' else 0
                    ques_basic_dict={'ques_id':int(item['questionId']),
                                                           # 'basic:answerCount':str(item['answerCount']),
                                                           # 'basic:isTopQues':str(isTopQuestion),
                                                           # 'basic:subTopicName':item['subTopicName'].encode('utf-8'),
                                                           'sub_topic_id':item['subTopicId'],
                                                           'ques_timestamp':int(item['questionTimestamp']),
                                                           'updated_at': datetime.datetime.now()
                                                           # 'basic:quesName':item['questionName'].encode('utf-8'),
                                                           # 'basic:quesIndex':str(questionIndex)
                                                           }
                    try:
                        # self.redis11.hsetnx(str(item['questionId']),quesBasicDict)
                        # self.col_question.update_one({'ques_id':int(item['questionId'].strip())},{'$set':quesBasicDict},True)
                        self.col_question.insert_one(ques_basic_dict)
                        # self.questionTable.put(str(item['questionId']),quesBasicDict)
                        # self.redis1.lset(str(item['questionId']),0,str(recordTimestamp))
                    except Exception,e:
                        logging.error('Error with put questionId into mongo: '+str(e)+' try again......')
                        try:


                            # self.questionTable.put(str(item['questionId']),quesBasicDict)
                            # self.col_question.update_one({'ques_id':int(item['questionId'].strip())},{'$set':quesBasicDict},True)
                            self.col_question.insert_one(ques_basic_dict)
                            # self.redis11.hsetnx(str(item['questionId']),quesBasicDict)
                            # self.redis1.lset(str(item['questionId']),0,str(recordTimestamp))

                            logging.error(' tried again and successfully put data into mongo ......')
                        except Exception,e:
                            logging.error('Error with put questionId into mongo: '+str(e)+'tried again and failed')
                            logging.error('The item is %s',str(item))
                    #更新记录的时间戳

            except Exception,e:
                logging.error('Error in try 0 with exception: '+str(e))
                logging.error('The item is %s',str(item))
                #解除锁
                # self.redis0.hdel('questionLock',str(item['questionId']))
            return item
        else:
            return item


class QuesInfoPipeline(object):
    def __init__(self):

        self.redis2 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=2)

        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_question_info = db['questionInfo']
        # connection = happybase.Connection(settings.HBASE_HOST,timeout=10000)
        # self.questionTable = connection.table('question')

    def process_item(self, item, spider):
        if item['spiderName'] == 'quesInfo':
            try:
                questionId = int(item['questionId'])
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
                    p2.lpush(int(questionId)
                             # ,int(questionId)
                             ,str(item['dataResourceId'])
                             # ,str(isTopQuestion)
                             ,int(item['questionFollowerCount'])

                             ,int(item['questionAnswerCount'])
                             # 其实commentCount也可以去掉
                             ,int(item['quesCommentCount'])
                             # ,str(item['questionShowTimes'])

                             # ,str(item['topicRelatedFollowerCount'])
                             # ,str(item['visitsCount'])
                             ,int(recordTimestamp))

                    p2.ltrim(int(questionId),0,4)
                    p2.execute()

                    isTopQuestion = 1 if item['isTopQuestion'] == 'true' else 0

                    ques_detail_dict={'ques_id':int(questionId),
                                    'id_znonce_content':str(item['idZnonceContent']),
                                   'data_url_token':str(item['dataUrlToken']),
                                   'is_top_ques':int(isTopQuestion),
                                   'tag_label_id_list': item['tagLabelIdList'],
                                   'tag_label_data_topic_id_list': item['tagLabelDataTopicIdList'],
                                   'ques_title': item['questionTitle'],
                                   'data_resource_id': str(item['dataResourceId']),
                                   'ques_answer_count': int(item['questionAnswerCount']),
                                   'ques_follower_count': int(item['questionFollowerCount']),
                                   'ques_latest_active_time': item['questionLatestActiveTime'],
                                   'ques_show_times': int(item['questionShowTimes']),
                                   'topic_related_follower_count': int(item['topicRelatedFollowerCount']),
                                   'ques_content': item['questionDetail'],
                                   'related_ques_link_list': item['relatedQuestionLinkList'],
                                   'ques_comment_count': int(item['quesCommentCount']),
                                   'visits_count': int(item['visitsCount']),
                                   'updated_at': datetime.datetime.now()}


                    try:
                        self.col_question_info.insert_one(ques_detail_dict)

                        # self.redis11.hsetnx(str(questionId),quesDetailDict)
                        # self.redis2.lset(str(item['questionId']),0,str(recordTimestamp))

                    except Exception,e:
                        logging.error('Error with put questionId into mongo: '+str(e)+' try again......')
                        try:
                            self.col_question_info.insert_one(ques_detail_dict)
                            # self.redis11.hsetnx(str(questionId),quesDetailDict)
                            # self.redis2.lset(str(item['questionId']),0,str(recordTimestamp))
                            logging.error('tried again and successfully put data into mongo ......')
                        except Exception,e:
                            logging.error('Error with put questionId into mongo: '+str(e)+'tried again and failed')
                            logging.error('The item is %s',str(item))



            except Exception,e:
                logging.error('Error in try 0 with exception: '+str(e))
                logging.error('The item is %s',str(item))
            return item
        else:
            return item


class QuesCommentPipeline(object):
    def __init__(self):

        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_question_comment = db['questionComment']
        self.col_user_link = db['userLink']
        # connection = happybase.Connection(settings.HBASE_HOST,timeout=10000)
        # self.commentTable = connection.table('comment')

    def process_item(self, item, spider):
        if item['spiderName'] == 'quesComment':
            try:
                questionId = str(item['questionId'])
                #如果有返回数据，即有评论
                if questionId:
                    #存储一个问题的所有
                    # self.redis11.sadd(str(questionId),str(item['commentDataId']))
                    if item['userLinkId']:
                        self.col_user_link.update_one({'key_name':'user_link_set'},{'$addToSet':{'user_link_set':item['userLinkId']}},True)
                    #无论之前有无记录，都会更新redis里的数据
                    comment_dict={'ques_id':int(questionId),
                                    'comment_data_id':int(item['commentDataId']),
                                   'content':item['commentContent'],
                                    #日期可能含有中文
                                   'date': item['commentDate'],
                                   'vote_count': int(item['commentUpCount']),
                                   'user_name': item['userName'],
                                   'user_link_id': item['userLinkId'],
                                   'user_img_link': item['userImgLink'],
                                    'comment_type':'q',
                                    'updated_at':datetime.datetime.now()
                                   }
                    try:
                        self.col_question_comment.insert_one(comment_dict)
                    except Exception,e:
                        logging.warning('Error with put questionId into mongo: '+str(e)+' try again......')
                        try:
                            self.col_question_comment.insert_one(comment_dict)
                            logging.warning('tried again and successfully put data into mongo ......')
                        except Exception,e:
                            logging.warning('Error with put comment into mongo: '+str(e)+'tried again and failed')
            except Exception,e:
                logging.error('Error in try 0 with exception: '+str(e))
                logging.error('The item is %s',str(item))
            return item
        else:
            return item
class QuesFollowerPipeline(object):

    def __init__(self):

        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_question_follower = db['questionFollower']
        self.col_user_link = db['userLink']

        # connection = happybase.Connection(settings.HBASE_HOST,timeout=10000)
        # self.userTable = connection.table('user')
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'quesFollower':
            try:
                if item['userDataId'] :
                    #userLinkId可能有中文
                    question_follower_dict = {
                        'ques_id': int(item['questionId']),
                        'user_data_id': str(item['userDataId'])
                    }
                    self.col_question_follower.insert_one(question_follower_dict)
                    self.col_user_link.update_one({'key_name':'user_link_set'},{'$addToSet':{'user_link_set':item['userLinkId']}},True)
                    # self.redis11.sadd(str(item['questionId']),str(item['userDataId']))
                    # self.redis3.sadd('userLinkIdSet',str(item['userLinkId'].encode('utf-8')))
            except Exception,e:
                logging.error('Error in try 0 with exception: '+str(e))
                logging.error('The item is %s',str(item))
            DropItem()
        else:
            DropItem()
