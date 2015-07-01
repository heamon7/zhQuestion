# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class QuesRootItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    # quesRoot
    spiderName = scrapy.Field()
    subTopicId = scrapy.Field()
    questionTimestamp = scrapy.Field()
    questionId = scrapy.Field()

    answerCount = scrapy.Field()
    isTopQuestion = scrapy.Field()
    subTopicName = scrapy.Field()
    questionName = scrapy.Field()


class QuesInfoItem(scrapy.Item):
    spiderName = scrapy.Field()
    questionId = scrapy.Field()
    idZnonceContent =  scrapy.Field()   #   //*[@id="znonce"]

    dataUrlToken = scrapy.Field()   #   //*[@id="zh-single-question-page"]
    isTopQuestion = scrapy.Field()  #   //*[@id="zh-single-question-page"]/meta[1]
    visitsCount = scrapy.Field()    #   //*[@id="zh-single-question-page"]/meta[2]


    tagLabelIdList = scrapy.Field()  # //*[@id="zh-single-question-page"]/div[1]/div/div[2]/div
    tagLabelDataTopicIdList = scrapy.Field()
    questionTitle = scrapy.Field()  #   //*[@id="zh-question-title"]/h2/text()

    questionDetail = scrapy.Field() #   //*[@id="zh-question-detail"]
    dataResourceId = scrapy.Field() #   //*[@id="zh-question-detail"]   or  //*[@id="zh-question-answer-summary"]

    #zmEditableContent = scrapy.Field()  # not always exists  //*[@id="zh-question-detail"]/div
    #questionContentHidden = scrapy.Field()  # not always exists    //*[@id="zh-question-detail"]/textarea
    #questionSummary = scrapy.Field()    # not always exists     //*[@id="zh-question-detail"]/div

    quesCommentCount = scrapy.Field()   #   //*[@id="zh-question-meta-wrap"]/div[1]/a[2]/text()
    quesCommentLink = scrapy.Field()    # need to analyze httpreq

    questionAnswerCount = scrapy.Field()  #   //*[@id="zh-question-answer-num"]








    dataPageSize = scrapy.Field()
    # 注意这里的 dataInit 值得注意   //*[@id="zh-question-answer-wrap"]
    pageSize = scrapy.Field()
    offset = scrapy.Field()
    nodeName = scrapy.Field()   #   //*[@id="zh-question-answer-wrap"]

    # field below is for answer
    dataAid = scrapy.Field()    # 实际上就是answer的id  //*[@id="zh-question-answer-wrap"]/div[1]
    dataAtoken =scrapy.Field()  # 这个是神马？值得注意
    dataCreated = scrapy.Field()
    dataDeleted = scrapy.Field()
    dataHelpful = scrapy.Field()
    dataScore = scrapy.Field()  #   //*[@id="zh-question-answer-wrap"]/div[1]

    answerAuthorPeopleLink = scrapy.Field()   #   //*[@id="zh-question-answer-wrap"]/div[1]/div[2]/div[1]/h3/a[1]
    answerAuthorImgLink = scrapy.Field()  #   //*[@id="zh-question-answer-wrap"]/div[1]/div[2]/div[1]/h3/a[1]/img
    answerAuthorName = scrapy.Field() #   //*[@id="zh-question-answer-wrap"]/div[1]/div[2]/div[1]/h3/a[2]
    answerAuthorBio = scrapy.Field()  #   //*[@id="zh-question-answer-wrap"]/div[1]/div[2]/div[1]/h3/strong
    voteUpCount = scrapy.Field()  #   //*[@id="zh-question-answer-wrap"]/div[1]/div[2]/div[2]
    answerVoterList = scrapy.Field()  #   //*[@id="zh-question-answer-wrap"]/div[1]/div[2]/div[2]/span

    answerDataResourceId = scrapy.Field() # 值得注意   //*[@id="zh-question-answer-wrap"]/div[1]/div[3]
    answerContent = scrapy.Field()    #   //*[@id="zh-question-answer-wrap"]/div[1]/div[3]/div
    answerCreateDate = scrapy.Field() #   //*[@id="zh-question-answer-wrap"]/div[1]/div[4]/div/span[1]/a
    answerUpdateDate = scrapy.Field() #   //*[@id="zh-question-answer-wrap"]/div[1]/div[4]/div/span[1]/a

    answerCommentCount = scrapy.Field()   #   //*[@id="zh-question-answer-wrap"]/div[1]/div[4]/div/a[1]/text()
    answerCommentLink = scrapy.Field()    # need to anlyze the http req
    # answer end
    questionFollowDataId = scrapy.Field() #   //*[@id="zh-question-side-header-wrap"]/button
    questionFollowerLink = scrapy.Field()   #   //*[@id="zh-question-side-header-wrap"]/div[2]/div[1]/a
    questionFollowerCount = scrapy.Field()  #   //*[@id="zh-question-side-header-wrap"]/div[2]/div[1]/a/strong
    questionFollowerList = scrapy.Field() #   //*[@id="zh-question-side-header-wrap"]/div[2]/div[2]

    sideSectionId = scrapy.Field()    # //*[@id="shameimaru-question-up-83594d68c"]

    relatedQuestionLinkList = scrapy.Field()  #   //*[@id="zh-question-related-questions"]/ul

    shareDataId = scrapy.Field()  #   //*[@id="zh-question-webshare-container"]

    questionLatestActiveTime = scrapy.Field() #   //*[@id="zh-single-question-page"]/div[2]/div[5]/div/div[1]/span[1]
    questionLog = scrapy.Field()  #   //*[@id="zh-single-question-page"]/div[2]/div[5]/div/div[1]/a
    questionShowTimes = scrapy.Field()    #   //*[@id="zh-single-question-page"]/div[2]/div[5]/div/div[2]/strong[1]
    topicRelatedFollowerCount = scrapy.Field() #   //*[@id="zh-single-question-page"]/div[2]/div[5]/div/div[2]/strong[2]





class QuesCommentItem(scrapy.Item):
    # quesComment
    spiderName = scrapy.Field()
    questionId = scrapy.Field()
    commentDataId = scrapy.Field()
    userName = scrapy.Field()
    userLinkId = scrapy.Field()
    userImgLink = scrapy.Field()
    commentContent = scrapy.Field()
    commentDate = scrapy.Field()
    commentUpCount = scrapy.Field()

    pass

class QuesFollowerItem(scrapy.Item):
    spiderName = scrapy.Field()

    offset = scrapy.Field()
    questionId = scrapy.Field()
    userDataId = scrapy.Field()
    userLinkId = scrapy.Field()
    userImgUrl = scrapy.Field()
    userName = scrapy.Field()
    userFollowerCount = scrapy.Field()
    userAskCount = scrapy.Field()
    userAnswerCount = scrapy.Field()
    userUpCount = scrapy.Field()

    pass






