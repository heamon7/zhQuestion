# -*- coding: utf-8 -*-

# Scrapy settings for zhQuestion project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'zhQuestion'

SPIDER_MODULES = ['zhQuestion.spiders']
NEWSPIDER_MODULE = 'zhQuestion.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'zhQuestion (+http://www.yourdomain.com)'


DOWNLOAD_TIMEOUT = 700

LOG_LEVEL = 'INFO'

DEFAULT_REQUEST_HEADERS = {
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate, sdch',
           'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,zh-TW;q=0.2',
           'Connection': 'keep-alive',
           'Host': 'www.zhihu.com',
           'Referer': 'http://www.zhihu.com/',

}

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36'

EXTENSIONS = {
    'scrapy.extensions.feedexport.FeedExporter': None
}

ITEM_PIPELINES = {
    'zhQuestion.pipelines.QuesRootPipeline': 300,
    'zhQuestion.pipelines.QuesInfoPipeline': 400,
    'zhQuestion.pipelines.QuesCommentPipeline': 500,
    'zhQuestion.pipelines.QuesFollowerPipeline': 600,

}
SPIDER_MIDDDLEWARES = {
    'scrapy.contrib.spidermiddleware.httperror.HttpErrorMiddleware':300,
}

DUPEFILTER_CLASS = 'zhQuesFollower.custom_filters.SeenURLFilter'



UPDATE_PERIOD = '432000' #最快5天更新一次


